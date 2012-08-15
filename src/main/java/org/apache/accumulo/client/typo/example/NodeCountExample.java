/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.client.typo.example;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map.Entry;

import org.apache.accumulo.client.typo.Typo;
import org.apache.accumulo.client.typo.encoders.LongLexicoder;
import org.apache.accumulo.client.typo.encoders.StringLexicoder;
import org.apache.accumulo.client.typo.mapreduce.TypoInputFormat;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This example shows how to use Typo with a map reduce job. The example reads from an edge table that represents edges in a graph and writes out summary
 * information to a node table. The node table contains in and out counts for each node in the graph. A summing combiner that uses the {@link LongLexicoder} is
 * setup on the node table to aggregate its values.
 * 
 */

public class NodeCountExample extends Configured implements Tool {
  
  static class NodeTypo extends Typo<Long,String,String,Long> {
    public NodeTypo() {
      super(new LongLexicoder(), new StringLexicoder(), new StringLexicoder(), new LongLexicoder());
    }
  }

  static class InputFormat extends TypoInputFormat {
    public InputFormat() {
      super(new GraphTypo());
    }
  }

  static class CCMapper extends Mapper<GraphTypo.Key,Long,Text,Mutation> {
    
    private NodeTypo nodeTypo = new NodeTypo();

    @Override
    protected void map(GraphTypo.Key key, Long value, Context context) throws IOException, InterruptedException {
      
      NodeTypo.Mutation m = nodeTypo.newMutation(key.getRow().getFirst());
      m.put("count", "out", 1l);
      context.write(null, m);
      
      m = nodeTypo.newMutation(key.getRow().getSecond());
      m.put("count", "in", 1l);
      context.write(null, m);
    }
  }

  @Override
  public int run(String[] args) throws Exception {

    Job job = new Job(getConf(), NodeCountExample.class.getName());
    
    job.getConfiguration().set("mapred.job.tracker", "local");
    job.getConfiguration().set("fs.default.name", "file:///");

    job.setJarByClass(this.getClass());
    
    job.setInputFormatClass(InputFormat.class);

    job.setMapperClass(CCMapper.class);
    
    InputFormat.setInputInfo(job.getConfiguration(), "root", "secret".getBytes(), "edges", Constants.NO_AUTHS);
    InputFormat.setMockInstance(job.getConfiguration(), "typeMRTest");
    
    // only want to pull back one column for each edge for counting purposes
    InputFormat.fetchColumns(new NodeTypo(), job.getConfiguration(), Arrays.asList(new Pair<String,String>("counts", "clicked")));
    
    job.setNumReduceTasks(0);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputKeyClass(Mutation.class);
    
    job.setOutputFormatClass(AccumuloOutputFormat.class);
    AccumuloOutputFormat.setMockInstance(job.getConfiguration(), "typeMRTest");
    AccumuloOutputFormat.setOutputInfo(job.getConfiguration(), "root", "secret".getBytes(), false, "nodes");
    

    return job.waitForCompletion(true) ? 0 : -1;
  }

  public static void main(String[] args) throws Exception {
    
    MockInstance mi = new MockInstance("typeMRTest");
    Connector conn = mi.getConnector("root", "secret");
    conn.tableOperations().create("edges");
    
    // insert some test data
    GraphExample.insertData(conn);
    
    // setup the output table
    createNodesTable(conn);

    // run the map reduce job to read the edge table and populate the node table
    int res = ToolRunner.run(new Configuration(), new NodeCountExample(), args);
    
    // print the out the counts for each node
    sumNodeCounts(conn);

    System.exit(res);
  }

  /**
   * Create a table for nodes with a Combiner that sums up counts in the columns.
   */
  public static void createNodesTable(Connector conn) throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
    conn.tableOperations().create("nodes");
    
    // set up a combiner to aggregate the counts in the node table
    IteratorSetting iterSetting = new IteratorSetting(10, SummingCombiner.class);
    // have the combiner use the LongLexicoder to interpret the data in the Accumulo value
    SummingCombiner.setEncodingType(iterSetting, LongLexicoder.class);
    SummingCombiner.setCombineAllColumns(iterSetting, true);
    conn.tableOperations().attachIterator("nodes", iterSetting);
  }

  /**
   * This method will print the in-degree and out-degree of each node in the graph.
   */

  public static void sumNodeCounts(Connector conn) throws TableNotFoundException {
    
    RowIterator rowIter = new RowIterator(conn.createScanner("nodes", Constants.NO_AUTHS));
    
    while (rowIter.hasNext()) {
      NodeTypo.TypoIterator columnIter = new NodeTypo().newIterator(rowIter.next());
      long in = 0;
      long out = 0;
      long node = 0;
      
      while (columnIter.hasNext()) {
        Entry<NodeTypo.Key,Long> entry = columnIter.next();
        
        node = entry.getKey().getRow();
        
        if (entry.getKey().getColumnFamily().equals("count")) {
          if (entry.getKey().getColumnQualifier().equals("out"))
            out = entry.getValue();
          else if (entry.getKey().getColumnQualifier().equals("in"))
            in = entry.getValue();
        }
      }
      
      System.out.printf("node: %10d in: %5d out: %5d\n", node, in, out);
    }


  }

}
