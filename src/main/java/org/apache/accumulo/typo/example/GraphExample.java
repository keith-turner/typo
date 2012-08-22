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
package org.apache.accumulo.typo.example;

import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.typo.Typo;
import org.apache.accumulo.typo.encoders.PairLexicoder;
import org.apache.accumulo.typo.encoders.StringLexicoder;
import org.apache.accumulo.typo.encoders.ULongLexicoder;
import org.apache.accumulo.typo.tuples.Pair;


class GraphTypo extends Typo<Pair<Long,Long>,String,String,Long> {
  public GraphTypo() {
    super(new PairLexicoder<Long,Long>(new ULongLexicoder(), new ULongLexicoder()), new StringLexicoder(), new StringLexicoder(), new ULongLexicoder());
  }
}

/**
 * A simple example of storing graphs in Accumlo. This is done via storing edges in the Accumulo row. An edge is composed of two Long node identifiers.
 * 
 */

public class GraphExample {
  public static void main(String[] args) throws Exception {
    MockInstance mi = new MockInstance();
    Connector conn = mi.getConnector("root", "secret");
    conn.tableOperations().create("edges");
    
    insertData(conn);
    scanData(conn);
  }
  
  static void insertData(Connector conn) throws Exception {
    BatchWriter bw = conn.createBatchWriter("edges", 1000000, 60000, 2);
    
    GraphTypo graphTypo = new GraphTypo();
    
    GraphTypo.Mutation edge1 = graphTypo.newMutation(new Pair<Long,Long>(95l, 9023580982l));
    edge1.put("counts", "clicked", 20l);
    edge1.put("counts", "dropped", 30l);
    bw.addMutation(edge1);
    
    GraphTypo.Mutation edge2 = graphTypo.newMutation(new Pair<Long,Long>(95l, 10567l));
    edge2.put("counts", "clicked", 67l);
    edge2.put("counts", "dropped", 90l);
    bw.addMutation(edge2);
    
    GraphTypo.Mutation edge3 = graphTypo.newMutation(new Pair<Long,Long>(95l, 123l));
    edge3.put("counts", "clicked", 2l);
    edge3.put("counts", "dropped", 6000l);
    bw.addMutation(edge3);
    
    
    GraphTypo.Mutation edge4 = graphTypo.newMutation(new Pair<Long,Long>(23l, 123l));
    edge4.put("counts", "clicked", 8l);
    edge4.put("counts", "dropped", 4l);
    bw.addMutation(edge4);
    
    // if a bi-directional link exist between two nodes X and Y, then two edges should be inserted
    // one for each direction.... earlier 95, 123 was inserted.. this is the only way that
    // both directions of the link can be found efficiently
    GraphTypo.Mutation edge5 = graphTypo.newMutation(new Pair<Long,Long>(123l, 95l));
    edge5.put("counts", "clicked", 12l);
    edge5.put("counts", "dropped", 50l);
    bw.addMutation(edge5);

    bw.close();
  }
  
  static void scanData(Connector conn) throws Exception {
    GraphTypo graphTypo = new GraphTypo();

    Scanner scanner = conn.createScanner("edges", Constants.NO_AUTHS);
    
    // scan over all edge that connect to node 95
    scanner.setRange(graphTypo.newRange(new Pair<Long,Long>(95l, 0l), new Pair<Long,Long>(95l, Long.MAX_VALUE)));
    
    GraphTypo.Scanner typoScanner = graphTypo.newScanner(scanner);
    
    for (Entry<GraphTypo.Key,Long> entry : typoScanner) {
      System.out.println(entry);
    }
    

  }
}
