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

import java.util.Map.Entry;

import org.apache.accumulo.client.typo.Typo;
import org.apache.accumulo.client.typo.TypoKey;
import org.apache.accumulo.client.typo.TypoMutation;
import org.apache.accumulo.client.typo.TypoScanner;
import org.apache.accumulo.client.typo.encoders.PairLexicoder;
import org.apache.accumulo.client.typo.encoders.StringLexicoder;
import org.apache.accumulo.client.typo.encoders.ULongLexicoder;
import org.apache.accumulo.client.typo.tuples.Pair;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;


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
    conn.tableOperations().create("foo");
    
    insertData(conn);
    scanData(conn);
  }
  
  static void insertData(Connector conn) throws Exception {
    BatchWriter bw = conn.createBatchWriter("foo", 1000000, 60000, 2);
    
    GraphTypo graphTypo = new GraphTypo();
    
    TypoMutation<Pair<Long,Long>,String,String,Long> edge1 = graphTypo.newMutation(new Pair<Long, Long>(95l,9023580982l));
    edge1.put("counts", "clicked", 20l);
    edge1.put("counts", "droped", 30l);
    bw.addMutation(edge1);
    
    TypoMutation<Pair<Long,Long>,String,String,Long> edge2 = graphTypo.newMutation(new Pair<Long, Long>(95l,10567l));
    edge2.put("counts", "clicked", 67l);
    edge2.put("counts", "droped", 90l);
    bw.addMutation(edge2);
    
    TypoMutation<Pair<Long,Long>,String,String,Long> edge3 = graphTypo.newMutation(new Pair<Long, Long>(95l,123l));
    edge3.put("counts", "clicked", 2l);
    edge3.put("counts", "droped", 6000l);
    bw.addMutation(edge3);
    
    
    TypoMutation<Pair<Long,Long>,String,String,Long> edge4 = graphTypo.newMutation(new Pair<Long, Long>(23l,123l));
    edge4.put("counts", "clicked", 8l);
    edge4.put("counts", "droped", 4l);
    bw.addMutation(edge4);
    
    bw.close();
  }
  
  static void scanData(Connector conn) throws Exception {
    GraphTypo graphTypo = new GraphTypo();

    Scanner scanner = conn.createScanner("foo", Constants.NO_AUTHS);
    
    // scan over all edge that connect to node 95
    scanner.setRange(graphTypo.newRange(new Pair<Long,Long>(95l, 0l), new Pair<Long,Long>(95l, Long.MAX_VALUE)));
    
    TypoScanner<Pair<Long,Long>,String,String,Long> typoScanner = graphTypo.newScanner(scanner);
    
    for (Entry<TypoKey<Pair<Long,Long>,String,String>,Long> entry : typoScanner) {
      System.out.println(entry);
    }
    

  }
}
