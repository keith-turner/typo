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

import java.util.Calendar;
import java.util.Date;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.typo.Typo;
import org.apache.accumulo.typo.encoders.DateLexicoder;
import org.apache.accumulo.typo.encoders.ReverseLexicoder;
import org.apache.accumulo.typo.encoders.StringLexicoder;
import org.apache.accumulo.typo.encoders.TextLexicoder;
import org.apache.hadoop.io.Text;

class RDTypo extends Typo<Date,String,String,Text> {
  public RDTypo() {
    super(new ReverseLexicoder<Date>(new DateLexicoder()), new StringLexicoder(), new StringLexicoder(), new TextLexicoder());
  }
}

public class ReverseExample {
  public static void main(String[] args) throws Exception {
    MockInstance mi = new MockInstance();
    Connector conn = mi.getConnector("root", "secret");
    conn.tableOperations().create("foo");
    
    insertData(conn);
    scanData(conn);
  }
  
  static void insertData(Connector conn) throws Exception {
    BatchWriter bw = conn.createBatchWriter("foo", 1000000, 60000, 2);
    
    RDTypo rdTypo = new RDTypo();
    
    Calendar cal = Calendar.getInstance();
    
    cal.set(2009, 1, 1, 22, 23, 24);
    RDTypo.Mutation mut1 = rdTypo.newMutation(cal.getTime());
    mut1.put("event", "meeting", new Text("@joes"));
    bw.addMutation(mut1);
    
    cal.set(2008, 1, 1, 22, 23, 24);
    RDTypo.Mutation mut2 = rdTypo.newMutation(cal.getTime());
    mut2.put("event", "meeting", new Text("@johns"));
    bw.addMutation(mut2);
    
    cal.set(2007, 1, 1, 22, 23, 24);
    RDTypo.Mutation mut3 = rdTypo.newMutation(cal.getTime());
    mut3.put("event", "meeting", new Text("@sues"));
    bw.addMutation(mut3);

    bw.close();
  }
  
  static void scanData(Connector conn) throws Exception {
    RDTypo rdTypo = new RDTypo();
    Scanner scanner = conn.createScanner("foo", Constants.NO_AUTHS);
    
    Calendar cal = Calendar.getInstance();
    cal.set(2008, 2, 1, 22, 23, 24);
    scanner.setRange(rdTypo.newRange(cal.getTime(), null));

    RDTypo.Scanner typoScanner = rdTypo.newScanner(scanner);

    for (Entry<RDTypo.Key,Text> entry : typoScanner) {
      System.out.println(entry);
    }
  }
}