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
import org.apache.accumulo.client.typo.encoders.DoubleEncoder;
import org.apache.accumulo.client.typo.encoders.LongEncoder;
import org.apache.accumulo.client.typo.encoders.StringEncoder;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;

class MyTypo extends Typo<Long,String,Double,String> {
  public MyTypo() {
    super(new LongEncoder(), new StringEncoder(), new DoubleEncoder(), new StringEncoder());
  }
}

public class TypoExample {
  public static void main(String[] args) throws Exception {
    MockInstance mi = new MockInstance();
    Connector conn = mi.getConnector("root", "secret");
    conn.tableOperations().create("foo");
    
    insertData(conn);
    scanData(conn);
  }
  
  static void insertData(Connector conn) throws Exception {
    BatchWriter bw = conn.createBatchWriter("foo", 1000000, 60000, 2);
    
    MyTypo myTypo = new MyTypo();
    
    for (long row = -4; row < 4; row++) {
      TypoMutation<Long,String,Double,String> mut = myTypo.newMutation(row);
      mut.put("sq", Math.pow(row, 2), "val");
      mut.put("cube", Math.pow(row, 3), "val");
      bw.addMutation(mut);
    }
    
    bw.close();
  }
  
  static void scanData(Connector conn) throws Exception {
    MyTypo myTypo = new MyTypo();
    Scanner scanner = conn.createScanner("foo", Constants.NO_AUTHS);
    
    scanner.setRange(myTypo.newRange(-2l, 3l));
    
    TypoScanner<Long,String,Double,String> typoScanner = myTypo.newScanner(scanner);
    
    typoScanner.fetchColumnFamily("sq");

    long rowSum = 0;
    double cqSum = 0;
    
    for (Entry<TypoKey<Long,String,Double>,String> entry : typoScanner) {
      rowSum += entry.getKey().getRow();
      cqSum += entry.getKey().getColumnQualifier();
      System.out.println(entry);
    }
    
    System.out.println("rowSum : " + rowSum);
    System.out.println("cqSum  : " + cqSum);
  }
}
