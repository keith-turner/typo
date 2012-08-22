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
import org.apache.accumulo.typo.TypoFormatter;
import org.apache.accumulo.typo.constraints.TypoConstraint;
import org.apache.accumulo.typo.encoders.DoubleLexicoder;
import org.apache.accumulo.typo.encoders.LongLexicoder;
import org.apache.accumulo.typo.encoders.StringLexicoder;

// An easy way to use Typo is to create a class that extends it and use the 
// subtype everywhere in your code.  This is what was done below. Nomrally the
// class below would be public and in its own file.  To keep the example self 
// contained, this was not done.

class MyTypo extends Typo<Long,String,Double,String> {
  public MyTypo() {
    super(new LongLexicoder(), new StringLexicoder(), new DoubleLexicoder(), new StringLexicoder());
  }
}


// If you would like to create a formatter for the Accumulo shell then create a
// class like the following. This class and Typo will then need to be placed on
// the Accumulo classpath.

class MyFormatter extends TypoFormatter {
  public MyFormatter() {
    super(new MyTypo());
  }
}

/*
 * A Typo Constraint can also be created like the Formatter was.
 */

class MyConstraint extends TypoConstraint {
  public MyConstraint() {
    super(new MyTypo());
  }
}

/**
 * A simple example that reads from and write to Accumulo using Typo.
 */

public class TypoExample {
  public static void main(String[] args) throws Exception {
    MockInstance mi = new MockInstance();
    Connector conn = mi.getConnector("root", "secret");
    conn.tableOperations().create("foo");
    
    insertData(conn);
    scanData(conn);
  }
  
  /*
   * Insert data using java types
   */
  static void insertData(Connector conn) throws Exception {
    BatchWriter bw = conn.createBatchWriter("foo", 1000000, 60000, 2);
    
    MyTypo myTypo = new MyTypo();
    
    for (long row = -4; row < 4; row++) {
      MyTypo.Mutation mut = myTypo.newMutation(row);
      mut.put("sq", Math.pow(row, 2), "val");
      mut.put("cube", Math.pow(row, 3), "val");
      bw.addMutation(mut);
    }
    
    bw.close();
  }
  
  static void scanData(Connector conn) throws Exception {
    MyTypo myTypo = new MyTypo();
    Scanner scanner = conn.createScanner("foo", Constants.NO_AUTHS);
    
    // you can create a range using java types
    scanner.setRange(myTypo.newRange(-2l, 3l));
    
    MyTypo.Scanner typoScanner = myTypo.newScanner(scanner);
    
    // you can fetch columns using Java types
    typoScanner.fetchColumnFamily("sq");

    long rowSum = 0;
    double cqSum = 0;
    
    // read data from Accumulo using java types
    for (Entry<MyTypo.Key,String> entry : typoScanner) {
      rowSum += entry.getKey().getRow();
      cqSum += entry.getKey().getColumnQualifier();
      System.out.println(entry);
    }
    
    System.out.println("rowSum : " + rowSum);
    System.out.println("cqSum  : " + cqSum);
  }
}
