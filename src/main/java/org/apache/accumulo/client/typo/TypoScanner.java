package org.apache.accumulo.client.typo;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.hadoop.io.Text;

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

/**
 * 
 */
public class TypoScanner<RT,CFT,CQT,VT> implements Iterable<Entry<TypoKey<RT,CFT,CQT>,VT>> {
  
  private TypoEncoders<RT,CFT,CQT,VT> ae;

  private ScannerBase scanner;
  
  public TypoScanner(ScannerBase scanner, TypoEncoders<RT,CFT,CQT,VT> ae) {
    this.ae = ae;
    this.scanner = scanner;
  }
  
  public void fetchColumnFamily(CFT cf) {
    scanner.fetchColumnFamily(new Text(ae.colfLexEnc.toBytes(cf)));
  }
  
  public void fetchColumn(CFT cf, CQT cq) {
    scanner.fetchColumn(new Text(ae.colfLexEnc.toBytes(cf)), new Text(ae.colqLexEnc.toBytes(cq)));
  }
  
  public ScannerBase getScanner() {
    return scanner;
  }

  
  @Override
  public Iterator<Entry<TypoKey<RT,CFT,CQT>,VT>> iterator() {
    return new TypoIterator<RT,CFT,CQT,VT>(scanner.iterator(), ae);
  }
  

}
