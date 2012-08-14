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
package org.apache.accumulo.client.typo;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.client.typo.encoders.Encoder;
import org.apache.accumulo.client.typo.encoders.Lexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.format.Formatter;

/**
 * 
 */
public class TypoFormatter<RT,CFT,CQT,VT> implements Formatter {
  
  private TypoEncoders<RT,CFT,CQT,VT> ae;
  private Iterator<Entry<Key,Value>> iter;
  
  public TypoFormatter(Lexicoder<RT> rowLexEnc, Lexicoder<CFT> colfLexEnc, Lexicoder<CQT> colqLexEnc, Encoder<VT> valEnc) {
    ae = new TypoEncoders<RT,CFT,CQT,VT>(rowLexEnc, colfLexEnc, colqLexEnc, valEnc);
  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }
  
  @Override
  public String next() {
    Entry<Key,Value> next = iter.next();
    TypoKey<RT,CFT,CFT> typoKey = new TypoKey<RT,CFT,CFT>(next.getKey(), ae.rowLexEnc, ae.colfLexEnc, ae.colfLexEnc);
    
    return typoKey.getRow() + " " + typoKey.getColumnFamily() + " " + typoKey.getColumnQualifier() + "  [" + typoKey.getColumnVisibility() + "] "
        + ae.valEnc.fromBytes(next.getValue().get());
  }
  
  @Override
  public void remove() {
    iter.remove();
  }
  
  @Override
  public void initialize(Iterable<Entry<Key,Value>> scanner, boolean printTimestamps) {
    this.iter = scanner.iterator();
  }
  
}