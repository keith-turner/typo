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
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

/**
 * 
 */
public abstract class Typo<RT,CFT,CQT,VT> {
  
  private TypoEncoders<RT,CFT,CQT,VT> ae;

  /**
   * @param longEncoder
   * @param stringEncoder
   * @param doubleEncoder
   * @param stringEncoder2
   */
  public Typo(Lexicoder<RT> rowLexEnc, Lexicoder<CFT> colfLexEnc, Lexicoder<CQT> colqLexEnc, Encoder<VT> valEnc) {
    ae = new TypoEncoders<RT,CFT,CQT,VT>(rowLexEnc, colfLexEnc, colqLexEnc, valEnc);
  }

  public TypoScanner<RT,CFT,CQT,VT> newScanner(ScannerBase scanner) {
    return new TypoScanner<RT,CFT,CQT,VT>(scanner, ae);
  }
  
  public TypoMutation<RT,CFT,CQT,VT> newMutation(RT row) {
    return new TypoMutation<RT,CFT,CQT,VT>(row, ae);
  }

  /**
   * This method can be used in conjunction with the {@link RowIterator}
   * 
   * @param iter
   * @return
   */
  public TypoIterator<RT,CFT,CQT,VT> newIterator(Iterator<Entry<Key,Value>> iter) {
    return new TypoIterator<RT,CFT,CQT,VT>(iter, ae);
  }

  public Range newRange(RT row) {
    return new Range(new Text(ae.rowLexEnc.toBytes(row)));
  }

  public Range newRange(RT start, RT end) {
    return new Range(start == null ? null : new Text(ae.rowLexEnc.toBytes(start)), end == null ? null : new Text(ae.rowLexEnc.toBytes(end)));
  }
  
  public Range newRange(RT start, boolean startInc, RT end, boolean endInc) {
    return new Range(start == null ? null : new Text(ae.rowLexEnc.toBytes(start)), startInc, end == null ? null : new Text(ae.rowLexEnc.toBytes(end)), endInc);
  }
  
  public TypoKey<RT,CFT,CQT> newKey() {
    return new TypoKey<RT,CFT,CQT>(ae.rowLexEnc, ae.colfLexEnc, ae.colqLexEnc);
  }
  
  public TypoEncoders<RT,CFT,CQT,VT> getEncoders() {
    return ae;
  }
}
