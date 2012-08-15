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

import org.apache.accumulo.client.typo.encoders.Encoder;
import org.apache.accumulo.client.typo.encoders.Lexicoder;

/**
 * 
 */
public class TypoEncoders<RT,CFT,CQT,VT> {
  Lexicoder<RT> rowLexEnc;
  Lexicoder<CFT> colfLexEnc;
  Lexicoder<CQT> colqLexEnc;
  Encoder<VT> valEnc;
  
  public TypoEncoders(Lexicoder<RT> rowLexEnc, Lexicoder<CFT> colfLexEnc, Lexicoder<CQT> colqLexEnc, Encoder<VT> valEnc) {
    this.rowLexEnc = rowLexEnc;
    this.colfLexEnc = colfLexEnc;
    this.colqLexEnc = colqLexEnc;
    this.valEnc = valEnc;
  }
  
  public Lexicoder<RT> getRowLexicoder() {
    return rowLexEnc;
  }
  
  public Lexicoder<CFT> getColumnFamilyLexicoder() {
    return colfLexEnc;
  }
  
  public Lexicoder<CQT> getColumnQualifierLexicoder() {
    return colqLexEnc;
  }
  
  public Encoder<VT> getValueEncoder() {
    return valEnc;
  }
}
