package org.apache.accumulo.client.typo;

import org.apache.accumulo.client.typo.encoders.LexEncoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.util.TextUtil;
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
public class TypoKey<RT,CFT,CQT> {
  
  private RT row;
  private CFT cf;
  private CQT cq;
  private Text cv;
  private long ts;

  private LexEncoder<RT> rowLexEnc;
  private LexEncoder<CFT> colfLexEnc;
  private LexEncoder<CQT> colqLexEnc;
  
  public TypoKey(LexEncoder<RT> rowLexEnc, LexEncoder<CFT> colfLexEnc, LexEncoder<CQT> colqLexEnc) {
    this(new Key(), rowLexEnc, colfLexEnc, colqLexEnc);
  }

  public TypoKey(Key key, LexEncoder<RT> rowLexEnc, LexEncoder<CFT> colfLexEnc, LexEncoder<CQT> colqLexEnc) {
    this.rowLexEnc = rowLexEnc;
    this.colfLexEnc = colfLexEnc;
    this.colqLexEnc = colqLexEnc;
    setKey(key);
  }
  
  public void setKey(Key key) {
    row = rowLexEnc.fromBytes(key.getRowData().toArray());
    cf = colfLexEnc.fromBytes(key.getColumnFamilyData().toArray());
    cq = colqLexEnc.fromBytes(key.getColumnQualifierData().toArray());
    cv = key.getColumnVisibility();
    ts = key.getTimestamp();
  }
  
  public Key getKey() {
    return new Key(rowLexEnc.toBytes(getRow()), colfLexEnc.toBytes(cf), colqLexEnc.toBytes(cq), TextUtil.getBytes(cv), ts);
  }
  
  public RT getRow() {
    return row;
  }
  
  public void setRow(RT row) {
    this.row = row;
  }

  public CFT getColumnFamily() {
    return cf;
  }
  
  public void setColumnFamily(CFT cf) {
    this.cf = cf;
  }

  public CQT getColumnQualifier() {
    return cq;
  }
  
  public void setColumnQualifier(CQT cq) {
    this.cq = cq;
  }

  public Text getColumnVisibility() {
    return cv;
  }
  
  public void getColumnVisibility(Text cv) {
    this.cv = cv;
  }

  public long getTimestamp() {
    return ts;
  }
  
  public void setTimestamp(long ts) {
    this.ts = ts;
  }
  
  public String toString() {
    return row + " " + cf + " " + cq + " [" + cv + "] " + ts;
  }
}
