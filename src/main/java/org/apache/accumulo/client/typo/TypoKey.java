package org.apache.accumulo.client.typo;

import org.apache.accumulo.client.typo.encoders.Lexicoder;
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
  private boolean isDelete = false;

  private Lexicoder<RT> rowLexEnc;
  private Lexicoder<CFT> colfLexEnc;
  private Lexicoder<CQT> colqLexEnc;
  
  public TypoKey(Lexicoder<RT> rowLexEnc, Lexicoder<CFT> colfLexEnc, Lexicoder<CQT> colqLexEnc) {
    this(new Key(), rowLexEnc, colfLexEnc, colqLexEnc);
  }

  public TypoKey(Key key, Lexicoder<RT> rowLexEnc, Lexicoder<CFT> colfLexEnc, Lexicoder<CQT> colqLexEnc) {
    this.rowLexEnc = rowLexEnc;
    this.colfLexEnc = colfLexEnc;
    this.colqLexEnc = colqLexEnc;
    setKey(key);
  }
  
  public TypoKey<RT,CFT,CQT> setKey(Key key) {
    row = rowLexEnc.fromBytes(key.getRowData().toArray());
    cf = colfLexEnc.fromBytes(key.getColumnFamilyData().toArray());
    cq = colqLexEnc.fromBytes(key.getColumnQualifierData().toArray());
    cv = key.getColumnVisibility();
    ts = key.getTimestamp();
    isDelete = key.isDeleted();
    return this;
  }
  
  public Key getKey() {
    Key key = new Key(rowLexEnc.toBytes(getRow()), colfLexEnc.toBytes(cf), colqLexEnc.toBytes(cq), TextUtil.getBytes(cv), ts);
    key.setDeleted(isDelete);
    return key;
  }
  
  public RT getRow() {
    return row;
  }
  
  public TypoKey<RT,CFT,CQT> setRow(RT row) {
    this.row = row;
    return this;
  }

  public CFT getColumnFamily() {
    return cf;
  }
  
  public TypoKey<RT,CFT,CQT> setColumnFamily(CFT cf) {
    this.cf = cf;
    return this;
  }

  public CQT getColumnQualifier() {
    return cq;
  }
  
  public TypoKey<RT,CFT,CQT> setColumnQualifier(CQT cq) {
    this.cq = cq;
    return this;
  }

  public Text getColumnVisibility() {
    return cv;
  }
  
  public TypoKey<RT,CFT,CQT> getColumnVisibility(Text cv) {
    this.cv = cv;
    return this;
  }

  public long getTimestamp() {
    return ts;
  }
  
  public TypoKey<RT,CFT,CQT> setTimestamp(long ts) {
    this.ts = ts;
    return this;
  }
  
  public boolean isDeleted() {
    return isDelete;
  }
  
  public TypoKey<RT,CFT,CQT> setDeleted(boolean del) {
    this.isDelete = del;
    return this;
  }

  public String toString() {
    return row + " " + cf + " " + cq + " [" + cv + "] " + ts;
  }
}
