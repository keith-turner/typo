package org.apache.accumulo.client.typo;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
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
public class TypoMutation<RT,CFT,CQT,VT> extends Mutation {
  
  private TypoEncoders<RT,CFT,CQT,VT> ae;
  
  public TypoMutation(RT row, TypoEncoders<RT,CFT,CQT,VT> ae) {
    super(new Text(ae.rowLexEnc.toBytes(row)));
    this.ae = ae;
  }
  
  public void put(CFT cf, CQT cq, ColumnVisibility cv, long ts, VT val) {
    super.put(new Text(ae.colfLexEnc.toBytes(cf)), new Text(ae.colqLexEnc.toBytes(cq)), cv, ts, new Value(ae.valEnc.toBytes(val)));
  }
  
  public void put(CFT cf, CQT cq, ColumnVisibility cv, VT val) {
    super.put(new Text(ae.colfLexEnc.toBytes(cf)), new Text(ae.colqLexEnc.toBytes(cq)), cv, new Value(ae.valEnc.toBytes(val)));
  }
  
  public void put(CFT cf, CQT cq, VT val) {
    super.put(new Text(ae.colfLexEnc.toBytes(cf)), new Text(ae.colqLexEnc.toBytes(cq)), new Value(ae.valEnc.toBytes(val)));
  }
  
  public void put(CFT cf, CQT cq, long ts, VT val) {
    super.put(new Text(ae.colfLexEnc.toBytes(cf)), new Text(ae.colqLexEnc.toBytes(cq)), ts, new Value(ae.valEnc.toBytes(val)));
  }
  
  public void putDelete(CFT cf, CQT cq, ColumnVisibility cv, long ts) {
    super.putDelete(new Text(ae.colfLexEnc.toBytes(cf)), new Text(ae.colqLexEnc.toBytes(cq)), cv, ts);
  }
  
  public void putDelete(CFT cf, CQT cq, ColumnVisibility cv) {
    super.putDelete(new Text(ae.colfLexEnc.toBytes(cf)), new Text(ae.colqLexEnc.toBytes(cq)), cv);
  }
  
  public void putDelete(CFT cf, CQT cq) {
    super.putDelete(new Text(ae.colfLexEnc.toBytes(cf)), new Text(ae.colqLexEnc.toBytes(cq)));
  }
  
  public void putDelete(CFT cf, CQT cq, long ts) {
    super.putDelete(new Text(ae.colfLexEnc.toBytes(cf)), new Text(ae.colqLexEnc.toBytes(cq)), ts);
  }
}
