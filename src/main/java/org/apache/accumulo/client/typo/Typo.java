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
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.client.typo.encoders.Encoder;
import org.apache.accumulo.client.typo.encoders.Lexicoder;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;

/**
 * 
 */
public abstract class Typo<RT,CFT,CQT,VT> {
  
  private TypoEncoders<RT,CFT,CQT,VT> ae;

  public class Scanner implements Iterable<Entry<TypoKey,VT>> {
    
    private ScannerBase scanner;
    
    public Scanner(ScannerBase scanner) {
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
    public Iterator<Entry<TypoKey,VT>> iterator() {
      return new TypoIterator(scanner.iterator());
    }
  }

  public class TypoKey {
    private RT row;
    private CFT cf;
    private CQT cq;
    private Text cv;
    private long ts;
    private boolean isDelete = false;
    
    
    public TypoKey() {}
    
    public TypoKey setKey(Key key) {
      row = ae.rowLexEnc.fromBytes(key.getRowData().toArray());
      cf = ae.colfLexEnc.fromBytes(key.getColumnFamilyData().toArray());
      cq = ae.colqLexEnc.fromBytes(key.getColumnQualifierData().toArray());
      cv = key.getColumnVisibility();
      ts = key.getTimestamp();
      isDelete = key.isDeleted();
      return this;
    }
    
    public Key getKey() {
      Key key = new Key(ae.rowLexEnc.toBytes(getRow()), ae.colfLexEnc.toBytes(cf), ae.colqLexEnc.toBytes(cq), TextUtil.getBytes(cv), ts);
      key.setDeleted(isDelete);
      return key;
    }
    
    public RT getRow() {
      return row;
    }

    public TypoKey setRow(RT row) {
      this.row = row;
      return this;
    }
    
    public CFT getColumnFamily() {
      return cf;
    }

    public TypoKey setColumnFamily(CFT cf) {
      this.cf = cf;
      return this;
    }
    
    public CQT getColumnQualifier() {
      return cq;
    }

    public TypoKey setColumnQualifier(CQT cq) {
      this.cq = cq;
      return this;
    }
    
    public Text getColumnVisibility() {
      return cv;
    }

    public TypoKey getColumnVisibility(Text cv) {
      this.cv = cv;
      return this;
    }
    
    public long getTimestamp() {
      return ts;
    }

    public TypoKey setTimestamp(long ts) {
      this.ts = ts;
      return this;
    }
    
    public boolean isDeleted() {
      return isDelete;
    }
    
    public TypoKey setDeleted(boolean del) {
      this.isDelete = del;
      return this;
    }
    
    public String toString() {
      return row + " " + cf + " " + cq + " [" + cv + "] " + ts;
    }
  }
  
  public class TypoIterator implements Iterator<Entry<TypoKey,VT>> {
    
    private Iterator<Entry<Key,Value>> iter;

    private class TypoEntry implements Map.Entry<TypoKey,VT> {
      
      private Entry<Key,Value> srcEntry;
      private TypoKey tk;
      private VT val;
      
      public TypoEntry(Entry<Key,Value> srcEntry, TypoKey tk, VT val) {
        this.srcEntry = srcEntry;
        this.tk = tk;
        this.val = val;
      }
      
      @Override
      public TypoKey getKey() {
        return tk;
      }
      
      @Override
      public VT getValue() {
        return val;
      }
      
      @Override
      public VT setValue(VT value) {
        return ae.valEnc.fromBytes(srcEntry.setValue(new Value(ae.valEnc.toBytes(value))).get());
      }
      
      public String toString() {
        return tk + " " + val;
      }
    }
    
    public TypoIterator(Iterator<Entry<Key,Value>> iterator) {
      this.iter = iterator;
    }
    
    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }
    
    @Override
    public Entry<TypoKey,VT> next() {
      Entry<Key,Value> srcEntry = iter.next();
      TypoKey tk = new TypoKey().setKey(srcEntry.getKey());
      VT val = ae.valEnc.fromBytes(srcEntry.getValue().get());
      
      return new TypoEntry(srcEntry, tk, val);
    }
    
    @Override
    public void remove() {
      iter.remove();
    }
    
  }

  public class Mutation extends org.apache.accumulo.core.data.Mutation {
    
    public Mutation(RT row) {
      super(new Text(ae.rowLexEnc.toBytes(row)));
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

  /**
   * @param longEncoder
   * @param stringEncoder
   * @param doubleEncoder
   * @param stringEncoder2
   */
  public Typo(Lexicoder<RT> rowLexEnc, Lexicoder<CFT> colfLexEnc, Lexicoder<CQT> colqLexEnc, Encoder<VT> valEnc) {
    ae = new TypoEncoders<RT,CFT,CQT,VT>(rowLexEnc, colfLexEnc, colqLexEnc, valEnc);
  }

  public Scanner newScanner(ScannerBase scanner) {
    return new Scanner(scanner);
  }
  
  public Mutation newMutation(RT row) {
    return new Mutation(row);
  }

  /**
   * This method can be used in conjunction with the {@link RowIterator}
   * 
   * @param iter
   * @return
   */
  public TypoIterator newIterator(Iterator<Entry<Key,Value>> iter) {
    return new TypoIterator(iter);
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
  
  public TypoKey newKey() {
    return new TypoKey();
  }
  
  public TypoEncoders<RT,CFT,CQT,VT> getEncoders() {
    return ae;
  }
}
