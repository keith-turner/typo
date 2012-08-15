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
package org.apache.accumulo.client.typo.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map.Entry;

import org.apache.accumulo.client.typo.Typo;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 
 */
public class TypoInputFormat extends InputFormatBase<Typo<?,?,?,?>.Key,Object> {
  
  private Typo<?,?,?,?> typo;
  
  public TypoInputFormat(Typo<?,?,?,?> typo) {
    this.typo = typo;
  }

  @Override
  public RecordReader<Typo<?,?,?,?>.Key,Object> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
    
    return new RecordReaderBase<Typo<?,?,?,?>.Key,Object>() {
      private Typo<?,?,?,?>.TypoIterator iter;
      
      @Override
      public void initialize(InputSplit inSplit, TaskAttemptContext attempt) throws IOException {
        super.initialize(inSplit, attempt);
        iter = typo.newIterator(scannerIterator);
      }
      
      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!iter.hasNext())
          return false;
        
        Entry<Typo<?,?,?,?>.Key,?> entry = iter.next();
        
        currentV = entry.getValue();
        numKeysRead++;
        currentKey = entry.getKey().getKey();
        currentK = entry.getKey();
        return true;
      }
    };
  }
  
  public static <CFT,CQT> void fetchColumns(Typo<?,CFT,CQT,?> typo, Configuration conf, Collection<Pair<CFT,CQT>> columnFamilyColumnQualifierPairs) {
    
    ArrayList<Pair<Text,Text>> colPairs = new ArrayList<Pair<Text,Text>>();
    
    for (Pair<CFT,CQT> pair : columnFamilyColumnQualifierPairs) {
      Text cf = new Text(typo.getEncoders().getColumnFamilyLexicoder().encode(pair.getFirst()));
      Text cq = null;
      if (pair.getSecond() != null)
        cq = new Text(typo.getEncoders().getColumnQualifierLexicoder().encode(pair.getSecond()));
      
      colPairs.add(new Pair<Text,Text>(cf, cq));
    }
    
    fetchColumns(conf, colPairs);
  }

}
