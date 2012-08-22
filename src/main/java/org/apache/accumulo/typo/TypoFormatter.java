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
package org.apache.accumulo.typo;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.format.Formatter;

/**
 * 
 */
public class TypoFormatter implements Formatter {
  
  private Iterator<Entry<Key,Value>> iter;
  private Typo<?,?,?,?> typo;
  private boolean printTimestamps;
  
  public TypoFormatter(Typo<?,?,?,?> typo) {
    this.typo = typo;
  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }
  
  @Override
  public String next() {
    Entry<Key,Value> next = iter.next();
    
    Typo<?,?,?,?>.Key typoKey = typo.decode(next.getKey());

    return typoKey.getRow() + " " + typoKey.getColumnFamily() + " " + typoKey.getColumnQualifier() + "  [" + typoKey.getColumnVisibility() + "] "
        + (printTimestamps ? +typoKey.getTimestamp() + " " : "")
        + typo.getEncoders().getValueEncoder().decode(next.getValue().get());
  }
  
  @Override
  public void remove() {
    iter.remove();
  }
  
  @Override
  public void initialize(Iterable<Entry<Key,Value>> scanner, boolean printTimestamps) {
    this.iter = scanner.iterator();
    this.printTimestamps = printTimestamps;
  }
  
}
