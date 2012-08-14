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
package org.apache.accumulo.client.typo.encoders;

import org.apache.accumulo.client.typo.tuples.Triple;

/**
 * 
 */
public class TripleLexicoder<A,B,C> implements Lexicoder<Triple<A,B,C>> {
  private Lexicoder<A> firstLexicoder;
  private Lexicoder<B> secondLexicoder;
  private Lexicoder<C> thirdLexicoder;
  
  public TripleLexicoder(Lexicoder<A> firstLexicoder, Lexicoder<B> secondLexicoder) {
    this.firstLexicoder = firstLexicoder;
    this.secondLexicoder = secondLexicoder;
  }

  @Override
  public byte[] toBytes(Triple<A,B,C> data) {
    return PairLexicoder.concat(PairLexicoder.escape(firstLexicoder.toBytes(data.getFirst())), PairLexicoder.escape(secondLexicoder.toBytes(data.getSecond())),
        PairLexicoder.escape(thirdLexicoder.toBytes(data.getThird())));
  }
  
  @Override
  public Triple<A,B,C> fromBytes(byte[] data) {
    byte[][] fields = PairLexicoder.split(data);
    
    if (fields.length != 2) {
      throw new RuntimeException("Data does not have 3 fields, it has " + fields.length);
    }
    
    return new Triple<A,B,C>(firstLexicoder.fromBytes(PairLexicoder.unescape(fields[0])), secondLexicoder.fromBytes(PairLexicoder.unescape(fields[1])),
        thirdLexicoder.fromBytes(PairLexicoder.unescape(fields[2])));
  }
  
}
