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
package org.apache.accumulo.client.typo.tuples;

/**
 * 
 */
public class Pair<A,B> implements Comparable<Pair<A,B>> {
  private A first;
  private B second;
  
  public Pair(A first, B second) {
    this.first = first;
    this.second = second;
  }
  
  public A getFirst() {
    return first;
  }
  
  public B getSecond() {
    return second;
  }
  
  public String toString() {
    return "(" + first + "," + second + ")";
  }
  
  @Override
  public int compareTo(Pair<A,B> o) {
    int cmp = ((Comparable<A>) first).compareTo(o.first);
    if (cmp == 0) {
      cmp = ((Comparable<B>) second).compareTo(o.second);
    }
    
    return cmp;
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof Pair) {
      Pair<A,B> p = (Pair<A,B>) o;
      return first.equals(p.first) && second.equals(p.second);
    }
    
    return false;
  }
  
  @Override
  public int hashCode() {
    return first.hashCode() + second.hashCode();
  }

}
