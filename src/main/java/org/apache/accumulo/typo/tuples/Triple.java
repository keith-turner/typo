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
package org.apache.accumulo.typo.tuples;

/**
 * 
 */
public class Triple<A,B,C> implements Comparable<Triple<A,B,C>> {
  private A first;
  private B second;
  private C third;
  
  public Triple(A first, B second, C third) {
    this.first = first;
    this.second = second;
    this.third = third;
  }
  
  public A getFirst() {
    return first;
  }
  
  public B getSecond() {
    return second;
  }

  public C getThird() {
    return third;
  }
  
  public String toString() {
    return "(" + first + "," + second + "," + third + ")";
  }
  
  @Override
  public int compareTo(Triple<A,B,C> o) {
    int cmp = ((Comparable<A>) first).compareTo(o.first);
    if (cmp == 0) {
      cmp = ((Comparable<B>) second).compareTo(o.second);
      if (cmp == 0) {
        cmp = ((Comparable<C>) third).compareTo(o.third);
      }
    }
    
    return cmp;
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof Triple) {
      Triple<A,B,C> p = (Triple<A,B,C>) o;
      return first.equals(p.first) && second.equals(p.second) && third.equals(p.third);
    }
    
    return false;
  }
  
  @Override
  public int hashCode() {
    return first.hashCode() + second.hashCode() + third.hashCode();
  }
}
