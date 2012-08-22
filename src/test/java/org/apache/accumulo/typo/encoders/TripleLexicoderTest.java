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
package org.apache.accumulo.typo.encoders;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.typo.encoders.StringLexicoder;
import org.apache.accumulo.typo.encoders.TripleLexicoder;
import org.apache.accumulo.typo.tuples.Triple;

/**
 * 
 */
public class TripleLexicoderTest extends LexicoderTest {
  public void testSortOrder() {
    
    TripleLexicoder<String,String,String> tl = new TripleLexicoder<String,String,String>(new StringLexicoder(), new StringLexicoder(), new StringLexicoder());
    
    List<Triple<String,String,String>> testData = new ArrayList<Triple<String,String,String>>();
    
    testData.add(new Triple<String,String,String>("a", "b", "c"));
    testData.add(new Triple<String,String,String>("a", "b", "e"));
    testData.add(new Triple<String,String,String>("a", "d", "e"));
    testData.add(new Triple<String,String,String>("a", "d", "c"));
    testData.add(new Triple<String,String,String>("aaa", "b", "c"));
    testData.add(new Triple<String,String,String>("a", "bb", "c"));
    testData.add(new Triple<String,String,String>("a", "b", "cc"));
    testData.add(new Triple<String,String,String>("abc", "", ""));
    testData.add(new Triple<String,String,String>("b", "b", "c"));
    testData.add(new Triple<String,String,String>("", "b", "c"));
    testData.add(new Triple<String,String,String>("", "", "c"));
    testData.add(new Triple<String,String,String>("", "", ""));
    
    Triple<String,String,String>[] array = testData.toArray(new Triple[0]);
    
    assertSortOrder(tl, array);
    
  }
}
