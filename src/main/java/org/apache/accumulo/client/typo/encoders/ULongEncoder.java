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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.accumulo.client.typo.encoders.util.FixedByteArrayOutputStream;

/**
 * Unsigned long encoder. The lexicographic encoding sorts first 0l and -1l last. This encoding does not correspond to the sort of Long because it does not
 * consider the sign bit. If Java had an unsigned long type, this encoder would correspond to its sort order.
 */
public class ULongEncoder extends LexEncoder<Long> {
  
  @Override
  public byte[] toBytes(Long l) {
    try {
      byte ret[] = new byte[8];
      DataOutputStream dos = new DataOutputStream(new FixedByteArrayOutputStream(ret));
      dos.writeLong(l);
      return ret;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
  
  @Override
  public Long fromBytes(byte[] data) {
    try {
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
      long l = dis.readLong();
      return l;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
  
}
