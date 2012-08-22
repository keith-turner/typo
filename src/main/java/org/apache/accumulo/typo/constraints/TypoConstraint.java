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
package org.apache.accumulo.typo.constraints;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.constraints.Constraint;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.typo.Typo;
import org.apache.accumulo.typo.TypoEncoders;

/**
 * A {@link Constraint} that ensures Typo can successfully decode the fields in a {@link Mutation}.
 * 
 */
public class TypoConstraint implements Constraint {
  
  private Typo<?,?,?,?> typo;

  public TypoConstraint(Typo<?,?,?,?> typo) {
    this.typo = typo;
  }
  
  @Override
  public String getViolationDescription(short violationCode) {
    switch (violationCode) {
      case 1:
        return "Failed to decode row";
      case 2:
        return "Failed to decode column family";
      case 3:
        return "Failed to decode column qualifier";
      case 4:
        return "Failed to decode value";
      default:
        throw new IllegalArgumentException("Uknown code");
    }
  }
  
  @Override
  public List<Short> check(Environment env, Mutation mutation) {
    
    TypoEncoders<?,?,?,?> encoders = typo.getEncoders();
    List<Short> ret = null;
    
    try {
      encoders.getRowLexicoder().decode(mutation.getRow());
    } catch (Exception e) {
      ret = new ArrayList<Short>();
      ret.add((short) 1);
    }
    
    for (ColumnUpdate cu : mutation.getUpdates()) {
      try {
        encoders.getColumnFamilyLexicoder().decode(cu.getColumnFamily());
      } catch (Exception e) {
        if (ret == null)
          ret = new ArrayList<Short>();
        ret.add((short) 2);
      }
      
      try {
        encoders.getColumnQualifierLexicoder().decode(cu.getColumnQualifier());
      } catch (Exception e) {
        if (ret == null)
          ret = new ArrayList<Short>();
        ret.add((short) 3);
      }
      
      try {
        encoders.getValueEncoder().decode(cu.getValue());
      } catch (Exception e) {
        if (ret == null)
          ret = new ArrayList<Short>();
        ret.add((short) 4);
      }
      
    }
    
    return ret;
  }
  
}
