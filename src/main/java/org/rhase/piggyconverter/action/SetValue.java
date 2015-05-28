/**
 * Copyright 2015 Ryoji Hasegawa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.rhase.piggyconverter.action;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.rhase.piggyconverter.Initable;
import org.rhase.piggyconverter.PCRuntimeException;
import org.rhase.piggyconverter.SchemaWrapper;
import org.rhase.piggyconverter.TupleWrapper;

public class SetValue extends SingleColAction implements Initable {
  private String val;

  @SuppressWarnings("rawtypes")
  private Comparable value;

  public void setValue(String value) {
    this.val = value;
  }

  @Override
  public void init(SchemaWrapper outputSchema) {
    try {
      value = TupleWrapper.castFromString(outputSchema.getField(column).type, val);
    } catch (FrontendException e) {
      throw new PCRuntimeException("Field does not exists.", e);
    }
  }

  @Override
  public void exec(TupleWrapper out) throws TupleToIgnoreException {
    out.set(column, value);
  }
}
