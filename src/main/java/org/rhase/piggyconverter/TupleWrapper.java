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

package org.rhase.piggyconverter;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class TupleWrapper {
  public static class NullVal {
  }

  private static NullVal nullVal = new NullVal();
  private Tuple tuple;
  private Schema schema;
  private boolean hasNullVal = false;

  public TupleWrapper(Schema schema) {
    this.schema = schema;
    TupleFactory tFactory = TupleFactory.getInstance();
    this.tuple = tFactory.newTuple(schema.size());
  }

  public TupleWrapper(Schema schema, Tuple tuple) {
    if (schema.size() != tuple.size())
      throw new PCRuntimeException("Size of schema and tuple aren't equal." + "\n  schema : " + schema.prettyPrint()
          + "\n  tuple : " + tuple);

    this.schema = schema;
    this.tuple = tuple;
  }

  public boolean hasNullVal() {
    return hasNullVal;
  }

  public void mergeTupleByAlias(TupleWrapper src) throws PCException {
    for (Schema.FieldSchema field : src.getSchema().getFields()) {
      this.set(field.alias, src.get(field.alias));
    }
  }

  public Tuple getTuple() {

    if (!hasNullVal) {
      for (int i = 0; i < tuple.size(); i++) {
        try {
          if (tuple.get(i) instanceof NullVal)
            tuple.set(i, null);
        } catch (ExecException e) {
          throw new PCRuntimeException("Bug! Should not reach here!!", e);
        }
      }
    }

    return tuple;
  }

  public Schema getSchema() {
    return schema;
  }

  public Object get(String alias) throws PCException {
    try {
      int pos = schema.getPosition(alias);
      if (pos < 0)
        throw new PCException("Field does not exist : " + alias);

      Object ret = tuple.get(pos);
      if (ret == null) {
        tuple.set(pos, nullVal);
        hasNullVal = true;
      }

      return ret != null ? ret : nullVal;
    } catch (ExecException | IndexOutOfBoundsException e) {
      throw new PCException("Field exists in schema, but position exceeds tuple size. Field alias : " + alias, e);
    } catch (FrontendException e) {
      throw new PCException("Could not get position of field \"" + alias + "\".", e);
    }
  }

  public String getString(String alias) throws PCException {
    return get(alias).toString();
  }

  public void set(String name, Object value) {
    try {
      tuple.set(schema.getPosition(name), value);
    } catch (ExecException e) {
      throw new PCRuntimeException("Field exists in schema, but position exceeds tuple size.", e);
    } catch (FrontendException e) {
      throw new PCRuntimeException("Field does not exists.", e);
    }
  }

  public int size() {
    return tuple.size();
  }

  @SuppressWarnings("rawtypes")
  public static Comparable castFromString(Byte type, String str) {
    if (type == DataType.CHARARRAY)
      return str;
    else if (type == DataType.BOOLEAN)
      return Boolean.parseBoolean(str);
    else if (type == DataType.INTEGER)
      return Integer.parseInt(str);
    else if (type == DataType.LONG)
      return Long.parseLong(str);
    else if (type == DataType.DOUBLE)
      return Double.parseDouble(str);
    else if (type == DataType.FLOAT)
      return Float.parseFloat(str);
    throw new PCRuntimeException("Data type not supported by piggy converter : " + DataType.findTypeName(type));
  }
}
