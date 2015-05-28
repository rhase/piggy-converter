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

import static org.junit.Assert.assertEquals;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.junit.BeforeClass;
import org.junit.Test;

public class PiggyConverterUDFTest {
  private static Schema input;

  @BeforeClass
  public static void beforeClass() {
    input = new Schema();
    input.add(new Schema.FieldSchema("col1", DataType.INTEGER));
    input.add(new Schema.FieldSchema("col2", DataType.CHARARRAY));
  }

  private void doTestoutputSchema(String[] outdef, String[] expectedArry) {
    String expected = buildExpected(expectedArry);

    String[] param;
    if (outdef == null) {
      // param = new String[] { pcScript };
      param = new String[] { "dummy.pc" };
    } else {
      param = new String[outdef.length + 1];
      param[0] = "dummy.pc";
      System.arraycopy(outdef, 0, param, 1, outdef.length);
    }
    String actual = new PiggyConverterUDF(param).outputSchema(input).prettyPrint();

    // System.out.println("* expected\n" + expected);
    // System.out.println("* actual\n" + actual);

    assertEquals(expected, actual);
  }

  private String buildExpected(String[] expectedArry) {
    StringBuilder sb = new StringBuilder();
    sb.append("{\n    conved: (\n        ");

    boolean not1st = false;
    for (String col : expectedArry) {
      if (not1st) {
        sb.append(",\n        ");
      } else {
        not1st = true;
      }
      sb.append(col);
    }
    sb.append("\n    )\n}");
    return sb.toString();
  }

  @Test
  public void testoutputSchemaNoOutDef() {
    doTestoutputSchema(null, new String[] { "col1: int", "col2: chararray" });
  }

  @Test
  public void testoutputSchemaOnlyExisting() {
    doTestoutputSchema(new String[] { "col1" }, new String[] { "col1: int" });
  }

  @Test
  public void testoutputSchemaOnlyNew() {
    doTestoutputSchema(new String[] { "col3:long" }, new String[] { "col3: long" });
  }

  @Test
  public void testoutputSchemaExistingAndNew() {
    doTestoutputSchema(new String[] { "col1", "col3:long" }, new String[] { "col1: int", "col3: long" });
  }

  @Test
  public void testoutputSchemaAster() {
    doTestoutputSchema(new String[] { "*" }, new String[] { "col1: int", "col2: chararray" });
  }

  @Test
  public void testoutputSchemaAsterAndNew() {
    doTestoutputSchema(new String[] { "*", "col3:long" }, new String[] { "col1: int", "col2: chararray", "col3: long" });
  }
}
