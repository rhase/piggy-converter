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

import static org.rhase.util.hadooputil.HadoopUtil.getDistCacheDFSPath;
import static org.rhase.util.hadooputil.HadoopUtil.getDistCacheFileName;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.antlr.runtime.RecognitionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.SchemaMergeException;
import org.apache.pig.impl.util.UDFContext;
import org.rhase.piggyconverter.action.TupleToIgnoreException;
import org.rhase.util.hadooputil.HadoopUtil;

public class PiggyConverterUDF extends EvalFunc<Tuple> {
  private String script_path;
  private Rules rules = null;
  private String[] outputCols = null;
  private Schema inSchema = null;
  private Schema workSchema = null;
  private Schema outInnerSchema = null;
  private Schema outSchema = null;
  private List<String> cacheFiles = null;

  public PiggyConverterUDF(String... param) {
    this.script_path = param[0];
    String ruleDFSPath = getDistCacheDFSPath(script_path);
    String ruleCacheName = getDistCacheFileName(script_path);
    this.outputCols = Arrays.copyOfRange(param, 1, param.length);

    InputStream is;
    try {
      if (UDFContext.getUDFContext().isFrontend()) {
        is = HadoopUtil.getFSInputStream(new Configuration(), ruleDFSPath);
      } else {
        is = new FileInputStream(ruleCacheName);
      }
    } catch (IOException e) {
      throw new PCRuntimeException(e);
    }

    try {
      rules = Rules.parseRules(is, "UTF8");
    } catch (IOException | RecognitionException e) {
      throw new PCRuntimeException(e);
    }
  }

  @Override
  public List<String> getCacheFiles() {
    if (cacheFiles != null)
      return cacheFiles;

    cacheFiles = new ArrayList<String>(1);
    cacheFiles.add(script_path);
    cacheFiles.addAll(rules.getRefFiles());

    return cacheFiles;
  }

  @Override
  public final Schema outputSchema(Schema inSchema) {
    if (this.inSchema == null)
      this.inSchema = inSchema;

    if (outSchema != null)
      return outSchema;

    outInnerSchema = new Schema();

    if (outputCols == null || outputCols.length == 0) {
      outInnerSchema = inSchema;
    } else {
      for (String addcol : outputCols) {
        if (addcol.equals("*")) {
          try {
            outInnerSchema = Schema.mergeSchemaByAlias(inSchema, outInnerSchema);
            continue;
          } catch (SchemaMergeException e) {
            throw new PCRuntimeException(e);
          }
        }

        String[] coldef = addcol.split(":");
        switch (coldef.length) {
        case 1:
          try {
            outInnerSchema.add(inSchema.getField(coldef[0]));
          } catch (FrontendException e) {
            throw new PCRuntimeException("Unknow field alias specified : " + addcol, e);
          }
          break;
        case 2:
          String sType = coldef[1].toUpperCase();
          Byte type = DataType.genNameToTypeMap().get(sType);
          if (type == null) {
            if (!sType.equals("INT")) {
              throw new PCRuntimeException("Invalid field type : " + addcol);
            }
            type = DataType.INTEGER;
          }
          outInnerSchema.add(new Schema.FieldSchema(coldef[0], type));
          break;
        default:
          throw new PCRuntimeException("Invalid field definition : " + addcol);
        }
      }
    }

    try {
      outSchema = new Schema(new Schema.FieldSchema("conved", outInnerSchema, DataType.TUPLE));
    } catch (FrontendException e) {
      throw new PCRuntimeException("Could not create output tuple schema.", e);
    }

    try {
      workSchema = Schema.mergeSchemaByAlias(inSchema, outInnerSchema);
    } catch (SchemaMergeException e) {
      throw new PCRuntimeException("Could not create working tuple schema.", e);
    }

    // When you know the output schema, you can init rules.
    SchemaWrapper workSW = new SchemaWrapper(workSchema);
    rules.init(workSW);

    return outSchema;
  }

  @Override
  public final Tuple exec(Tuple input) throws IOException {
    outputSchema(getInputSchema());

    if (input == null || input.size() == 0)
      return null;

    TupleWrapper work = new TupleWrapper(workSchema);
    try {
      work.mergeTupleByAlias(new TupleWrapper(inSchema, input));
    } catch (PCException e1) {
      throw new PCRuntimeException("Error while building work tuple.");
    }

    for (Rule rule : rules.getRuleList()) {
      try {
        rule.exec(work);
      } catch (TupleToIgnoreException e) {
        if (log.isDebugEnabled()) {
          log.debug("Ignore tuple : " + input.toString());
          log.debug("  matched rule : " + rule.toString());
        }
        return null;
      } catch (PCException e) {
        throw new PCRuntimeException("Exception at rule : " + rule + "\n  tuple : " + input.toDelimitedString("\t"), e);
      }
    }

    TupleWrapper output = new TupleWrapper(outInnerSchema);
    try {
      output.mergeTupleByAlias(work);
    } catch (PCException e) {
      throw new PCRuntimeException("Error while building output tuple.");
    }

    return output.getTuple();
  }

}
