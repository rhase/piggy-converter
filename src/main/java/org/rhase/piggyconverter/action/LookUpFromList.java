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

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.rhase.piggyconverter.Initable;
import org.rhase.piggyconverter.PCException;
import org.rhase.piggyconverter.PCRuntimeException;
import org.rhase.piggyconverter.SchemaWrapper;
import org.rhase.piggyconverter.TupleWrapper;
import org.rhase.util.javautil.JavaUtil;

public class LookUpFromList extends SingleColAction implements Initable {

  public static enum MATCH_MODE {
    EXACT, PARTIAL
  };

  private interface Matcher {
    boolean match(String matchee, String pattern);
  };

  @SuppressWarnings("rawtypes")
  private LinkedHashMap<String, Comparable> list = null;
  private String indexFileName;
  private String listDelimiter;
  private String matcheeFld;
  private Matcher matcher = null;

  public void setIndexFileName(String indexFileName) {
    this.indexFileName = new File(indexFileName).getName();
  }

  public void setListDelimiter(String listDelimiter) {
    this.listDelimiter = listDelimiter;
  }

  public void setMatcheeFld(String matcheeFld) {
    this.matcheeFld = matcheeFld;
  }

  public void setMode(MATCH_MODE mode) {
    switch (mode) {
    case EXACT:
      // use Map#get method for exact match.
      break;
    case PARTIAL:
      matcher = new Matcher() {
        @Override
        public boolean match(String matchee, String pattern) {
          return matchee.contains(pattern);
        }
      };
      break;
    default:
      throw new PCRuntimeException("Unknown match mode : " + mode);
    }
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void init(SchemaWrapper outputSchema) {
    Byte valueType;
    try {
      valueType = outputSchema.getField(column).type;
    } catch (FrontendException e) {
      throw new PCRuntimeException("Field does not exists.", e);
    }

    List<String[]> strList;

    try {
      strList = JavaUtil.readFileToList(indexFileName, listDelimiter, 2);
    } catch (IOException e) {
      throw new PCRuntimeException("Error while reading index file.", e);
    }

    list = new LinkedHashMap<String, Comparable>(strList.size());
    for (String[] row : strList) {
      list.put(row[0], TupleWrapper.castFromString(valueType, row[1]));
    }
  }

  @Override
  public void exec(TupleWrapper out) throws PCException {

    if (matcher == null) {
      // mode = EXACT
      @SuppressWarnings("rawtypes")
      Comparable val = list.get(out.getString(matcheeFld));
      if (val != null) {
        out.set(column, val);
      }
    } else {
      // mode != EXACT
      for (@SuppressWarnings("rawtypes")
      Entry<String, Comparable> row : list.entrySet()) {
        if (matcher.match(out.getString(matcheeFld), row.getKey())) {
          out.set(column, row.getValue());
        }
      }
    }
  }

}
