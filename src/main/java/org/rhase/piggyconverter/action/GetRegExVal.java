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

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

import org.rhase.piggyconverter.PCException;
import org.rhase.piggyconverter.TupleWrapper;
import org.rhase.piggyconverter.condition.RegExMatch;

public class GetRegExVal extends Action {
  private RegExMatch regMatch = null;
  private Map<Integer, String> valueFlds = null;

  public GetRegExVal() {
    regMatch = new RegExMatch();
  }

  public void setValueFlds(Map<Integer, String> valueFlds) {
    this.valueFlds = valueFlds;
  }

  public void addValueFld(int group, String name) {
    if (valueFlds == null)
      valueFlds = new HashMap<Integer, String>();

    valueFlds.put(group, name);
  }

  public void setPatternStr(String patternStr) {
    regMatch.setPatternStr(patternStr);
  }

  public void setColumn(String column) {
    regMatch.setColumn(column);
  }

  @Override
  public void exec(TupleWrapper out) throws TupleToIgnoreException, PCException {
    Matcher matcher;
    if (regMatch.isTrue(out)) {
      matcher = regMatch.getMatcher();

      for (Map.Entry<Integer, String> fld : valueFlds.entrySet()) {
        try {
          // FUTURE Enable to set default value in case of unmatch.
          out.set(fld.getValue(), matcher.group(fld.getKey()));
        } catch (IndexOutOfBoundsException e) {
          throw new PCException("get regex value: value field defined more than match group.");
        }
      }
    }
  }
}
