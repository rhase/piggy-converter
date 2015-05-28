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

package org.rhase.piggyconverter.condition;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.rhase.piggyconverter.PCException;
import org.rhase.piggyconverter.TupleWrapper;

public class RegExMatch extends MatchCondition {

  private Pattern patternObj = null;
  private Matcher matcher = null;

  public Matcher getMatcher() {
    return matcher;
  }

  @Override
  public void setPatternStr(String patternStr) {
    super.setPatternStr(patternStr);
    // FUTURE Variations like case insensitive.
    patternObj = Pattern.compile(patternStr);
  }

  @Override
  public boolean isTrue(TupleWrapper dat) throws PCException {

    String matcheeStr = dat.getString(column);
    if (matcheeStr == null)
      return false;

    matcher = patternObj.matcher(matcheeStr);

    return matcher.matches();
  }

}
