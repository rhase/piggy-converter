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

import org.rhase.piggyconverter.PCException;
import org.rhase.piggyconverter.TupleWrapper;

public class And extends ComplexCondition {

  public And() {
    super();
  }

  public And(Condition cond) {
    super(cond);
  }

  @Override
  public boolean isTrue(TupleWrapper dat) throws PCException {

    for (Condition cond : conds) {
      if (!cond.isTrue(dat))
        return false;
    }
    return true;
  }

}
