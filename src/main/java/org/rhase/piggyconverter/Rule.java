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

import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.rhase.piggyconverter.action.Action;
import org.rhase.piggyconverter.action.TupleToIgnoreException;
import org.rhase.piggyconverter.condition.Condition;

public class Rule {
  private Condition condition;
  private List<Action> actions;

  public void setCondition(Condition condition) {
    this.condition = condition;
  }

  public Condition getCondition() {
    return condition;
  }

  public void setActions(List<Action> actions) {
    this.actions = actions;
  }

  public List<Action> getActions() {
    return actions;
  }

  public void exec(TupleWrapper dat) throws TupleToIgnoreException, PCException {
    if (condition != null && !condition.isTrue(dat))
      return;

    for (Action action : actions) {
      action.exec(dat);
    }
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this);
  }
}
