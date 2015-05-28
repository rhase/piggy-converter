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
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;
import org.junit.Test;
import org.rhase.piggyconverter.action.Action;
import org.rhase.piggyconverter.action.GetURLParamVal;
import org.rhase.piggyconverter.action.Ignore;
import org.rhase.piggyconverter.action.TupleToIgnoreException;
import org.rhase.piggyconverter.condition.ExactMatch;
import org.rhase.piggyconverter.condition.MatchCondition;
import org.rhase.piggyconverter.condition.RegExMatch;

public class RulesTest {
  TupleFactory tFactory = TupleFactory.getInstance();

  Schema buildSchema1() throws ParserException {
    return Utils
        .getSchemaFromString("userid:chararray, accessDTM:chararray, domain:chararray, url:chararray, id:chararray");
  }

  Rules buildRules1() {
    // Rule1
    MatchCondition cond1 = new ExactMatch();
    cond1.setColumn("domain");
    cond1.setPatternStr("hoge.co.jp");

    Ignore action1 = new Ignore();

    ArrayList<Action> actions1 = new ArrayList<Action>();
    actions1.add(action1);

    Rule rule1 = new Rule();
    rule1.setCondition(cond1);
    rule1.setActions(actions1);

    // Rule2
    MatchCondition cond2 = new RegExMatch();
    cond2.setPatternStr("http:\\/\\/www\\.fuga\\.co\\.jp.*");
    cond2.setColumn("url");

    GetURLParamVal action2 = new GetURLParamVal();
    List<String[]> targets = new LinkedList<String[]>();
    targets.add(new String[] { "id", "id" });
    action2.setColumn("url");
    action2.setTargets(targets);

    ArrayList<Action> actions2 = new ArrayList<Action>();
    actions2.add(action2);

    Rule rule2 = new Rule();
    rule2.setCondition(cond2);
    rule2.setActions(actions2);

    List<Rule> ruleList = new LinkedList<Rule>();
    ruleList.add(rule1);
    ruleList.add(rule2);

    Rules rules = new Rules();
    rules.setRuleList(ruleList);

    return rules;
  }

  @Test
  public void testExecRules1() throws ParserException, PCException {
    Schema schema = buildSchema1();
    TupleWrapper dat = new TupleWrapper(schema, tFactory.newTuple(schema.size()));
    dat.set("userid", "192.168.1.151");
    dat.set("accessDTM", "01/Oct/2011:00:01:01");
    dat.set("domain", "hoge.co.jp");
    dat.set("url", "http://www.hoge.co.jp/");

    for (Rule rule : buildRules1().getRuleList()) {
      try {
        rule.exec(dat);
        fail("Tuple must be ignored.");
      } catch (TupleToIgnoreException e) {
        return;
      }
    }
  }

  @Test
  public void testExecRules2() throws TupleToIgnoreException, ParserException, ExecException, PCException {
    Schema schema = buildSchema1();
    TupleWrapper dat = new TupleWrapper(schema, tFactory.newTuple(schema.size()));
    dat.set("userid", "192.168.1.151");
    dat.set("accessDTM", "01/Oct/2011:00:01:01");
    dat.set("domain", "fuga.co.jp");
    dat.set("url", "http://www.fuga.co.jp?id=hoge");

    for (Rule rule : buildRules1().getRuleList()) {
      rule.exec(dat);
    }

    assertEquals("192.168.1.151\t01/Oct/2011:00:01:01\tfuga.co.jp\thttp://www.fuga.co.jp?id=hoge\thoge", dat.getTuple()
        .toDelimitedString("\t"));
  }
}
