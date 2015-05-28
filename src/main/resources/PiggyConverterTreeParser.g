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

tree grammar PiggyConverterTreeParser;

options {
    tokenVocab=PiggyConverterLexer;
    tokenVocab=PiggyConverterParser;
    ASTLabelType = CommonTree;
}

@header {
package org.rhase.piggyconverter.generated;

import static org.rhase.util.hadooputil.HadoopUtil.getDistCacheFileName;
import org.rhase.piggyconverter.*;
import org.rhase.piggyconverter.condition.*;
import org.rhase.piggyconverter.action.*;
import org.rhase.piggyconverter.action.LookUpFromList.MATCH_MODE;

import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
}

@members {
    private List<Rule> ruleList = new LinkedList<Rule>();
    private List<Initable> initables = new LinkedList<Initable>();
    private List<String> refFiles = new LinkedList<String>();

    public Rules parseRules() {
      Rules rules = new Rules();

      rules.setRuleList(ruleList);
      rules.setInitableList(initables);

      List<String> modRefFiles = new LinkedList<String>();
      for(String file : refFiles) {
        if(!file.contains("#")) {
          file = file + "#" + getDistCacheFileName(file);
        }
        modRefFiles.add(file);
      }
      rules.setRefFiles(modRefFiles);

      return rules;
    }

    public List<String> getRefFiles() {
        return refFiles;
    }
}

rules : rule+;

rule : ^(RULE cond=orFactor? acts=actions)
  {
   Rule rule = new Rule();
   if(cond != null)
   rule.setCondition($cond.condition );
   rule.setActions($acts.actions );
   ruleList.add(rule);
  };

orFactor returns [Condition condition]
 : {
   int i =0;
   Or or = null;
  }
  (^(OR_F cond=andFactor
  {
   if(i==0)
    condition = cond;
   else if(i==1) {
    or = new Or(condition);
    or.addCond(cond);
   } else
    or.addCond(cond);
   i++;
  }
  ))+
  {
   if(or != null)
    condition = or;
  };

andFactor returns [Condition condition]
 : {
   int i =0;
   And and = null;
  }
  (^(AND_F cond=condAtom
  {
   if(i==0)
    condition = cond;
   else if(i==1) {
    and = new And(condition);
    and.addCond(cond);
   } else
    and.addCond(cond);

   i++;
  }
  ))+
  {
   if(and != null)
    condition = and;
  };

condAtom returns [Condition condition]
 : ( ^(COND not=NOT? cond=condition)
  | ^(COND not=NOT? cond=orFactor)
  )
  {
   if(not != null) {
    Not tmpCond = new Not();
    tmpCond.setOriginal(cond);
    condition = tmpCond;
   } else
    condition = cond;
  };

/* conditions abstract*/
condition returns [Condition condition]
 :
  cCond=columnCond {condition = $cCond.condition;}
//  | rCond=rowCond {condition = $rCond.condition;}
  ;

columnCond returns [Condition condition]
 :   mCond=matchCond {condition = $mCond.condition;}
  | urlCond=validURLCond {condition = $urlCond.condition;};
/* conditions abstract*/

// matchCond start
matchCond returns [MatchCondition condition]
 : ^(MATCH_COND colName=STRING operator=matchOp matchPattern=STRING)
  {
   condition = $operator.condition;
   condition.setColumn($colName.text);
   condition.setPatternStr($matchPattern.text);
  };

matchOp returns [MatchCondition condition]
 : EXACTLY {condition = new ExactMatch();}
  | PARTIALY {condition = new PartialMatch();}
  | REGEX {condition = new RegExMatch();}
  ;
// matchCond end

validURLCond returns [ValidateURL condition]
 : ^(VALID_URL_COND colName=STRING)
  {
   condition = new ValidateURL();
   condition.setColumn($colName.text);
  };

/* row conditions abstract*/
// rowCond returns [Condition condition]
//  : cnCond=colNumCond {condition = $cnCond.condition;};
/* row conditions abstract*/

// colNumCond start
// colNumCond returns [ValidateColNum condition]
//  : ^(COLNUM_COND colNum=INT) {condition = new ValidateColNum(); condition.setColNum(Integer.parseInt($colNum.text) );};
// colNumCond end


/* actions abstract*/
actions returns [List<Action> actions]
 : {actions = new LinkedList<Action>();}
  (^(ACTIONS act=action {actions.add($act.action);}))+;
/* actions abstract*/

action returns [Action action]
 : IGNORE_ACT {action = new Ignore();}
  | LOOKUP mode=matchMode matchCol=STRING listFile=STRING setCol=STRING
    {
     LookUpFromList lookup = new LookUpFromList();
     lookup.setIndexFileName($listFile.text);
     lookup.setListDelimiter("\t");
     lookup.setMatcheeFld($matchCol.text);
     lookup.setColumn($setCol.text);
     lookup.setMode($mode.mode);
     refFiles.add($listFile.text);
     //initables.add( (Initable)lookup);
     initables.add(lookup);
     action = lookup;
    }
  | GET_REGEX_VAL_ACT regEx=STRING reMatchee=STRING reSetClause=regExSetClause
    {
     GetRegExVal getREV = new GetRegExVal();
     getREV.setPatternStr($regEx.text);
     getREV.setColumn($reMatchee.text);
     getREV.setValueFlds($reSetClause.valueFlds);
     action = getREV;
    }
  | GET_URL_PARAM_ACT urlCol=STRING uSetClause=urlParamSetClause
    {
     GetURLParamVal getParam = new GetURLParamVal();
     getParam.setColumn($urlCol.text);
     getParam.setTargets($uSetClause.targets);
     action = getParam;
    }
  | DEC_URL_ENC_ACT encCol=STRING
    {
     DecodeURLEncoded dec = new DecodeURLEncoded();
     dec.setColumn($encCol.text);
     action = dec;
    }
  | SET_VAL_ACT col=STRING value=STRING
    {
     SetValue setVal = new SetValue();
     setVal.setColumn($col.text);
     setVal.setValue($value.text);
     initables.add(setVal);
     action = setVal;
    }
  ;

matchMode returns [MATCH_MODE mode]
 : EXACTLY {mode = MATCH_MODE.EXACT;}
  | PARTIALY {mode = MATCH_MODE.PARTIAL;}
  ;

regExSetClause returns [Map<Integer, String> valueFlds]
 : {valueFlds = new HashMap<Integer, String>();}
  (^(PUT group=INT toCol=STRING
     {valueFlds.put(Integer.parseInt($group.text), $toCol.text);}
     ))+
  ;

urlParamSetClause returns [List<String[\]> targets]
 : {targets = new ArrayList<String[]>();}
  (^(PUT paramName=STRING toCol=STRING
     {targets.add(new String[] {$paramName.text, $toCol.text, });}
     ))+
  ;

