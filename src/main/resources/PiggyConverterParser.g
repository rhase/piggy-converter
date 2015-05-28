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

parser grammar PiggyConverterParser;

options {
    tokenVocab=PiggyConverterLexer;
    output=AST;
}

tokens {
    RULE;

    COND;
    OR_F;
    AND_F;
    MATCH_COND;
    VALID_URL_COND;

    ACTIONS;
    IGNORE_ACT;
    GET_REGEX_VAL_ACT;
    GET_URL_PARAM_ACT;
    DEC_URL_ENC_ACT;
    SET_VAL_ACT;
}

@parser::header {package org.rhase.piggyconverter.generated;}


rules : rule ((RULE_SEP!)+ rule)* RULE_SEP!* EOF!;

rule : (conditionClause)? actionClause -> ^(RULE conditionClause? actionClause);

//// condition
conditionClause
 : COND_START! orFactor;

orFactor
 : andFactor (OR andFactor)* -> (^(OR_F andFactor))+;

andFactor
 : condAtom (AND condAtom)* -> (^(AND_F condAtom))+;

condAtom
 : NOT?
  ( condition   -> ^(COND NOT? condition)
  | LPAREN orFactor RPAREN -> ^(COND NOT? orFactor)
  )
  ;

condition
 : columnCond /*| rowCond */ ;

// columnCond
columnCond
 : matchCond | validURLCond;

matchCond
 : colName MATCH matchOp matchPattern
  -> ^(MATCH_COND colName matchOp matchPattern)
  ;
colName : STRING;
matchOp : EXACTLY | PARTIALY | REGEX;
matchPattern
 : STRING;

validURLCond
 : colName IS VALID URL
  -> ^(VALID_URL_COND colName);

// rowCond
// rowCond : colNumCond;

// colNumCond
//  : COLNUM_OP colNum -> ^(COLNUM_COND colNum);

// colNum : INT;

//// action
actionClause
 : ACT_START action (AND action)* -> (^(ACTIONS action))+;

action : ignoreAct | lookupAct | getRegExValAct
  | getURLParamAct | decodeURLEncodedAct | setValueAct;

ignoreAct
 : IGNORE -> IGNORE_ACT;

lookupAct
 : LOOKUP matchMode matchCol FROM LIST listFile PUT VALUE TO setCol
  -> LOOKUP matchMode matchCol listFile setCol;
matchMode
 : EXACTLY | PARTIALY;
matchCol: STRING;
listFile: STRING;
setCol : STRING;

getRegExValAct
 : GET REGEX VALUE regEx FROM fromCol PUT regExSetClause
  -> GET_REGEX_VAL_ACT regEx fromCol regExSetClause;
regExSetClause
 : (group TO toCol)+
  -> (^(PUT group toCol))+;
regEx : STRING;
group : INT;
fromCol : STRING;
toCol : STRING;

getURLParamAct
 : GET URL PARAM VALUE FROM fromCol PUT urlParamSetClause
  -> GET_URL_PARAM_ACT fromCol urlParamSetClause;
urlParamSetClause
 : (paramName TO toCol)+
  -> (^(PUT paramName toCol))+;
paramName
 : STRING;

decodeURLEncodedAct
 : DECODE URL ENCODED encCol
  -> DEC_URL_ENC_ACT encCol;
encCol : STRING;

setValueAct
 : SET col TO value
  -> SET_VAL_ACT col value;
col : STRING;
value : STRING;
