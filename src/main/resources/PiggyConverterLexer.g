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

lexer grammar PiggyConverterLexer;

@lexer::header {package org.rhase.piggyconverter.generated;}
@lexer::members {
    private List<RecognitionException> exceptions = new ArrayList<RecognitionException>();

    public List<RecognitionException> getExceptions() {
        return exceptions;
    }

    @Override
    public void reportError(RecognitionException e) {
        super.reportError(e);
        exceptions.add(e);
    }
}

INT : '0'..'9'+
    ;

WS  :   ( ' '| '\t') {$channel=HIDDEN;};
NEW_LINE:( '\r'| '\n') {$channel=HIDDEN;};

LINE_COMMENT
  : '//' ~('\r'| '\n')* NEW_LINE {$channel=HIDDEN;};

STRING
@init{StringBuilder sb = new StringBuilder();}
    :  '"'
      ( esc=ESC_SEQ {sb.append(getText() );}
      | normal=~('\\'|'"') {sb.appendCodePoint(normal);} )*
      '"'
    {setText(sb.toString() );}
    ;

fragment
HEX_DIGIT : ('0'..'9'|'a'..'f'|'A'..'F') ;

fragment
ESC_SEQ
    :  '\\' ('b'|'t'|'n'|'f'|'r'|'"') {setText(getText().substring(1));}
      |'\\' ('\'' {setText("\'");}
      |'\\' {setText("\\");}
    )
    ;


// grammar specific tokens

// rule separator
RULE_SEP: ';';

COND_START
  : 'if';

// COLNUM_OP
//   : 'the number of column is';

MATCH : 'match';
REGEX : 'regex';

VALID : 'valid';

AND : 'and';
OR : 'or';
NOT : 'not';
IS : 'is';
LPAREN : '(';
RPAREN : ')';

ACT_START
 : 'do';

IGNORE : 'ignore record';

LOOKUP : 'lookup';
EXACTLY : 'exactly';
PARTIALY: 'partialy';
FROM : 'from';
LIST : 'list';
PUT : 'put';
VALUE : 'value';
TO : 'to';

GET : 'get';
URL : 'url';
PARAM : 'parameter';

DECODE  : 'decode';
ENCODED : 'encoded';

SET : 'set';
