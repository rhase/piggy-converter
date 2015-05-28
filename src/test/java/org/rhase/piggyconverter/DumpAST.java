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

import org.antlr.runtime.ANTLRFileStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeNodeStream;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.rhase.piggyconverter.action.Action;
import org.rhase.piggyconverter.condition.Condition;
import org.rhase.piggyconverter.generated.PiggyConverterLexer;
import org.rhase.piggyconverter.generated.PiggyConverterParser;
import org.rhase.piggyconverter.generated.PiggyConverterTreeParser;

public class DumpAST {

  private static void printUsage() {
    System.out.println("usage: java " + new Throwable().getStackTrace()[0].getClassName()
        + " [piggy converter script path]");
  }

  public static void main(String args[]) throws Exception {
    if (args.length != 1) {
      printUsage();
      System.exit(0);
    }

    String piggyConverterScript = args[0];

    PiggyConverterLexer lex = new PiggyConverterLexer(new ANTLRFileStream(piggyConverterScript, "UTF8"));
    CommonTokenStream tokens = new CommonTokenStream(lex);
    PiggyConverterParser parser = new PiggyConverterParser(tokens);

    try {
      PiggyConverterParser.rules_return r = parser.rules();
      CommonTree t = (CommonTree)r.getTree();
      CommonTreeNodeStream nodes = new CommonTreeNodeStream(t);
      PiggyConverterTreeParser walker = new PiggyConverterTreeParser(nodes);
      walker.rules();

      for (Rule rule : walker.parseRules().getRuleList()) {
        System.out.println("\n" + "** rule:");

        System.out.println("\n" + "* action:");
        for (Action action : rule.getActions()) {
          System.out.println(ReflectionToStringBuilder.toString(action));
        }

        Condition cond = rule.getCondition();
        if (cond == null)
          continue;

        System.out.println("\n" + "* condition:");
        System.out.println(ReflectionToStringBuilder.toString(cond));

      }

    } catch (RecognitionException e) {
      System.out.println("exception caught!");
      e.printStackTrace();
    }
  }
}