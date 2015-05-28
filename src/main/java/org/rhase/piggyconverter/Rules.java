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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.antlr.runtime.ANTLRInputStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeNodeStream;
import org.rhase.piggyconverter.generated.PiggyConverterLexer;
import org.rhase.piggyconverter.generated.PiggyConverterParser;
import org.rhase.piggyconverter.generated.PiggyConverterTreeParser;

public class Rules implements Initable {
  private List<Rule> ruleList;
  private List<Initable> initableList;
  private List<String> refFiles;

  public void setRuleList(List<Rule> ruleList) {
    this.ruleList = ruleList;
  }

  public List<Rule> getRuleList() {
    return ruleList;
  }

  public void setInitableList(List<Initable> initableList) {
    this.initableList = initableList;
  }

  public List<String> getRefFiles() {
    return refFiles;
  }

  public void setRefFiles(List<String> refFiles) {
    this.refFiles = refFiles;
  }

  @Override
  public void init(SchemaWrapper outputSchema) {
    for (Initable init : initableList)
      init.init(outputSchema);
  }

  public static Rules parseRules(InputStream is, String encoding) throws IOException, RecognitionException {
    PiggyConverterLexer lex = new PiggyConverterLexer(new ANTLRInputStream(is, "UTF8"));
    CommonTokenStream tokens = new CommonTokenStream(lex);

    PiggyConverterParser parser = new PiggyConverterParser(tokens);
    PiggyConverterParser.rules_return r = parser.rules();
    CommonTree t = (CommonTree)r.getTree();

    CommonTreeNodeStream nodes = new CommonTreeNodeStream(t);
    PiggyConverterTreeParser walker = new PiggyConverterTreeParser(nodes);

    walker.rules();

    List<RecognitionException> lexerExs = lex.getExceptions();
    if (!lexerExs.isEmpty())
      throw new PCRuntimeException("lexical error.");

    return walker.parseRules();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    for (Rule rule : ruleList)
      sb.append(rule + "\n");

    return sb.toString();
  }
}
