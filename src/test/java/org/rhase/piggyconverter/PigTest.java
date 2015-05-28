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

import static org.junit.Assert.fail;
import static org.rhase.junitex.Assert.assertFileEquals;
import static org.rhase.util.javautil.JavaUtil.delRecursive;
import static org.rhase.util.javautil.JavaUtil.getFsPath;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.junit.Test;

public class PigTest {
  @Test
  public final void testMain() throws IOException, InterruptedException {

    String TEST_DIR = "src/test/resources/test1";

    String PIG_SCRIPT = TEST_DIR + "/test.pig";
    String CONVERTER_SCRIPT = TEST_DIR + "/test.pc";
    String CONVERTER_CACHE = "script.pc";
    String LOOKUP_LIST = TEST_DIR + "/lookup.list";
    String LOOKUP_CACHE = "lookup.list";

    String output = "tmp/output/part-m-00000";
    String expected = TEST_DIR + "/expected";

    // delete previous output.
    delRecursive(new File(output).getParentFile());

    Files.copy(getFsPath(CONVERTER_SCRIPT), getFsPath(CONVERTER_CACHE), StandardCopyOption.REPLACE_EXISTING);
    Files.copy(getFsPath(LOOKUP_LIST), getFsPath(LOOKUP_CACHE), StandardCopyOption.REPLACE_EXISTING);

    Map<String, String> params = new HashMap<String, String>();
    params.put("script", "src/test/resources/test1/test.pc");
    params.put("input", "src/test/resources/test1/access.log");
    params.put("output", "tmp/output");

    PigServer pigsv = new PigServer(ExecType.LOCAL);
    pigsv.setBatchOn();
    pigsv.registerScript(PIG_SCRIPT, params);
    List<ExecJob> jobs = pigsv.executeBatch();

    for (ExecJob job : jobs) {
      if (!job.hasCompleted())
        fail("Job failed!");
    }
    assertFileEquals(expected, output);
  }
}
