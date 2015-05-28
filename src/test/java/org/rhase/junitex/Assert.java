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

package org.rhase.junitex;

import static org.junit.Assert.assertEquals; // CHECKSTYLE IGNORE

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public final class Assert {

  public static String readFileToStr(File file) throws IOException {
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));

      StringBuffer buff = new StringBuffer();
      int ch;
      while ((ch = reader.read()) != -1) {
        buff.append((char)ch);
      }

      return buff.toString();

    } finally {
      if (reader != null)
        reader.close();
    }
  }

  public static String readFileToStr(String path) throws IOException {
    return readFileToStr(new File(path));
  }

  public static void assertFileEquals(String pathToExpected, String pathToActual) throws IOException {
    String expected = readFileToStr(pathToExpected);
    String actual = readFileToStr(pathToActual);

    assertEquals(expected, actual);
  }
}
