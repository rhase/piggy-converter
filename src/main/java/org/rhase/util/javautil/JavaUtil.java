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

package org.rhase.util.javautil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;

public class JavaUtil {

  public static List<String[]> readFileToList(String listFileName, String delimiter, int numOfFields)
      throws IOException {
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(listFileName))));

      List<String[]> list = new LinkedList<String[]>();
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.equals(""))
          continue;

        String[] row = line.split(delimiter);
        if (row.length == numOfFields)
          list.add(row);
        else
          throw new IOException("Error while reading file: " + listFileName + ". Expected " + numOfFields
              + " columns, got " + row.length);
      }

      return list;

    } finally {
      if (reader != null)
        reader.close();
    }
  }

  public static boolean delRecursive(final File file) {
    if (!file.exists())
      return true;

    if (!file.isDirectory())
      return file.delete();

    boolean ret = true;
    for (File child : file.listFiles()) {
      ret = delRecursive(child) && ret;
    }

    return file.delete() && ret;
  }

  public static Path getFsPath(String path) {
    return FileSystems.getDefault().getPath(path);
  }
}
