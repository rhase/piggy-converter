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

package org.rhase.util.hadooputil;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(new Throwable().getStackTrace()[0].getClassName());

  public static FSDataInputStream getFSInputStream(Configuration conf, String uri) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    return fs.open(new Path(uri));
  }

  public static void deleteFile(Configuration conf, String strPath) throws IOException {

    Path path = new Path(strPath);
    FileSystem fs = path.getFileSystem(conf);

    LOGGER.debug("deleting path: {}", strPath);

    fs.delete(path, true);
  }

  public static void writeStrToFile(Configuration conf, String outputfile, String content) throws IOException {

    Path outPath = new Path(outputfile);
    FileSystem fs = outPath.getFileSystem(conf);
    OutputStream os = fs.create(outPath);
    InputStream is = new ByteArrayInputStream(content.getBytes());
    IOUtils.copyBytes(is, os, conf);

    // Never close FileSystem !! will cause oozie execution to fail with "IOException: Filesystem closed"
    // fs.close();
    // os.close();
    // is.close();
  }

  public static void recreateDir(Configuration conf, Path dirpath) throws IOException {
    FileSystem fs = dirpath.getFileSystem(conf);
    if (fs.exists(dirpath))
      fs.delete(dirpath, true);
    fs.mkdirs(dirpath);
  }

  public static Properties loadPropFileByURI(Configuration conf, String uri) throws IOException {

    Properties prop = new Properties();
    InputStream is = getFSInputStream(conf, uri);
    prop.load(is);

    // Never close FileSystem !! will cause oozie execution to fail with "IOException: Filesystem closed"
    // is.close();
    // fs.close();

    return prop;
  }

  public static URI getQualifiedURI(FileSystem fs, String path, String symlinkName) throws URISyntaxException {
    String qualified = new Path(path).makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString();

    if (symlinkName == null || symlinkName.isEmpty())
      symlinkName = new Path(path).getName();

    qualified = qualified + "#" + symlinkName;

    return new URI(qualified);
  }

  public static String getDistCacheFileName(String distCacheFullPath) {
    int idx = distCacheFullPath.lastIndexOf("#");
    if (idx == -1)
      idx = distCacheFullPath.lastIndexOf("/");

    return idx == -1 ? distCacheFullPath : distCacheFullPath.substring(idx + 1);
  }

  public static String getDistCacheDFSPath(String distCacheFullPath) {
    int idx = distCacheFullPath.lastIndexOf("#");
    return idx == -1 ? distCacheFullPath : distCacheFullPath.substring(0, idx);
  }
}