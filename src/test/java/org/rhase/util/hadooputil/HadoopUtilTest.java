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

import static org.junit.Assert.assertEquals;
import static org.rhase.util.hadooputil.HadoopUtil.getDistCacheDFSPath;
import static org.rhase.util.hadooputil.HadoopUtil.getDistCacheFileName;

import org.junit.Test;

public class HadoopUtilTest {

  @Test
  public void testgetDistCacheFileName() {
    assertEquals("Without alias.", "file", getDistCacheFileName("/app/test/file"));
  }

  @Test
  public void testgetDistCacheFileNameAlias() {
    assertEquals("With alias.", "hoge", getDistCacheFileName("/app/test/file#hoge"));
  }

  @Test
  public void testgetDistCacheDFSPath() {
    assertEquals("With alias.", "/app/test/file", getDistCacheDFSPath("/app/test/file"));
  }

  @Test
  public void testgetDistCacheDFSPathAlias() {
    assertEquals("With alias.", "/app/test/file", getDistCacheDFSPath("/app/test/file#hoge"));
  }
}
