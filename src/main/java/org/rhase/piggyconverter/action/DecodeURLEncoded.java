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

package org.rhase.piggyconverter.action;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.rhase.piggyconverter.PCException;
import org.rhase.piggyconverter.TupleWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DecodeURLEncoded extends SingleColAction {
  private static final Logger LOGGER = LoggerFactory.getLogger(new Throwable().getStackTrace()[0].getClassName());

  @Override
  public void exec(TupleWrapper out) throws TupleToIgnoreException, PCException {
    String ret = "";

    try {
      ret = URLDecoder.decode(out.getString(column), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      handleException(out, e);
    } catch (IllegalArgumentException e) {
      handleException(out, e);
    }

    out.set(column, ret);
  }

  private void handleException(TupleWrapper out, Exception e) throws PCException {
    // FUTURE Function to write malformed datas to HDFS.
    LOGGER.info("malformed URL encode. skip to decode: {}", out.getString(column));
    LOGGER.debug("caused by", e);
  }

}
