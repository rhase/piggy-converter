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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.rhase.piggyconverter.PCException;
import org.rhase.piggyconverter.TupleWrapper;

public class GetURLParamVal extends SingleColAction {

  private static final Logger LOGGER = Logger.getLogger(new Throwable().getStackTrace()[0].getClassName());

  // [URL parameter name, value column, ]
  private List<String[]> targets = new ArrayList<String[]>();

  // <parameter name, parameter value>
  private Map<String, String> urlParams = new HashMap<String, String>();

  /**
   * @param targets
   *          list of [URL parameter name, value column,]
   */
  public void setTargets(List<String[]> targets) {
    this.targets = targets;
  }

  @Override
  public void exec(TupleWrapper out) throws TupleToIgnoreException, PCException {
    URL url = null;
    try {
      url = new URL(out.getString(column));
    } catch (MalformedURLException e) {
      if (LOGGER.isInfoEnabled())
        LOGGER.info("malformed URL. skip to get url parameter: " + out.getString(column));
    }

    if (url == null)
      return;

    String query = null;
    query = url.getQuery();

    if (query == null)
      query = url.getRef(); // for google "#hl" parameter.

    if (query == null)
      return;

    String[] params = null;
    params = query.split("&");

    urlParams.clear();
    for (String param : params) {
      String[] paramSet = param.split("=", 2);
      if (paramSet.length != 2)
        continue;

      urlParams.put(paramSet[0], paramSet[1]);
    }

    for (String[] target : targets) {
      String value = urlParams.get(target[0]);
      if (value != null)
        out.set(target[1], value);
    }
  }

}
