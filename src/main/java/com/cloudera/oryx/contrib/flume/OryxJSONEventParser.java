/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.oryx.contrib.flume;

import java.util.List;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.ReadContext;

/**
 * <p>
 * An {@link OryxEventParser} implementation for parsing JSON formatted {@link Event} bodies.
 * </p>
 * <p>
 * {@link JsonPath} is used to extract the <tt>oryxFields</tt> from the JSON objects. To configure
 * this parser <tt>oryxFields</tt> should be given as JSON paths. For example, to extract
 * <tt>user-id</tt> and <tt>product-code</tt> from the following JSON object:
 * </p>
 * <p>
 * {@code
 * {
 *   "user": {
 *     "id": "fd156752df3d",
 *     "email": "kinley@cloudera.com"
 *   },
 *   "product": {
 *     "group": "running shoes",
 *     "product-name": "Inov-8 Road-X Lite",
 *     "product-code": "488589e7c994",
 *     "search-terms": "natural running"
 *   }
 * }
 * }
 * </p>
 * <p>
 * <tt>oryxFields</tt> should be defined in the Flume configuration as follows:
 * </p>
 * <p>
 * {@code
 * AgentName.sinks.SinkName.oryxFields = $.user.id, $.product.product-code, 1.0
 * }
 * </p>
 * See https://code.google.com/p/json-path for more information.
 */
public final class OryxJSONEventParser implements OryxEventParser {
  private static final Logger log = LoggerFactory.getLogger(OryxJSONEventParser.class);

  @Override
  public void parseEvent(Event event, List<List<String>> fields, List<String> batch) {
    ReadContext context = JsonPath.parse(new String(event.getBody()));

    for (List<String> keys : fields) {
      StringBuilder record = new StringBuilder();
      // add user and item
      for (int i = 0; i < 2; i++) {
        String val = null;
        try {
          val = context.read(keys.get(i));
        } catch (Exception ex) {
          if (ex instanceof PathNotFoundException) {
            log.error("Unable to find key '{}'", keys.get(i), ex);
          } else if (ex instanceof ClassCastException) {
            log.error("Unable to cast value for key '{}'", keys.get(i), ex);
          }
          if (log.isDebugEnabled()) {
            log.debug("Skipping record '{}'", new String(event.getBody()));
          }
          record.setLength(0);
          break;
        }
        record.append(StringEscapeUtils.escapeCsv(val)).append(',');
      }

      if (record.length() > 0) {
        if (keys.size() == 3) {
          // add strength
          record.append(keys.get(2));
        } else {
          record.deleteCharAt(record.length() - 1);
        }
        if (log.isDebugEnabled()) {
          log.debug("Adding record '{}' to batch", record);
        }
        batch.add(record.toString());
      }
    }
  }

}
