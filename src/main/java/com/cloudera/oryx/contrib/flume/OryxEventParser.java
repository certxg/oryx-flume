/**
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

import org.apache.flume.Event;

public interface OryxEventParser {
  /**
   * <p>
   * Extracts the given {@code fields} from the given {@code event} and adds them to the given
   * {@code batch} in the required Oryx format.
   * </p>
   * <p>
   * Oryx expects data of the form {@code user,item} or {@code user,item,strength}. Identifiers in
   * the first two columns can be numeric or non-numeric, and represent any kind of entity. A
   * numeric strength value is optional, and provides simple rating information.
   * </p>
   * @param event the Flume {@link Event}
   * @param fields a list of keys to identify the {@code user} and {@code item} fields to extract
   *          from the {@link Event}. If the optional {@code strength} field is specified, it is
   *          taken literally (i.e. added to the Oryx record as-is). For example:
   *          <ul>
   *          <li>username,product_id,2.0</li>
   *          <li>username,search_term</li>
   *          </ul>
   * @param batch the batch of records to send to Oryx
   */
  public void parseEvent(Event event, List<List<String>> fields, List<String> batch);
}
