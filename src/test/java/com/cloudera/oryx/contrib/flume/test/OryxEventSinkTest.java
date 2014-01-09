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

package com.cloudera.oryx.contrib.flume.test;

import org.apache.flume.Context;
import org.junit.Assert;
import org.junit.Test;

import com.cloudera.oryx.contrib.flume.OryxEventSink;

/**
 * {@link OryxEventSink} tests
 */
public final class OryxEventSinkTest extends Assert {
  @Test
  public void validContextTest() {
    Context context = new Context();
    context.put("oryxHostname", "localhost");
    context.put("oryxEventParser", "com.cloudera.oryx.contrib.flume.OryxJSONEventParser");
    context.put("oryxFields", "user,product-code,1.0");
    context.put("oryxFields.0", "user,search-terms");
    context.put("oryxFields.1", "search-terms,product-code");

    new OryxEventSink().configure(context);
  }

  @Test
  public void invalidContextTest() {
    Context context = new Context();
    context.put("oryxHostname", "localhost");
    context.put("oryxEventParser", "java.lang.String");
    context.put("oryxFields", "user,product-code,1.0");

    try {
      new OryxEventSink().configure(context);
      fail("Invalid oryxEventParser was not caught");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof ClassCastException);
    }
  }

}
