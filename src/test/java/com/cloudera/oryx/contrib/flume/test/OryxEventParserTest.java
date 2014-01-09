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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;
import org.junit.Test;

import com.cloudera.oryx.contrib.flume.OryxEventParser;
import com.cloudera.oryx.contrib.flume.OryxJSONEventParser;
import com.google.common.collect.Lists;

/**
 * {@link OryxEventParser} tests
 */
public final class OryxEventParserTest extends Assert {
  @Test
  public void jsonEventParserTest() throws IOException {
    String record;
    FileInputStream fis = new FileInputStream("src/test/resources/example.json");
    try {
      record = IOUtils.toString(fis);
    } finally {
      fis.close();
    }

    Event event = EventBuilder.withBody(record.getBytes());
    List<String> batch = Lists.newArrayList();

    List<List<String>> fields = Lists.newArrayList();
    fields.add(Lists.newArrayList("$.user.id", "$.product.product-code", "1.0"));
    fields.add(Lists.newArrayList("$.user.id", "$.product.search-terms"));

    OryxEventParser ep = new OryxJSONEventParser();
    ep.parseEvent(event, fields, batch);

    assertEquals("fd156752df3d,488589e7c994,1.0", batch.get(0));
    assertEquals("fd156752df3d,natural running", batch.get(1));
  }
}