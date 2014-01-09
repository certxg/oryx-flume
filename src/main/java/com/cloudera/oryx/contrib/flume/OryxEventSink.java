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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * <p>
 * A Flume {@link Sink} implementation that sends events to an instance of Cloudera Oryx's serving
 * layer.
 * </p>
 * <p>
 * Events are taken from the {@link Channel} in batches of the configured <tt>batchSize</tt>. The
 * events are processed to extract the configured <tt>oryxFields</tt> and the values are transformed
 * into CSV records. The records are sent to Oryx in a HTTP POST request.
 * </p>
 * <p>
 * Batch underruns (i.e. batches smaller than the configured <tt>batchSize</tt>) are supported. If
 * the channel returns a null event, meaning it is empty, then the batch is immediately sent,
 * regardless of size.
 * </p>
 * <p>
 * For more information on Oryx see the projects GitHub: https://github.com/cloudera/oryx
 * </p>
 */
public final class OryxEventSink extends AbstractSink implements Configurable {
  private static final Logger log = LoggerFactory.getLogger(OryxEventSink.class);

  /** The maximum number of events to take from the channel per transaction */
  private static final String BATCH_SIZE = "batchSize";
  private static final int DEFAULT_BATCH_SIZE = 100;

  /** The hostname running the Oryx serving layer instance **/
  private static final String ORYX_HOSTNAME = "oryxHostname";

  /** The port the Oryx serving layer instance is listening on **/
  private static final String ORYX_PORT = "oryxPort";
  private static final int ORYX_DEFAULT_PORT = 80;

  /** The endpoint path for Oryx's REST API **/
  private static final String ORYX_ENDPOINT = "oryxEndpoint";
  private static final String ORYX_DEFAULT_ENDPOINT = "/ingest";

  /** A {@link OryxEventParser} implementation */
  private static final String ORYX_EVENT_PARSER = "oryxEventParser";
  
  /**
   * A list of fields to extract from an event and send to Oryx. Multiple <tt>oryxFields</tt> can be
   * specified by using a numeric postfix (i.e. exploding an event):
   * <ul>
   * <li>oryxFields = user,item[,strength]</li>
   * <li>oryxFields.0 = user,item0[,strength0]</li>
   * <li>oryxFields.1 = user,item1[,strength1]</li>
   * <li>oryxFields.2 = user,item2[,strength2]</li>
   * </ul>
   **/
  private static final String ORYX_FIELDS = "oryxFields";

  private int batchSize;
  private URI oryxUri;
  private List<List<String>> oryxFields;
  private OryxEventParser eventParser;
  private SinkCounter sinkCounter;
  private HttpClient client = null;
  
  @Override
  public void configure(Context context) {
    sinkCounter = new SinkCounter(getName());
    batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
    String oryxEndpoint = context.getString(ORYX_ENDPOINT, ORYX_DEFAULT_ENDPOINT);    
    String oryxHostname = context.getString(ORYX_HOSTNAME);
    int oryxPort = context.getInteger(ORYX_PORT, ORYX_DEFAULT_PORT);

    Preconditions.checkState(oryxHostname != null, "No Oryx hostname specified");
    
    try {
      oryxUri = new URIBuilder().setScheme("http")
          .setHost(oryxHostname)
          .setPort(oryxPort)
          .setPath(oryxEndpoint).build();
    } catch (URISyntaxException e) {
      throw new ConfigurationException(e);
    }
    
    String parserClass = context.getString(ORYX_EVENT_PARSER);
    try {
      eventParser = OryxEventParser.class.cast(Class.forName(parserClass).getConstructor().newInstance());
    } catch (Exception e) {
      throw new ConfigurationException("Unable to load Oryx event parser: " + parserClass, e);
    }

    oryxFields = Lists.newArrayList();
    String fields = context.getString(ORYX_FIELDS);
    if (fields != null) {
      addFields(fields);
    }

    for (int i = 0;; i++) {
      fields = context.getString(ORYX_FIELDS + '.' + i);
      if (fields == null) {
        break;
      }
      addFields(fields);
    }
    
    Preconditions.checkState(!oryxFields.isEmpty(), "No Oryx fields specified");

    if (log.isDebugEnabled()) {
      log.debug("Batch size: {}", batchSize);
      log.debug("Oryx URI: {}", oryxUri);
      log.debug("Event parser: {}", eventParser.getClass().getName());
      log.debug("Number of oryxFields: {}", oryxFields.size());      
    }
  }
  
  private void addFields(String fields) {
    String[] items = fields.split(",");
    if (items.length < 2 || items.length > 3) {
      throw new ConfigurationException("Incorrect number of items. " + fields
          + " should be user,item[,strength]");
    }
    for (int i = 0; i < items.length; i++) {
      items[i] = items[i].trim();
    }
    if (log.isDebugEnabled()) {
      log.debug("Adding {}: {}", ORYX_FIELDS, items);
    }
    oryxFields.add(Lists.newArrayList(items));
  }
  
  @Override
  public synchronized void start() {
    log.info("Starting Oryx sink: {}", getName());
    client = new DefaultHttpClient();
    sinkCounter.start();
    super.start();
  }

  @Override
  public synchronized void stop() {
    log.info("Stopping Oryx sink: {}", getName());
    sinkCounter.stop();
    super.stop();    
    log.info("Oryx sink {} stopped: {}", getName(), sinkCounter);
  }

  /**
   * Sends the given {@code batch} to Oryx in a HTTP POST request.
   * @param batch the batch of records to send to Oryx
   */
  private void processBatch(Collection<String> batch) {
    if (log.isDebugEnabled()) {
      log.debug("Sending batch of {} records to Oryx at {}", batch.size(), oryxUri);
    }

    StringBuilder sb = new StringBuilder();
    for (String record : batch) {
      sb.append(record).append('\n');
    }

    HttpPost post = new HttpPost(oryxUri);
    HttpEntity entity = new StringEntity(sb.toString(), ContentType.TEXT_PLAIN);
    post.setEntity(entity);

    try {
      HttpResponse response = client.execute(post);
      if (log.isDebugEnabled()) {
        log.debug("HTTP response from Oryx: '{}'", response.getStatusLine());
      }
      EntityUtils.consumeQuietly(response.getEntity());
    } catch (IOException e) {
      log.error("Unable to POST batch to Oryx", e);
    }
  }
  
  @Override
  public Status process() throws EventDeliveryException {
    Status status = Status.READY;
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    List<String> batch = Lists.newArrayList();

    try {
      transaction.begin();
      
      for (int i = 0; i < batchSize; i++) {
        Event event = channel.take();
        if (event == null || batch.size() >= batchSize) {
          // underrun if channel is empty
          break;
        }
        eventParser.parseEvent(event, oryxFields, batch);
      }

      int txSize = batch.size();

      if (txSize == 0) {
        sinkCounter.incrementBatchEmptyCount();
        status = Status.BACKOFF;
        if (log.isDebugEnabled()) {
          log.debug("Batch is empty. Backing off");
        }
      } else {
        if (txSize >= batchSize) {
          // The batch size can be bigger than configured if events are being exploded into
          // multiple Oryx records
          sinkCounter.incrementBatchCompleteCount();
        } else {
          sinkCounter.incrementBatchUnderflowCount();
        }
        processBatch(batch);
        sinkCounter.addToEventDrainSuccessCount(txSize);
      }

      transaction.commit();

    } catch (Throwable t) {
      transaction.rollback();
      if (t instanceof ChannelException) {
        log.error("Oryx sink {} unable to get event from channel {}", getName(), channel.getName(), t);
        status = Status.BACKOFF;
      } else {
        throw new EventDeliveryException("Failed to send events", t);
      }
    } finally {
      transaction.close();
    }

    return status;
  }
}
