/*
 * Copyright 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.pubsub.kafka.sink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.kafka.common.ConnectorUtils;
import com.google.pubsub.v1.PubsubMessage;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.threeten.bp.Duration;

/** Tests for {@link CloudPubSubSinkTask}. */
public class CloudPubSubSinkTaskTest {

  private static final String CPS_TOPIC = "the";
  private static final String CPS_PROJECT = "quick";
  private static final String CPS_MIN_BATCH_SIZE1 = "2";
  private static final String CPS_MIN_BATCH_SIZE2 = "9";
  private static final String KAFKA_TOPIC = "brown";
  private static final ByteString KAFKA_MESSAGE1 = ByteString.copyFromUtf8("fox");
  private static final ByteString KAFKA_MESSAGE2 = ByteString.copyFromUtf8("jumps");
  private static final String FIELD_STRING1 = "over";
  private static final String FIELD_STRING2 = "lazy";
  private static final String BODY = "cps_message_body";
  private static final String KAFKA_MESSAGE_KEY1 = "dog";
  private static final String KAFKA_MESSAGE_KEY2 = "cat";
  private static final Schema STRING_SCHEMA = SchemaBuilder.string().build();
  private static final Schema BYTE_STRING_SCHEMA =
      SchemaBuilder.bytes().name(ConnectorUtils.SCHEMA_NAME).build();

  private CloudPubSubSinkTask task;
  private Map<String, String> props;
  private Publisher publisher;

  private class SpyableFuture<V> implements ApiFuture<V> {
    private V value = null;
    private Throwable exception = null;

    public SpyableFuture(V value) {
      this.value = value;
    }

    public <V> SpyableFuture(Throwable exception) {
      this.exception = exception;
    }

    @Override
    public V get() throws ExecutionException {
      if (exception != null) {
        throw new ExecutionException(exception);
      }
      return value;
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws ExecutionException {
      return get();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return true;
    }

    @Override
    public void addListener(Runnable listener, Executor executor) {
      executor.execute(listener);
    }
  }

  private SinkTaskContext context;
  private ErrantRecordReporter reporter;

  @Before
  public void setup() {
    publisher = mock(Publisher.class, RETURNS_DEEP_STUBS);
    task = new CloudPubSubSinkTask(publisher);
    props = new HashMap<>();
    props.put(ConnectorUtils.CPS_TOPIC_CONFIG, CPS_TOPIC);
    props.put(ConnectorUtils.CPS_PROJECT_CONFIG, CPS_PROJECT);
    props.put(CloudPubSubSinkConnector.MAX_BUFFER_SIZE_CONFIG, CPS_MIN_BATCH_SIZE2);
    reporter = mock(ErrantRecordReporter.class);
    when(reporter.report(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
    context = mock(SinkTaskContext.class);
    when(context.errantRecordReporter()).thenReturn(reporter);
  }

  /** Tests that an exception is thrown when the schema of the value is not BYTES. */
  @Test
  public void testPutPrimitives() {
    task.start(props);
    SinkRecord record8 = new SinkRecord(null, -1, null, null, SchemaBuilder.int8(), (byte) 5, -1);
    SinkRecord record16 =
        new SinkRecord(null, -1, null, null, SchemaBuilder.int16(), (short) 5, -1);
    SinkRecord record32 = new SinkRecord(null, -1, null, null, SchemaBuilder.int32(), (int) 5, -1);
    SinkRecord record64 = new SinkRecord(null, -1, null, null, SchemaBuilder.int64(), (long) 5, -1);
    SinkRecord recordFloat32 =
        new SinkRecord(null, -1, null, null, SchemaBuilder.float32(), (float) 8, -1);
    SinkRecord recordFloat64 =
        new SinkRecord(null, -1, null, null, SchemaBuilder.float64(), (double) 8, -1);
    SinkRecord recordBool = new SinkRecord(null, -1, null, null, SchemaBuilder.bool(), true, -1);
    SinkRecord recordString =
        new SinkRecord(null, -1, null, null, SchemaBuilder.string(), "Test put.", -1);
    List<SinkRecord> list = new ArrayList<>();
    list.add(record8);
    list.add(record16);
    list.add(record32);
    list.add(record64);
    list.add(recordFloat32);
    list.add(recordFloat64);
    list.add(recordBool);
    list.add(recordString);
    task.put(list);
  }

  @Test
  public void testStructSchema() {
    task.start(props);
    Schema schema =
        SchemaBuilder.struct()
            .field(FIELD_STRING1, SchemaBuilder.string())
            .field(FIELD_STRING2, SchemaBuilder.string())
            .build();
    Struct val = new Struct(schema);
    val.put(FIELD_STRING1, "tide");
    val.put(FIELD_STRING2, "eagle");
    SinkRecord record = new SinkRecord(null, -1, null, null, schema, val, -1);
    List<SinkRecord> list = new ArrayList<>();
    list.add(record);
    task.put(list);
  }

  @Test
  public void testStructSchemaWithOptionalField() {
    task.start(props);

    Schema schema =
        SchemaBuilder.struct()
            .field(FIELD_STRING1, SchemaBuilder.string())
            .field(FIELD_STRING2, SchemaBuilder.string().optional())
            .build();

    // With the optional field missing.
    Struct val = new Struct(schema);
    val.put(FIELD_STRING1, "tide");
    SinkRecord record = new SinkRecord(null, -1, null, null, schema, val, -1);
    List<SinkRecord> list = new ArrayList<>();
    list.add(record);
    task.put(list);

    // With the optional field present.
    val.put(FIELD_STRING2, "eagle");
    record = new SinkRecord(null, -1, null, null, schema, val, -1);
    list = new ArrayList<>();
    list.add(record);
    task.put(list);
  }

  @Test(expected = DataException.class)
  public void testStructSchemaWithMissingField() {
    task.start(props);

    Schema schema =
        SchemaBuilder.struct()
            .field(FIELD_STRING1, SchemaBuilder.string())
            .field(FIELD_STRING2, SchemaBuilder.string())
            .build();
    Struct val = new Struct(schema);
    val.put(FIELD_STRING1, "tide");
    SinkRecord record = new SinkRecord(null, -1, null, null, schema, val, -1);
    List<SinkRecord> list = new ArrayList<>();
    list.add(record);
    task.put(list);
  }

  @Test(expected = DataException.class)
  public void testStringSchemaWithNonStringValue() {
    task.start(props);
    SinkRecord record = new SinkRecord(null, -1, null, null, Schema.STRING_SCHEMA, 45, -1);
    task.put(Collections.singletonList(record));
  }

  @Test(expected = DataException.class)
  public void testMapSchemaWithMismatchedValue() {
    task.start(props);
    Schema schema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();
    Map<String, Object> val = new HashMap<>();
    val.put("count", "abc");
    SinkRecord record = new SinkRecord(null, -1, null, null, schema, val, -1);
    task.put(Collections.singletonList(record));
  }

  @Test(expected = DataException.class)
  public void testBytesSchemaWithNonBytesValue() {
    task.start(props);
    SinkRecord record = new SinkRecord(null, -1, null, null, Schema.BYTES_SCHEMA, "abc", -1);
    task.put(Collections.singletonList(record));
  }

  @Test
  public void testStructSchemaWithNestedSchema() {
    props.put(CloudPubSubSinkConnector.CPS_MESSAGE_BODY_NAME, BODY);
    task.start(props);

    Schema nestedSchema = SchemaBuilder.struct().build();
    Struct nestedVal = new Struct(nestedSchema);

    Schema schema =
        SchemaBuilder.struct()
            .field(FIELD_STRING1, SchemaBuilder.string())
            .field(FIELD_STRING2, nestedSchema)
            .field(BODY, SchemaBuilder.string())
            .build();
    Struct val = new Struct(schema);
    val.put(FIELD_STRING1, "tide");
    val.put(FIELD_STRING2, nestedVal);
    val.put(BODY, "eagle");
    SinkRecord record = new SinkRecord(null, -1, null, null, schema, val, -1);
    List<SinkRecord> list = new ArrayList<>();
    list.add(record);
    task.put(list);
    PubsubMessage message = publishedMessage();
    assertEquals("tide", message.getAttributesOrThrow(FIELD_STRING1));
    assertEquals("{}", message.getAttributesOrThrow(FIELD_STRING2));
    assertEquals("eagle", message.getData().toStringUtf8());
  }

  @Test
  public void testMapSchema() {
    task.start(props);
    Schema schema = SchemaBuilder.map(SchemaBuilder.string(), SchemaBuilder.string()).build();
    Map<String, String> val = new HashMap<>();
    val.put(FIELD_STRING1, "tide");
    val.put(FIELD_STRING2, "eagle");
    SinkRecord record = new SinkRecord(null, -1, null, null, schema, val, -1);
    List<SinkRecord> list = new ArrayList<>();
    list.add(record);
    task.put(list);
    schema = SchemaBuilder.map(SchemaBuilder.string(), SchemaBuilder.bytes()).build();
    record = new SinkRecord(null, -1, null, null, schema, val, -1);
    list.add(record);
    try {
      task.put(list);
    } catch (DataException e) {
    } // Expected, pass.
  }

  @Test
  public void testArraySchema() {
    task.start(props);
    Schema schema = SchemaBuilder.array(SchemaBuilder.string()).build();
    List<String> val = Arrays.asList("Roll", "tide");
    SinkRecord record = new SinkRecord(null, -1, null, null, schema, val, -1);
    task.put(Collections.singletonList(record));
    assertEquals("[\"Roll\",\"tide\"]", publishedMessage().getData().toStringUtf8());
  }

  @Test
  public void testNullSchema() {
    task.start(props);
    String val = "I have no schema";
    SinkRecord record = new SinkRecord(null, -1, null, null, null, val, -1);
    List<SinkRecord> list = new ArrayList<>();
    list.add(record);
    task.put(list);
  }

  /** Tests that the correct message is sent to the publisher. */
  @Test
  public void testPutWherePublishesAreInvoked() {
    props.put(CloudPubSubSinkConnector.MAX_BUFFER_SIZE_CONFIG, CPS_MIN_BATCH_SIZE1);
    task.start(props);
    List<SinkRecord> records = getSampleRecords();
    task.put(records);
    ArgumentCaptor<PubsubMessage> captor = ArgumentCaptor.forClass(PubsubMessage.class);
    verify(publisher, times(2)).publish(captor.capture());
    List<PubsubMessage> requestArgs = captor.getAllValues();
    assertEquals(requestArgs, getPubsubMessagesFromSampleRecords());
  }

  /** Tests that the correct message is sent to the publisher when the record has a null value. */
  @Test
  public void testPutWithNullValues() {
    props.put(CloudPubSubSinkConnector.MAX_BUFFER_SIZE_CONFIG, CPS_MIN_BATCH_SIZE1);
    task.start(props);
    List<SinkRecord> records = new ArrayList<>();
    records.add(
        new SinkRecord(KAFKA_TOPIC, 0, STRING_SCHEMA, KAFKA_MESSAGE_KEY1, STRING_SCHEMA, null, -1));
    task.put(records);
    ArgumentCaptor<PubsubMessage> captor = ArgumentCaptor.forClass(PubsubMessage.class);
    verify(publisher, times(1)).publish(captor.capture());
    List<PubsubMessage> requestArgs = captor.getAllValues();
    List<PubsubMessage> expectedMessages = new ArrayList<>();
    Map<String, String> attributes = new HashMap<>();
    attributes.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY1);
    expectedMessages.add(PubsubMessage.newBuilder().putAllAttributes(attributes).build());
    assertEquals(requestArgs, expectedMessages);
  }

  /** Tests that the no message is sent when a message is completely null. */
  @Test
  public void testPutWithNullMessage() {
    System.out.println("NULL MSG");
    props.put(CloudPubSubSinkConnector.MAX_BUFFER_SIZE_CONFIG, CPS_MIN_BATCH_SIZE1);
    task.start(props);
    List<SinkRecord> records = new ArrayList<>();
    records.add(new SinkRecord(KAFKA_TOPIC, 0, STRING_SCHEMA, null, STRING_SCHEMA, null, -1));
    task.put(records);
    ArgumentCaptor<PubsubMessage> captor = ArgumentCaptor.forClass(PubsubMessage.class);
    verify(publisher, times(0)).publish(captor.capture());
  }

  /** Tests that a call to flush() processes the Futures that were generated by calls to put. */
  @Test
  public void testFlushWithNoPublishInPut() throws Exception {
    task.start(props);
    Map<TopicPartition, OffsetAndMetadata> partitionOffsets = new HashMap<>();
    partitionOffsets.put(new TopicPartition(KAFKA_TOPIC, 0), null);
    List<SinkRecord> records = getSampleRecords();
    ApiFuture<String> goodFuture = getSuccessfulPublishFuture();
    when(goodFuture.isDone()).thenReturn(false);
    when(publisher.publish(any(PubsubMessage.class))).thenReturn(goodFuture);
    task.put(records);
    task.flush(partitionOffsets);
    verify(publisher, times(2)).publish(any(PubsubMessage.class));
    verify(goodFuture, times(2)).addListener(any(Runnable.class), any(Executor.class));
  }

  /**
   * Tests that if a Future that is being processed in flush() failed with an exception, that an
   * exception is thrown.
   */
  @Test(expected = RuntimeException.class)
  public void testFlushExceptionCase() throws Exception {
    task.start(props);
    Map<TopicPartition, OffsetAndMetadata> partitionOffsets = new HashMap<>();
    partitionOffsets.put(new TopicPartition(KAFKA_TOPIC, 0), null);
    List<SinkRecord> records = getSampleRecords();
    ApiFuture<String> badFuture = getFailedPublishFuture();
    when(publisher.publish(any(PubsubMessage.class))).thenReturn(badFuture);
    task.put(records);
    task.flush(partitionOffsets);
    verify(publisher, times(1)).publish(any(PubsubMessage.class));
    verify(badFuture, times(1)).addListener(any(Runnable.class), any(Executor.class));
  }

  /**
   * Tests that when requested, Kafka metadata is included in the messages published to Cloud
   * Pub/Sub.
   */
  @Test
  public void testKafkaMetadata() {
    props.put(CloudPubSubSinkConnector.PUBLISH_KAFKA_METADATA, "true");
    props.put(CloudPubSubSinkConnector.MAX_BUFFER_SIZE_CONFIG, CPS_MIN_BATCH_SIZE1);
    task.start(props);
    List<SinkRecord> records = new ArrayList<SinkRecord>();
    records.add(
        new SinkRecord(
            KAFKA_TOPIC,
            4,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY1,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE1,
            1000,
            50000L,
            TimestampType.CREATE_TIME));
    records.add(
        new SinkRecord(
            KAFKA_TOPIC,
            4,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY1,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE2,
            1001,
            50001L,
            TimestampType.CREATE_TIME));
    records.add(
        new SinkRecord(
            KAFKA_TOPIC,
            4,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY1,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE2,
            1002,
            null,
            TimestampType.CREATE_TIME));
    task.put(records);
    ArgumentCaptor<PubsubMessage> captor = ArgumentCaptor.forClass(PubsubMessage.class);
    verify(publisher, times(3)).publish(captor.capture());
    List<PubsubMessage> requestArgs = captor.getAllValues();

    List<PubsubMessage> expectedMessages = new ArrayList<>();
    Map<String, String> attributes1 = new HashMap<>();
    attributes1.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY1);
    attributes1.put(ConnectorUtils.KAFKA_TOPIC_ATTRIBUTE, KAFKA_TOPIC);
    attributes1.put(ConnectorUtils.KAFKA_PARTITION_ATTRIBUTE, "4");
    attributes1.put(ConnectorUtils.KAFKA_OFFSET_ATTRIBUTE, "1000");
    attributes1.put(ConnectorUtils.KAFKA_TIMESTAMP_ATTRIBUTE, "50000");
    expectedMessages.add(
        PubsubMessage.newBuilder().putAllAttributes(attributes1).setData(KAFKA_MESSAGE1).build());
    Map<String, String> attributes2 = new HashMap<>();
    attributes2.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY1);
    attributes2.put(ConnectorUtils.KAFKA_TOPIC_ATTRIBUTE, KAFKA_TOPIC);
    attributes2.put(ConnectorUtils.KAFKA_PARTITION_ATTRIBUTE, "4");
    attributes2.put(ConnectorUtils.KAFKA_OFFSET_ATTRIBUTE, "1001");
    attributes2.put(ConnectorUtils.KAFKA_TIMESTAMP_ATTRIBUTE, "50001");
    expectedMessages.add(
        PubsubMessage.newBuilder().putAllAttributes(attributes2).setData(KAFKA_MESSAGE2).build());
    Map<String, String> attributes3 = new HashMap<>();
    attributes3.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY1);
    attributes3.put(ConnectorUtils.KAFKA_TOPIC_ATTRIBUTE, KAFKA_TOPIC);
    attributes3.put(ConnectorUtils.KAFKA_PARTITION_ATTRIBUTE, "4");
    attributes3.put(ConnectorUtils.KAFKA_OFFSET_ATTRIBUTE, "1002");
    expectedMessages.add(
        PubsubMessage.newBuilder().putAllAttributes(attributes3).setData(KAFKA_MESSAGE2).build());

    assertEquals(requestArgs, expectedMessages);
  }

  /**
   * Tests that when requested, Kafka headers are included in the messages published to Cloud
   * Pub/Sub.
   */
  @Test
  public void testKafkaHeaders() {
    props.put(CloudPubSubSinkConnector.PUBLISH_KAFKA_HEADERS, "true");
    task.start(props);
    List<SinkRecord> records = new ArrayList<SinkRecord>();
    SinkRecord record =
        new SinkRecord(
            KAFKA_TOPIC,
            4,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY1,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE1,
            1000,
            50000L,
            TimestampType.CREATE_TIME);
    record.headers().addString("myHeader", "myValue");
    records.add(record);
    record =
        new SinkRecord(
            KAFKA_TOPIC,
            4,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY1,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE2,
            1001,
            50001L,
            TimestampType.CREATE_TIME);
    record.headers().addString("yourHeader", "yourValue");
    records.add(record);
    task.put(records);
    ArgumentCaptor<PubsubMessage> captor = ArgumentCaptor.forClass(PubsubMessage.class);
    verify(publisher, times(2)).publish(captor.capture());
    List<PubsubMessage> requestArgs = captor.getAllValues();

    List<PubsubMessage> expectedMessages = new ArrayList<>();
    Map<String, String> attributes1 = new HashMap<>();
    attributes1.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY1);
    attributes1.put("myHeader", "myValue");
    expectedMessages.add(
        PubsubMessage.newBuilder().putAllAttributes(attributes1).setData(KAFKA_MESSAGE1).build());
    Map<String, String> attributes2 = new HashMap<>();
    attributes2.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY1);
    attributes2.put("yourHeader", "yourValue");
    expectedMessages.add(
        PubsubMessage.newBuilder().putAllAttributes(attributes2).setData(KAFKA_MESSAGE2).build());

    assertEquals(expectedMessages, requestArgs);
  }

  /**
   * Tests that when requested, Kafka headers are included in the messages published to Cloud
   * Pub/Sub but if some of these headers are unsupported either if its key has more than 256 bytes
   * long or its value has more than 1024 bytes it will be discarded.
   */
  @Test
  public void testUnsupportedKafkaHeaders() {
    props.put(CloudPubSubSinkConnector.PUBLISH_KAFKA_HEADERS, "true");
    task.start(props);
    String veryLongHeaderName;
    String veryLongValue;
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < 257; i++) {
      stringBuilder.append("-");
    }
    veryLongHeaderName = stringBuilder.toString();
    stringBuilder.setLength(0);
    for (int i = 0; i < 1025; i++) {
      stringBuilder.append(".");
    }
    veryLongValue = stringBuilder.toString();
    stringBuilder.setLength(0);
    List<SinkRecord> records = new ArrayList<SinkRecord>();
    SinkRecord record =
        new SinkRecord(
            KAFKA_TOPIC,
            4,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY1,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE1,
            1000,
            50000L,
            TimestampType.CREATE_TIME);
    record.headers().addString("myHeader", "myValue");
    record.headers().addString(veryLongHeaderName, "anotherValue");
    record.headers().addString("anotherHeader", veryLongValue);
    record.headers().addString(veryLongHeaderName, veryLongValue);
    records.add(record);
    record =
        new SinkRecord(
            KAFKA_TOPIC,
            4,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY1,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE2,
            1001,
            50001L,
            TimestampType.CREATE_TIME);
    record.headers().addString("yourHeader", "yourValue");
    records.add(record);
    task.put(records);
    ArgumentCaptor<PubsubMessage> captor = ArgumentCaptor.forClass(PubsubMessage.class);
    verify(publisher, times(2)).publish(captor.capture());
    List<PubsubMessage> requestArgs = captor.getAllValues();

    List<PubsubMessage> expectedMessages = new ArrayList<>();
    Map<String, String> attributes1 = new HashMap<>();
    attributes1.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY1);
    attributes1.put("myHeader", "myValue");
    expectedMessages.add(
        PubsubMessage.newBuilder().putAllAttributes(attributes1).setData(KAFKA_MESSAGE1).build());
    Map<String, String> attributes2 = new HashMap<>();
    attributes2.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY1);
    attributes2.put("yourHeader", "yourValue");
    expectedMessages.add(
        PubsubMessage.newBuilder().putAllAttributes(attributes2).setData(KAFKA_MESSAGE2).build());

    assertEquals(257, veryLongHeaderName.getBytes().length);
    assertEquals(1025, veryLongValue.getBytes().length);
    assertEquals(expectedMessages, requestArgs);
  }

  /**
   * Tests that if a Future that is being processed in flush() ran out of retries, behavior.on.error
   * handles it, and a second Future is processed successfully in a subsequent flush.
   */
  @Test
  public void testFlushExceptionThenNoExceptionCase() throws Exception {
    props.put(CloudPubSubSinkConnector.BEHAVIOR_ON_ERROR, "log");
    task.start(props);
    Map<TopicPartition, OffsetAndMetadata> partitionOffsets = new HashMap<>();
    partitionOffsets.put(new TopicPartition(KAFKA_TOPIC, 0), null);
    List<SinkRecord> records = getSampleRecords();
    ApiFuture<String> badFuture = getFailedPublishFuture(unavailable());
    when(badFuture.isDone()).thenReturn(false);
    ApiFuture<String> goodFuture = getSuccessfulPublishFuture();
    when(goodFuture.isDone()).thenReturn(false);
    when(publisher.publish(any(PubsubMessage.class)))
        .thenReturn(badFuture)
        .thenReturn(badFuture)
        .thenReturn(goodFuture);
    task.put(records);
    task.flush(partitionOffsets);
    records = getSampleRecords();
    task.put(records);
    task.flush(partitionOffsets);
    verify(publisher, times(4)).publish(any(PubsubMessage.class));
    verify(badFuture, times(2)).addListener(any(Runnable.class), any(Executor.class));
    verify(goodFuture, times(2)).addListener(any(Runnable.class), any(Executor.class));
  }

  @Test
  public void testPublisherShutdownOnStop() throws Exception {
    int maxShutdownTimeoutMs = 20000;
    props.put(
        CloudPubSubSinkConnector.MAX_SHUTDOWN_TIMEOUT_MS, Integer.toString(maxShutdownTimeoutMs));

    task.start(props);
    task.stop();

    verify(publisher, times(1)).shutdown();
    verify(publisher, times(1)).awaitTermination(maxShutdownTimeoutMs, TimeUnit.MILLISECONDS);
  }

  /** Tests that the emulator configuration is properly defined and parsed. */
  @Test
  public void testEmulatorConfiguration() {
    CloudPubSubSinkConnector connector = new CloudPubSubSinkConnector();
    ConfigDef configDef = connector.config();

    assertTrue(
        "Emulator configuration should be defined",
        configDef.names().contains(ConnectorUtils.CPS_USE_EMULATOR));

    Map<String, String> emulatorProps = new HashMap<>();
    emulatorProps.put(ConnectorUtils.CPS_TOPIC_CONFIG, CPS_TOPIC);
    emulatorProps.put(ConnectorUtils.CPS_PROJECT_CONFIG, CPS_PROJECT);
    emulatorProps.put(ConnectorUtils.CPS_USE_EMULATOR, "true");

    Map<String, Object> parsedProps = configDef.parse(emulatorProps);
    assertTrue(
        "Emulator should be enabled", (Boolean) parsedProps.get(ConnectorUtils.CPS_USE_EMULATOR));
  }

  @Test
  public void testCreatePublisherWithEmulatorEnabled() {
    props.put(ConnectorUtils.CPS_USE_EMULATOR, "true");
    props.put(ConnectorUtils.CPS_ENDPOINT, "localhost:8085");
    CloudPubSubSinkTask task = new CloudPubSubSinkTask(publisher);
    task.start(props);
    assertEquals(CloudPubSubSinkTask.class, task.getClass());
  }

  @Test
  public void testCreatePublisherWithEmulatorDisabled() {
    props.put(ConnectorUtils.CPS_USE_EMULATOR, "false");
    props.put(ConnectorUtils.CPS_ENDPOINT, "pubsub.googleapis.com:443");
    CloudPubSubSinkTask task = new CloudPubSubSinkTask(publisher);
    task.start(props);
    assertEquals(CloudPubSubSinkTask.class, task.getClass());
  }

  /** Get some sample SinkRecords's to use in the tests. */
  private List<SinkRecord> getSampleRecords() {
    List<SinkRecord> records = new ArrayList<>();
    records.add(
        new SinkRecord(
            KAFKA_TOPIC,
            0,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY1,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE1,
            -1));
    records.add(
        new SinkRecord(
            KAFKA_TOPIC,
            0,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY1,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE2,
            -1));
    return records;
  }

  /**
   * Tests that when requested, Kafka key is used as the ordering key in the messages published to
   * Cloud Pub/Sub.
   */
  @Test
  public void testOrderingKeysKey() {
    props.put(CloudPubSubSinkConnector.ORDERING_KEY_SOURCE, "key");
    task.start(props);
    List<SinkRecord> records = new ArrayList<SinkRecord>();
    SinkRecord record =
        new SinkRecord(
            KAFKA_TOPIC,
            4,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY1,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE1,
            1000,
            50000L,
            TimestampType.CREATE_TIME);
    records.add(record);
    record =
        new SinkRecord(
            KAFKA_TOPIC,
            4,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY2,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE2,
            1001,
            50001L,
            TimestampType.CREATE_TIME);
    records.add(record);
    task.put(records);
    ArgumentCaptor<PubsubMessage> captor = ArgumentCaptor.forClass(PubsubMessage.class);
    verify(publisher, times(2)).publish(captor.capture());
    List<PubsubMessage> requestArgs = captor.getAllValues();

    List<PubsubMessage> expectedMessages = new ArrayList<>();
    Map<String, String> attributes1 = new HashMap<>();
    attributes1.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY1);
    expectedMessages.add(
        PubsubMessage.newBuilder()
            .putAllAttributes(attributes1)
            .setOrderingKey(KAFKA_MESSAGE_KEY1)
            .setData(KAFKA_MESSAGE1)
            .build());
    Map<String, String> attributes2 = new HashMap<>();
    attributes2.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY2);
    expectedMessages.add(
        PubsubMessage.newBuilder()
            .putAllAttributes(attributes2)
            .setOrderingKey(KAFKA_MESSAGE_KEY2)
            .setData(KAFKA_MESSAGE2)
            .build());

    assertEquals(expectedMessages, requestArgs);
  }

  /**
   * Tests that when requested, Kafka key is used as the ordering key in the messages published to
   * Cloud Pub/Sub.
   */
  @Test
  public void testOrderingKeysPartition() {
    props.put(CloudPubSubSinkConnector.ORDERING_KEY_SOURCE, "partition");
    task.start(props);
    List<SinkRecord> records = new ArrayList<SinkRecord>();
    SinkRecord record =
        new SinkRecord(
            KAFKA_TOPIC,
            4,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY1,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE1,
            1000,
            50000L,
            TimestampType.CREATE_TIME);
    records.add(record);
    record =
        new SinkRecord(
            KAFKA_TOPIC,
            5,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY2,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE2,
            1001,
            50001L,
            TimestampType.CREATE_TIME);
    records.add(record);
    task.put(records);
    ArgumentCaptor<PubsubMessage> captor = ArgumentCaptor.forClass(PubsubMessage.class);
    verify(publisher, times(2)).publish(captor.capture());
    List<PubsubMessage> requestArgs = captor.getAllValues();

    List<PubsubMessage> expectedMessages = new ArrayList<>();
    Map<String, String> attributes1 = new HashMap<>();
    attributes1.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY1);
    expectedMessages.add(
        PubsubMessage.newBuilder()
            .putAllAttributes(attributes1)
            .setOrderingKey("4")
            .setData(KAFKA_MESSAGE1)
            .build());
    Map<String, String> attributes2 = new HashMap<>();
    attributes2.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY2);
    expectedMessages.add(
        PubsubMessage.newBuilder()
            .putAllAttributes(attributes2)
            .setOrderingKey("5")
            .setData(KAFKA_MESSAGE2)
            .build());

    assertEquals(expectedMessages, requestArgs);
  }

  /**
   * Get some PubsubMessage's which correspond to the SinkRecord's created in {@link
   * #getSampleRecords()}.
   */
  private List<PubsubMessage> getPubsubMessagesFromSampleRecords() {
    List<PubsubMessage> messages = new ArrayList<>();
    Map<String, String> attributes = new HashMap<>();
    attributes.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY1);
    messages.add(
        PubsubMessage.newBuilder().putAllAttributes(attributes).setData(KAFKA_MESSAGE1).build());
    messages.add(
        PubsubMessage.newBuilder().putAllAttributes(attributes).setData(KAFKA_MESSAGE2).build());
    return messages;
  }

  private ApiFuture<String> getSuccessfulPublishFuture() {
    SpyableFuture<String> future = new SpyableFuture("abcd");
    return spy(future);
  }

  private ApiFuture<String> getFailedPublishFuture() {
    return getFailedPublishFuture(new Exception());
  }

  private ApiFuture<String> getFailedPublishFuture(Throwable error) {
    SpyableFuture<String> future = new SpyableFuture(error);
    return spy(future);
  }

  @Test
  public void testStructKeyIsJson() {
    task.start(props);
    Schema keySchema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
    Struct key = new Struct(keySchema).put("id", 45);
    task.put(
        Collections.singletonList(
            new SinkRecord(KAFKA_TOPIC, 0, keySchema, key, STRING_SCHEMA, "fox", 1)));
    assertEquals(
        "{\"id\":45}",
        publishedMessage().getAttributesOrThrow(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE));
  }

  @Test
  public void testBytesKeyIsBase64() {
    props.put(CloudPubSubSinkConnector.ORDERING_KEY_SOURCE, "key");
    task.start(props);
    byte[] key = "dog".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    task.put(
        Collections.singletonList(
            new SinkRecord(KAFKA_TOPIC, 0, Schema.BYTES_SCHEMA, key, STRING_SCHEMA, "fox", 1)));
    PubsubMessage message = publishedMessage();
    assertEquals("ZG9n", message.getAttributesOrThrow(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE));
    assertEquals("ZG9n", message.getOrderingKey());
  }

  @Test
  public void testNumbersAndBooleansAreText() {
    task.start(props);
    task.put(
        Arrays.asList(
            new SinkRecord(KAFKA_TOPIC, 0, null, null, Schema.INT32_SCHEMA, 45, 1),
            new SinkRecord(KAFKA_TOPIC, 0, null, null, Schema.FLOAT64_SCHEMA, 1.5, 2),
            new SinkRecord(KAFKA_TOPIC, 0, null, null, Schema.BOOLEAN_SCHEMA, true, 3)));
    ArgumentCaptor<PubsubMessage> captor = ArgumentCaptor.forClass(PubsubMessage.class);
    verify(publisher, times(3)).publish(captor.capture());
    assertEquals("45", captor.getAllValues().get(0).getData().toStringUtf8());
    assertEquals("1.5", captor.getAllValues().get(1).getData().toStringUtf8());
    assertEquals("true", captor.getAllValues().get(2).getData().toStringUtf8());
  }

  @Test
  public void testFieldAttributes() {
    props.put(CloudPubSubSinkConnector.CPS_MESSAGE_BODY_NAME, BODY);
    task.start(props);
    Schema address = SchemaBuilder.struct().field("city", Schema.STRING_SCHEMA).build();
    Schema schema =
        SchemaBuilder.struct()
            .field("raw", Schema.BYTES_SCHEMA)
            .field("created", Timestamp.SCHEMA)
            .field("address", address)
            .field("props", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build())
            .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .field(BODY, Schema.STRING_SCHEMA)
            .build();
    Struct value =
        new Struct(schema)
            .put("raw", java.nio.ByteBuffer.wrap(new byte[] {'d', 'o', 'g'}))
            .put("created", new java.util.Date(1730718000000L))
            .put("address", new Struct(address).put("city", "Pune"))
            .put("props", Collections.singletonMap("region", "eu"))
            .put("tags", Arrays.asList("a", "b,c"))
            .put(BODY, "order placed");
    task.put(
        Collections.singletonList(new SinkRecord(KAFKA_TOPIC, 0, null, null, schema, value, 1)));
    PubsubMessage message = publishedMessage();
    assertEquals("order placed", message.getData().toStringUtf8());
    assertEquals("ZG9n", message.getAttributesOrThrow("raw"));
    assertEquals("2024-11-04T11:00:00.000Z", message.getAttributesOrThrow("created"));
    assertEquals("{\"city\":\"Pune\"}", message.getAttributesOrThrow("address"));
    assertEquals("{\"region\":\"eu\"}", message.getAttributesOrThrow("props"));
    assertEquals("[\"a\",\"b,c\"]", message.getAttributesOrThrow("tags"));
  }

  @Test
  public void testNestedBodyFieldIsJson() {
    props.put(CloudPubSubSinkConnector.CPS_MESSAGE_BODY_NAME, BODY);
    task.start(props);
    Schema order = SchemaBuilder.struct().field("order", Schema.INT32_SCHEMA).build();
    Schema schema =
        SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field(BODY, order)
            .build();
    Struct value =
        new Struct(schema)
            .put("id", 45)
            .put(
                BODY,
                new Struct(order).put("order", 45));
    task.put(
        Collections.singletonList(new SinkRecord(KAFKA_TOPIC, 0, null, null, schema, value, 1)));
    PubsubMessage message = publishedMessage();
    assertEquals("{\"order\":45}", message.getData().toStringUtf8());
    assertEquals("45", message.getAttributesOrThrow("id"));
  }

  @Test
  public void testSchemalessObjectIsSplitLikeStruct() {
    props.put(CloudPubSubSinkConnector.CPS_MESSAGE_BODY_NAME, BODY);
    task.start(props);
    Map<String, Object> value = new HashMap<>();
    value.put("id", 45L);
    value.put("note", null);
    value.put("address", Collections.singletonMap("city", "Pune"));
    value.put(
        BODY, Collections.singletonMap("order", 45L));
    task.put(Collections.singletonList(new SinkRecord(KAFKA_TOPIC, 0, null, null, null, value, 1)));
    PubsubMessage message = publishedMessage();
    assertEquals("{\"order\":45}", message.getData().toStringUtf8());
    assertEquals("45", message.getAttributesOrThrow("id"));
    assertEquals("{\"city\":\"Pune\"}", message.getAttributesOrThrow("address"));
    assertFalse(message.containsAttributes("note"));
  }

  /**
   * Tests that a struct without a field named by messageBodyName is published whole, as JSON in the
   * body, and that its fields are not added as attributes.
   */
  @Test
  public void testStructWithoutBodyFieldIsPublishedAsJson() {
    props.put(CloudPubSubSinkConnector.PUBLISH_KAFKA_METADATA, "true");
    task.start(props);
    Schema address = SchemaBuilder.struct().field("city", Schema.STRING_SCHEMA).build();
    Schema schema =
        SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("address", address)
            .field("created", Timestamp.SCHEMA)
            .build();
    Struct value =
        new Struct(schema)
            .put("id", 45)
            .put("address", new Struct(address).put("city", "Pune"))
            .put("created", new java.util.Date(1730718000000L));
    task.put(
        Collections.singletonList(
            new SinkRecord(KAFKA_TOPIC, 0, Schema.STRING_SCHEMA, "order-1", schema, value, 1)));
    PubsubMessage message = publishedMessage();
    assertEquals(
        "{\"id\":45,\"address\":{\"city\":\"Pune\"},\"created\":1730718000000}",
        message.getData().toStringUtf8());
    assertFalse(message.containsAttributes("id"));
    assertFalse(message.containsAttributes("address"));
    assertEquals("order-1", message.getAttributesOrThrow(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE));
    assertEquals(KAFKA_TOPIC, message.getAttributesOrThrow(ConnectorUtils.KAFKA_TOPIC_ATTRIBUTE));
  }

  /** Tests that a JSON object without a messageBodyName entry is published whole, as JSON. */
  @Test
  public void testSchemalessObjectWithoutBodyIsPublishedAsJson() {
    task.start(props);
    Map<String, Object> value = new LinkedHashMap<>();
    value.put("id", 45L);
    value.put("address", Collections.singletonMap("city", "Pune"));
    task.put(Collections.singletonList(new SinkRecord(KAFKA_TOPIC, 0, null, null, null, value, 1)));
    PubsubMessage message = publishedMessage();
    assertEquals("{\"id\":45,\"address\":{\"city\":\"Pune\"}}", message.getData().toStringUtf8());
    assertEquals(0, message.getAttributesCount());
  }

  /** Tests that a header Pub/Sub could not accept is left out and the record is still published. */
  @Test
  public void testOversizedHeaderIsDroppedAndRecordPublished() {
    props.put(CloudPubSubSinkConnector.PUBLISH_KAFKA_HEADERS, "true");
    task.start(props);
    SinkRecord record = new SinkRecord(KAFKA_TOPIC, 0, null, null, Schema.STRING_SCHEMA, "a", 1);
    char[] big = new char[1025];
    Arrays.fill(big, 'x');
    record.headers().addString("big", new String(big));
    record.headers().addString("trace", "abc");
    task.put(Collections.singletonList(record));
    PubsubMessage message = publishedMessage();
    assertEquals("a", message.getData().toStringUtf8());
    assertFalse(message.containsAttributes("big"));
    assertEquals("abc", message.getAttributesOrThrow("trace"));
  }

  /**
   * Tests that without messageBodyName a struct is published whole as JSON, even if it has a field
   * named cps_message_body.
   */
  @Test
  public void testBodyFieldIsNotSpecialWhenMessageBodyNameNotSet() {
    task.start(props);
    Schema schema =
        SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field(BODY, Schema.STRING_SCHEMA)
            .build();
    Struct value = new Struct(schema).put("id", 45).put(BODY, "order placed");
    task.put(
        Collections.singletonList(new SinkRecord(KAFKA_TOPIC, 0, null, null, schema, value, 1)));
    PubsubMessage message = publishedMessage();
    assertEquals(
        "{\"id\":45,\"cps_message_body\":\"order placed\"}", message.getData().toStringUtf8());
    assertEquals(0, message.getAttributesCount());
  }

  /**
   * Tests that with messageBodyName set, a struct without that field is still split: the body is
   * empty and every field is an attribute.
   */
  @Test
  public void testMissingBodyFieldIsSplitWhenMessageBodyNameSet() {
    props.put(CloudPubSubSinkConnector.CPS_MESSAGE_BODY_NAME, "payload");
    task.start(props);
    Schema schema =
        SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("city", Schema.STRING_SCHEMA)
            .build();
    Struct value = new Struct(schema).put("id", 45).put("city", "Pune");
    task.put(
        Collections.singletonList(new SinkRecord(KAFKA_TOPIC, 0, null, null, schema, value, 1)));
    PubsubMessage message = publishedMessage();
    assertEquals(ByteString.EMPTY, message.getData());
    assertEquals("45", message.getAttributesOrThrow("id"));
    assertEquals("Pune", message.getAttributesOrThrow("city"));
  }

  /** Tests that with messageBodyName set, a JSON object without that key is split the same way. */
  @Test
  public void testSchemalessObjectWithoutBodyIsSplitWhenMessageBodyNameSet() {
    props.put(CloudPubSubSinkConnector.CPS_MESSAGE_BODY_NAME, "payload");
    task.start(props);
    Map<String, Object> value = new LinkedHashMap<>();
    value.put("id", 45L);
    task.put(Collections.singletonList(new SinkRecord(KAFKA_TOPIC, 0, null, null, null, value, 1)));
    PubsubMessage message = publishedMessage();
    assertEquals(ByteString.EMPTY, message.getData());
    assertEquals("45", message.getAttributesOrThrow("id"));
  }

  /** Tests that a blank messageBodyName counts as not set. */
  @Test
  public void testBlankMessageBodyNameIsNotSet() {
    props.put(CloudPubSubSinkConnector.CPS_MESSAGE_BODY_NAME, "  ");
    task.start(props);
    Map<String, Object> value = new LinkedHashMap<>();
    value.put("id", 45L);
    task.put(Collections.singletonList(new SinkRecord(KAFKA_TOPIC, 0, null, null, null, value, 1)));
    assertEquals("{\"id\":45}", publishedMessage().getData().toStringUtf8());
  }

  @Test
  public void testSchemalessArrayIsJson() {
    task.start(props);
    task.put(
        Collections.singletonList(
            new SinkRecord(KAFKA_TOPIC, 0, null, null, null, Arrays.asList(1L, 2L), 1)));
    assertEquals("[1,2]", publishedMessage().getData().toStringUtf8());
  }

  @Test
  public void testLogicalTypeValuesAreText() {
    task.start(props);
    Schema decimal = Decimal.schema(2);
    task.put(
        Arrays.asList(
            new SinkRecord(KAFKA_TOPIC, 0, null, null, decimal, new BigDecimal("12.50"), 1),
            new SinkRecord(
                KAFKA_TOPIC,
                0,
                null,
                null,
                Timestamp.SCHEMA,
                new java.util.Date(1730718000000L),
                2)));
    ArgumentCaptor<PubsubMessage> captor = ArgumentCaptor.forClass(PubsubMessage.class);
    verify(publisher, times(2)).publish(captor.capture());
    assertEquals("12.50", captor.getAllValues().get(0).getData().toStringUtf8());
    assertEquals("2024-11-04T11:00:00.000Z", captor.getAllValues().get(1).getData().toStringUtf8());
  }

  @Test
  public void testMapBodyBytesAndNullEntries() {
    props.put(CloudPubSubSinkConnector.CPS_MESSAGE_BODY_NAME, BODY);
    task.start(props);
    Schema schema =
        SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.bytes().optional().build()).build();
    Map<String, Object> value = new HashMap<>();
    value.put(
        BODY,
        java.nio.ByteBuffer.wrap(new byte[] {'f', 'o', 'x'}));
    value.put(FIELD_STRING1, null);
    task.put(
        Collections.singletonList(new SinkRecord(KAFKA_TOPIC, 0, null, null, schema, value, 1)));
    PubsubMessage message = publishedMessage();
    assertEquals("fox", message.getData().toStringUtf8());
    assertEquals(0, message.getAttributesCount());
  }

  private PubsubMessage publishedMessage() {
    ArgumentCaptor<PubsubMessage> captor = ArgumentCaptor.forClass(PubsubMessage.class);
    verify(publisher).publish(captor.capture());
    return captor.getValue();
  }

  @Test
  public void testHeadersDoNotReplaceAttributes() {
    props.put(CloudPubSubSinkConnector.PUBLISH_KAFKA_HEADERS, "true");
    props.put(CloudPubSubSinkConnector.PUBLISH_KAFKA_METADATA, "true");
    task.start(props);
    SinkRecord record = record(4, 1000, KAFKA_MESSAGE_KEY1, "a");
    record.headers().addString(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, "fromHeader");
    record.headers().addString(ConnectorUtils.KAFKA_TOPIC_ATTRIBUTE, "fromHeader");
    record.headers().addString("myHeader", "myValue");
    task.put(Collections.singletonList(record));
    ArgumentCaptor<PubsubMessage> captor = ArgumentCaptor.forClass(PubsubMessage.class);
    verify(publisher).publish(captor.capture());
    Map<String, String> attributes = captor.getValue().getAttributesMap();
    assertEquals(KAFKA_MESSAGE_KEY1, attributes.get(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE));
    assertEquals(KAFKA_TOPIC, attributes.get(ConnectorUtils.KAFKA_TOPIC_ATTRIBUTE));
    assertEquals("myValue", attributes.get("myHeader"));
  }

  @Test
  public void testNullHeaderValueIsSkipped() {
    props.put(CloudPubSubSinkConnector.PUBLISH_KAFKA_HEADERS, "true");
    task.start(props);
    SinkRecord record = record(0, 1, null, "a");
    record.headers().addString("empty", null);
    record.headers().addString("myHeader", "myValue");
    task.put(Collections.singletonList(record));
    verify(publisher)
        .publish(message("a").toBuilder().putAttributes("myHeader", "myValue").build());
  }

  @Test
  public void testHeadersDoNotReplaceFieldAttributes() {
    props.put(CloudPubSubSinkConnector.PUBLISH_KAFKA_HEADERS, "true");
    props.put(CloudPubSubSinkConnector.CPS_MESSAGE_BODY_NAME, BODY);
    task.start(props);
    Schema schema =
        SchemaBuilder.struct()
            .field("id", SchemaBuilder.int32())
            .field(BODY, SchemaBuilder.string())
            .build();
    Struct value =
        new Struct(schema)
            .put("id", 45)
            .put(BODY, "order placed");
    SinkRecord record = new SinkRecord(KAFKA_TOPIC, 0, null, null, schema, value, 1);
    record.headers().addString("id", "fromHeader");
    task.put(Collections.singletonList(record));
    assertEquals("45", publishedMessage().getAttributesOrThrow("id"));
  }

  private static SinkRecord record(int partition, long offset, String key, String value) {
    return new SinkRecord(KAFKA_TOPIC, partition, STRING_SCHEMA, key, STRING_SCHEMA, value, offset);
  }

  /** Returns the message the task publishes for a record with this value and no key. */
  private static PubsubMessage message(String value) {
    return PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(value)).build();
  }

  /**
   * Tests that messages cancelled behind a message that ran out of retries share its outcome
   * instead of being published again.
   */
  @Test
  public void testCancelledMessagesBehindOutOfRetriesShareOutcome() {
    props.put(CloudPubSubSinkConnector.ORDERING_KEY_SOURCE, "key");
    startWithBehavior("log");
    SinkRecord first = record(0, 1, KAFKA_MESSAGE_KEY1, "a");
    SinkRecord second = record(0, 2, KAFKA_MESSAGE_KEY1, "b");
    PubsubMessage a = orderedMessage(KAFKA_MESSAGE_KEY1, "a");
    PubsubMessage b = orderedMessage(KAFKA_MESSAGE_KEY1, "b");
    when(publisher.publish(a)).thenReturn(failed(unavailable()));
    when(publisher.publish(b)).thenReturn(failed(cancelled()));
    task.put(Arrays.asList(first, second));
    task.flush(partitions(0));
    verify(publisher).resumePublish(KAFKA_MESSAGE_KEY1);
    verify(publisher, times(1)).publish(a);
    verify(publisher, times(1)).publish(b);
    verify(reporter).report(same(first), any(DataException.class));
    verify(reporter).report(same(second), any(DataException.class));
  }

  /**
   * Tests that every message in a request Pub/Sub rejected goes to the dead letter queue, since
   * Pub/Sub does not say which message was at fault, and none is published again.
   */
  @Test
  public void testRejectedRequestIsSentToDeadLetterQueue() {
    startWithBehavior("log");
    SinkRecord first = record(0, 1, null, "a");
    SinkRecord second = record(0, 2, null, "b");
    when(publisher.publish(message("a"))).thenReturn(failed(invalidArgument()));
    when(publisher.publish(message("b"))).thenReturn(failed(invalidArgument()));
    task.put(Arrays.asList(first, second));
    task.flush(partitions(0));
    verify(publisher, times(1)).publish(message("a"));
    verify(publisher, times(1)).publish(message("b"));
    verify(reporter).report(same(first), any(DataException.class));
    verify(reporter).report(same(second), any(DataException.class));
  }

  /**
   * Tests that messages cancelled behind a rejected message with the same ordering key share its
   * outcome, and that the ordering key is resumed for later messages.
   */
  @Test
  public void testCancelledMessagesBehindRejectedMessageShareOutcome() {
    props.put(CloudPubSubSinkConnector.ORDERING_KEY_SOURCE, "key");
    startWithBehavior("log");
    SinkRecord first = record(0, 1, KAFKA_MESSAGE_KEY1, "a");
    SinkRecord second = record(0, 2, KAFKA_MESSAGE_KEY1, "b");
    PubsubMessage a = orderedMessage(KAFKA_MESSAGE_KEY1, "a");
    PubsubMessage b = orderedMessage(KAFKA_MESSAGE_KEY1, "b");
    when(publisher.publish(a)).thenReturn(failed(invalidArgument()));
    when(publisher.publish(b)).thenReturn(failed(cancelled()));
    task.put(Arrays.asList(first, second));
    task.flush(partitions(0));
    verify(publisher).resumePublish(KAFKA_MESSAGE_KEY1);
    verify(publisher, times(1)).publish(a);
    verify(publisher, times(1)).publish(b);
    verify(reporter).report(same(first), any(DataException.class));
    verify(reporter).report(same(second), any(DataException.class));
  }

  /** Tests that close() forgets the publishes of the closed partitions. */
  @Test
  public void testCloseDropsPendingPublishes() {
    task.start(props);
    when(publisher.publish(any(PubsubMessage.class)))
        .thenReturn(failed(apiError(StatusCode.Code.PERMISSION_DENIED, false)));
    task.put(Collections.singletonList(record(0, 1, null, "a")));
    task.close(Collections.singletonList(new TopicPartition(KAFKA_TOPIC, 0)));
    task.flush(partitions(0));
  }

  /** Tests that a record that cannot be converted fails the task by default. */
  @Test
  public void testConversionErrorFailsTaskByDefault() {
    task.start(props);
    DataException e =
        assertThrows(
            DataException.class,
            () -> task.put(Collections.singletonList(recordThatCannotBeConverted(7))));
    assertTrue(
        e.getMessage()
            .startsWith(
                "Failed to publish the record at topic brown, partition 0, offset 7 to "
                    + "projects/quick/topics/the: it cannot be made into a valid Pub/Sub message: "
                    + "Invalid Java object for schema with type STRING"));
  }

  /** Tests that with behavior.on.error=ignore, a record that cannot be converted is skipped. */
  @Test
  public void testConversionErrorWithIgnoreSkipsRecord() {
    startWithBehavior("ignore");
    task.put(Arrays.asList(recordThatCannotBeConverted(1), record(0, 2, null, "a")));
    verify(reporter, never()).report(any(), any());
    verify(publisher, times(1)).publish(any(PubsubMessage.class));
  }

  /**
   * Tests that with behavior.on.error=log, a record that cannot be converted goes to the dead
   * letter queue and the rest of the batch is published.
   */
  @Test
  public void testConversionErrorWithLogIsSentToDeadLetterQueue() {
    startWithBehavior("log");
    SinkRecord bad = recordThatCannotBeConverted(1);
    task.put(Arrays.asList(bad, record(0, 2, null, "a")));
    verify(reporter).report(same(bad), any(DataException.class));
    verify(publisher, times(1)).publish(any(PubsubMessage.class));
    verify(publisher).publish(message("a"));
  }

  /**
   * Tests that the task fails when the dead letter queue refuses a record during flush(), instead
   * of the worker rewinding and retrying forever.
   */
  @Test
  public void testDeadLetterQueueRefusalInFlushFailsTask() {
    when(reporter.report(any(), any()))
        .thenThrow(new ConnectException("Tolerance exceeded in error handler"));
    startWithBehavior("log");
    when(publisher.publish(any(PubsubMessage.class))).thenReturn(failed(unavailable()));
    task.put(Collections.singletonList(record(0, 1, null, "a")));
    assertThrows(ConnectException.class, () -> task.flush(partitions(0)));
    assertThrows(
        ConnectException.class, () -> task.put(Collections.singletonList(record(0, 2, null, "b"))));
  }

  /**
   * Tests that the task fails when the dead letter queue refuses a record during put(), for example
   * because errors.tolerance is none.
   */
  @Test
  public void testDeadLetterQueueRefusalInPutFailsTask() {
    when(reporter.report(any(), any()))
        .thenThrow(new ConnectException("Tolerance exceeded in error handler"));
    startWithBehavior("log");
    assertThrows(
        ConnectException.class,
        () -> task.put(Collections.singletonList(recordThatCannotBeConverted(1))));
  }

  /**
   * Tests that errors that are not specific to a record fail the task even with
   * behavior.on.error=log, without sending anything to the dead letter queue.
   */
  @Test
  public void testErrorsNotSpecificToRecordFailTask() {
    Throwable[] errors = {
      apiError(StatusCode.Code.NOT_FOUND, false),
      apiError(StatusCode.Code.PERMISSION_DENIED, false),
      apiError(StatusCode.Code.UNAUTHENTICATED, false),
      apiError(StatusCode.Code.FAILED_PRECONDITION, false),
      apiError(StatusCode.Code.UNIMPLEMENTED, false),
      new Exception("unexpected")
    };
    for (Throwable error : errors) {
      setup();
      startWithBehavior("log");
      when(publisher.publish(any(PubsubMessage.class))).thenReturn(failed(error));
      task.put(Collections.singletonList(record(0, 1, null, "a")));
      assertThrows(ConnectException.class, () -> task.flush(partitions(0)));
      assertThrows(
          ConnectException.class,
          () -> task.put(Collections.singletonList(record(0, 2, null, "b"))));
      verify(reporter, never()).report(any(), any());
    }
  }

  /** Tests that a failed dead letter queue write during flush() fails the task. */
  @Test
  public void testFailedDeadLetterQueueWriteFailsTask() {
    CompletableFuture<Void> failedWrite = new CompletableFuture<>();
    failedWrite.completeExceptionally(new RuntimeException("dead letter queue unavailable"));
    when(reporter.report(any(), any())).thenReturn(failedWrite);
    startWithBehavior("log");
    when(publisher.publish(any(PubsubMessage.class))).thenReturn(failed(unavailable()));
    task.put(Collections.singletonList(record(0, 1, null, "a")));
    assertThrows(ConnectException.class, () -> task.flush(partitions(0)));
    assertThrows(
        ConnectException.class, () -> task.put(Collections.singletonList(record(0, 2, null, "b"))));
  }

  /** Tests that flush() leaves the publishes of partitions it was not asked about for later. */
  @Test
  public void testFlushOnlyHandlesGivenPartitions() {
    task.start(props);
    when(publisher.publish(message("a"))).thenReturn(published());
    when(publisher.publish(message("b")))
        .thenReturn(failed(apiError(StatusCode.Code.PERMISSION_DENIED, false)));
    task.put(Arrays.asList(record(0, 1, null, "a"), record(1, 1, null, "b")));
    task.flush(partitions(0));
    assertThrows(ConnectException.class, () -> task.flush(partitions(1)));
  }

  /**
   * Tests that flush() waits for a record whose topic an SMT renamed, because the worker commits
   * its offset under the topic it was consumed from.
   */
  @Test
  public void testFlushWaitsForRecordFromRenamedTopic() {
    task.start(props);
    SinkRecord renamed =
        new SinkRecord(
            "renamed",
            0,
            null,
            null,
            STRING_SCHEMA,
            "a",
            5,
            null,
            TimestampType.NO_TIMESTAMP_TYPE,
            new ConnectHeaders(),
            KAFKA_TOPIC,
            0,
            5);
    when(publisher.publish(any(PubsubMessage.class)))
        .thenReturn(failed(apiError(StatusCode.Code.PERMISSION_DENIED, false)));
    task.put(Collections.singletonList(renamed));
    assertThrows(ConnectException.class, () -> task.flush(partitions(0)));
  }

  /** Tests that a message Pub/Sub rejects on its own fails the task by default. */
  @Test
  public void testInvalidMessageFailsTaskByDefault() {
    task.start(props);
    when(publisher.publish(message("a"))).thenReturn(failed(invalidArgument()));
    task.put(Collections.singletonList(record(0, 1, null, "a")));
    DataException e = assertThrows(DataException.class, () -> task.flush(partitions(0)));
    assertTrue(e.getMessage().contains("INVALID_ARGUMENT"));
    assertThrows(
        ConnectException.class, () -> task.put(Collections.singletonList(record(0, 2, null, "b"))));
  }

  /** Tests that behavior.on.error=log skips a record when no dead letter queue is configured. */
  @Test
  public void testLogWithoutDeadLetterQueueSkipsRecord() {
    when(context.errantRecordReporter()).thenReturn(null);
    startWithBehavior("log");
    task.put(Arrays.asList(recordThatCannotBeConverted(1), record(0, 2, null, "a")));
    verify(publisher, times(1)).publish(any(PubsubMessage.class));
  }

  /** Tests that a message that ran out of retries fails the task by default. */
  @Test
  public void testOutOfRetriesFailsTaskByDefault() {
    task.start(props);
    when(publisher.publish(any(PubsubMessage.class))).thenReturn(failed(unavailable()));
    task.put(Collections.singletonList(record(0, 1, null, "a")));
    assertThrows(DataException.class, () -> task.flush(partitions(0)));
    assertThrows(
        ConnectException.class, () -> task.put(Collections.singletonList(record(0, 2, null, "b"))));
  }

  /** Tests that a message that ran out of retries goes to the dead letter queue, unretried. */
  @Test
  public void testOutOfRetriesIsSentToDeadLetterQueue() {
    startWithBehavior("log");
    SinkRecord record = record(0, 1, null, "a");
    when(publisher.publish(message("a"))).thenReturn(failed(unavailable()));
    task.put(Collections.singletonList(record));
    task.flush(partitions(0));
    ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
    verify(reporter).report(same(record), error.capture());
    assertEquals(
        "Failed to publish the record at topic brown, partition 0, offset 1 to "
            + "projects/quick/topics/the: publishing still failed after retrying for up to "
            + "maxTotalTimeoutMs=60000 ms (retry.backoff.ms=5): UNAVAILABLE from Pub/Sub",
        error.getValue().getMessage());
    verify(publisher, times(1)).publish(message("a"));
  }

  /** Tests that put() forgets publishes that already succeeded, so their records can be freed. */
  @Test
  public void testPutForgetsSucceededPublishes() {
    task.start(props);
    when(publisher.publish(message("a"))).thenReturn(published());
    when(publisher.publish(message("b"))).thenReturn(SettableApiFuture.<String>create());
    task.put(Arrays.asList(record(0, 1, null, "a"), record(0, 2, null, "b")));
    assertEquals(2, task.pendingPublishCount());
    task.put(Collections.<SinkRecord>emptyList());
    assertEquals(1, task.pendingPublishCount());
  }

  /** Tests that a worker older than Connect 2.6, which has no dead letter queue API, works. */
  @Test
  public void testWorkerWithoutDeadLetterQueueApi() {
    when(context.errantRecordReporter()).thenThrow(new NoSuchMethodError());
    startWithBehavior("log");
    task.put(Arrays.asList(recordThatCannotBeConverted(1), record(0, 2, null, "a")));
    verify(publisher, times(1)).publish(any(PubsubMessage.class));
  }

  /** Tests that by default the publisher retries exactly as it did before retry.backoff.ms. */
  @Test
  public void testRetrySettingsDefaults() {
    task.start(props);
    RetrySettings settings = task.buildRetrySettings();
    assertEquals(Duration.ofMillis(5), settings.getInitialRetryDelay());
    assertEquals(2.0, settings.getRetryDelayMultiplier(), 0);
    assertEquals(Duration.ofMillis(Long.MAX_VALUE), settings.getMaxRetryDelay());
    assertEquals(Duration.ofMillis(60000), settings.getTotalTimeout());
    assertEquals(Duration.ofSeconds(10), settings.getInitialRpcTimeout());
    assertEquals(2.0, settings.getRpcTimeoutMultiplier(), 0);
    assertEquals(Duration.ofMillis(10000), settings.getMaxRpcTimeout());
  }

  /** Tests that the publisher retries with the configured backoff and timeouts. */
  @Test
  public void testRetrySettingsUseConfiguredValues() {
    props.put(CloudPubSubSinkConnector.RETRY_BACKOFF_MS, "250");
    props.put(CloudPubSubSinkConnector.MAX_TOTAL_TIMEOUT_MS, "30000");
    props.put(CloudPubSubSinkConnector.MAX_REQUEST_TIMEOUT_MS, "15000");
    task.start(props);
    RetrySettings settings = task.buildRetrySettings();
    assertEquals(Duration.ofMillis(250), settings.getInitialRetryDelay());
    assertEquals(Duration.ofMillis(30000), settings.getTotalTimeout());
    assertEquals(Duration.ofMillis(15000), settings.getMaxRpcTimeout());
  }

  /** Starts the task with the given behavior.on.error on a worker with a dead letter queue. */
  private void startWithBehavior(String behaviorOnError) {
    props.put(CloudPubSubSinkConnector.BEHAVIOR_ON_ERROR, behaviorOnError);
    task.initialize(context);
    task.start(props);
  }

  /** Returns the message the task publishes for a record with this key, ordered by key. */
  private static PubsubMessage orderedMessage(String key, String value) {
    return PubsubMessage.newBuilder()
        .putAttributes(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, key)
        .setOrderingKey(key)
        .setData(ByteString.copyFromUtf8(value))
        .build();
  }

  private static Map<TopicPartition, OffsetAndMetadata> partitions(int... partitions) {
    Map<TopicPartition, OffsetAndMetadata> partitionOffsets = new HashMap<>();
    for (int partition : partitions) {
      partitionOffsets.put(new TopicPartition(KAFKA_TOPIC, partition), null);
    }
    return partitionOffsets;
  }

  private static ApiFuture<String> published() {
    return ApiFutures.immediateFuture("id");
  }

  private static ApiFuture<String> failed(Throwable error) {
    return ApiFutures.immediateFailedFuture(error);
  }

  private static ApiException invalidArgument() {
    return apiError(StatusCode.Code.INVALID_ARGUMENT, false);
  }

  private static ApiException unavailable() {
    return apiError(StatusCode.Code.UNAVAILABLE, true);
  }

  /** The error the publisher gives messages behind a failed one with the same ordering key. */
  private static CancellationException cancelled() {
    return new CancellationException(
        "Execution cancelled because executing previous runnable failed.");
  }

  private static ApiException apiError(StatusCode.Code code, boolean retryable) {
    StatusCode statusCode =
        new StatusCode() {
          @Override
          public Code getCode() {
            return code;
          }

          @Override
          public Object getTransportCode() {
            return code;
          }
        };
    return new ApiException(new Exception(code + " from Pub/Sub"), statusCode, retryable);
  }

  /** Returns a record whose value doesn't match its schema, so it can't be converted. */
  private static SinkRecord recordThatCannotBeConverted(long offset) {
    return new SinkRecord(KAFKA_TOPIC, 0, null, null, Schema.STRING_SCHEMA, 45, offset);
  }
}
