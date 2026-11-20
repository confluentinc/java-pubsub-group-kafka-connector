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

import static com.google.pubsub.kafka.common.ConnectorUtils.getSystemExecutor;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController;
import com.google.api.gax.core.FixedExecutorProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.pubsub.kafka.common.ConnectorCredentialsProvider;
import com.google.pubsub.kafka.common.ConnectorUtils;
import com.google.pubsub.kafka.sink.CloudPubSubSinkConnector.OrderingKeySource;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

/**
 * A {@link SinkTask} used by a {@link CloudPubSubSinkConnector} to write messages to <a
 * href="https://cloud.google.com/pubsub">Google Cloud Pub/Sub</a>.
 */
public class CloudPubSubSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(CloudPubSubSinkTask.class);

  // Maps a topic to another map which contains the outstanding futures per partition
  private Map<String, Map<Integer, OutstandingFuturesForPartition>> allOutstandingFutures =
      new HashMap<>();
  private String cpsProject;
  private String cpsTopic;
  private String cpsEndpoint;
  private boolean useEmulator;
  private String messageBodyName;
  private long maxBufferSize;
  private long maxBufferBytes;
  private long maxOutstandingRequestBytes;
  private long maxOutstandingMessages;
  private int maxDelayThresholdMs;
  private int maxRequestTimeoutMs;
  private int maxTotalTimeoutMs;
  private int maxShutdownTimeoutMs;
  private boolean includeMetadata;
  private boolean includeHeaders;
  private OrderingKeySource orderingKeySource;
  private boolean enableCompression;
  private long compressionBytesThreshold;
  private ConnectorCredentialsProvider gcpCredentialsProvider;
  private com.google.cloud.pubsub.v1.Publisher publisher;
  private final JsonConverter jsonConverter = createJsonConverter();
  // Most header names remembered for warning once each, so headers with ever-changing names
  // cannot grow this set without bound.
  private static final int MAX_WARNED_HEADER_NAMES = 1000;
  private final Set<String> warnedHeaderNames = new HashSet<>();

  /** Holds a list of the publishing futures that have not been processed for a single partition. */
  private class OutstandingFuturesForPartition {
    public List<ApiFuture<String>> futures = new ArrayList<>();
  }

  /**
   * Holds a list of the unpublished messages for a single partition and the total size in bytes of
   * the messages in the list.
   */
  private class UnpublishedMessagesForPartition {
    public List<PubsubMessage> messages = new ArrayList<>();
    public int size = 0;
  }

  public CloudPubSubSinkTask() {}

  @VisibleForTesting
  public CloudPubSubSinkTask(Publisher publisher) {
    this.publisher = publisher;
  }

  @Override
  public String version() {
    return new CloudPubSubSinkConnector().version();
  }

  @Override
  public void start(Map<String, String> props) {
    Map<String, Object> validatedProps = new CloudPubSubSinkConnector().config().parse(props);
    cpsProject = validatedProps.get(ConnectorUtils.CPS_PROJECT_CONFIG).toString();
    cpsTopic = validatedProps.get(ConnectorUtils.CPS_TOPIC_CONFIG).toString();
    cpsEndpoint = validatedProps.get(ConnectorUtils.CPS_ENDPOINT).toString();
    useEmulator = (Boolean) validatedProps.get(ConnectorUtils.CPS_USE_EMULATOR);
    maxBufferSize = (Integer) validatedProps.get(CloudPubSubSinkConnector.MAX_BUFFER_SIZE_CONFIG);
    maxBufferBytes = (Long) validatedProps.get(CloudPubSubSinkConnector.MAX_BUFFER_BYTES_CONFIG);
    maxOutstandingRequestBytes =
        (Long) validatedProps.get(CloudPubSubSinkConnector.MAX_OUTSTANDING_REQUEST_BYTES);
    maxOutstandingMessages =
        (Long) validatedProps.get(CloudPubSubSinkConnector.MAX_OUTSTANDING_MESSAGES);
    maxDelayThresholdMs =
        (Integer) validatedProps.get(CloudPubSubSinkConnector.MAX_DELAY_THRESHOLD_MS);
    maxRequestTimeoutMs =
        (Integer) validatedProps.get(CloudPubSubSinkConnector.MAX_REQUEST_TIMEOUT_MS);
    maxTotalTimeoutMs = (Integer) validatedProps.get(CloudPubSubSinkConnector.MAX_TOTAL_TIMEOUT_MS);
    maxShutdownTimeoutMs =
        (Integer) validatedProps.get(CloudPubSubSinkConnector.MAX_SHUTDOWN_TIMEOUT_MS);
    messageBodyName = (String) validatedProps.get(CloudPubSubSinkConnector.CPS_MESSAGE_BODY_NAME);
    includeMetadata = (Boolean) validatedProps.get(CloudPubSubSinkConnector.PUBLISH_KAFKA_METADATA);
    includeHeaders = (Boolean) validatedProps.get(CloudPubSubSinkConnector.PUBLISH_KAFKA_HEADERS);
    orderingKeySource =
        OrderingKeySource.getEnum(
            (String) validatedProps.get(CloudPubSubSinkConnector.ORDERING_KEY_SOURCE));
    enableCompression = (Boolean) validatedProps.get(CloudPubSubSinkConnector.ENABLE_COMPRESSION);
    compressionBytesThreshold =
        (Long) validatedProps.get(CloudPubSubSinkConnector.COMPRESSION_BYTES_THRESHOLD);
    gcpCredentialsProvider = ConnectorCredentialsProvider.fromConfig(validatedProps);
    if (publisher == null) {
      // Only do this if we did not use the constructor.
      createPublisher();
    }
    log.info("Start CloudPubSubSinkTask");
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    log.debug("Received " + sinkRecords.size() + " messages to send to CPS.");
    for (SinkRecord record : sinkRecords) {
      log.trace("Received record: " + record.toString());
      Map<String, String> attributes = new HashMap<>();
      ByteString value = handleValue(record.valueSchema(), record.value(), attributes);
      String key = null;
      String partition = record.kafkaPartition().toString();
      if (record.key() != null) {
        key = toAttributeValue(record.keySchema(), record.key());
        attributes.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, key);
      }
      if (includeMetadata) {
        attributes.put(ConnectorUtils.KAFKA_TOPIC_ATTRIBUTE, record.topic());
        attributes.put(ConnectorUtils.KAFKA_PARTITION_ATTRIBUTE, partition);
        attributes.put(ConnectorUtils.KAFKA_OFFSET_ATTRIBUTE, Long.toString(record.kafkaOffset()));
        if (record.timestamp() != null) {
          attributes.put(ConnectorUtils.KAFKA_TIMESTAMP_ATTRIBUTE, record.timestamp().toString());
        }
      }
      if (includeHeaders) {
        // A header never replaces an attribute set from the record's value, key or metadata.
        Set<String> recordAttributes = new HashSet<>(attributes.keySet());
        for (Header header : getRecordHeaders(record)) {
          if (header.value() == null) {
            continue;
          }
          if (recordAttributes.contains(header.key())) {
            warnDroppedHeader(
                header.key(), "the message already has an attribute with that name");
            continue;
          }
          attributes.put(header.key(), header.value().toString());
        }
      }
      if (attributes.isEmpty() && (value == null || value.isEmpty())) {
        log.warn("Message received with no value and no attributes. Not publishing message");
        SettableApiFuture<String> nullMessageFuture = SettableApiFuture.<String>create();
        nullMessageFuture.set("No message");
        addPendingMessageFuture(record.topic(), record.kafkaPartition(), nullMessageFuture);
        continue;
      }
      PubsubMessage.Builder builder = PubsubMessage.newBuilder();
      builder.putAllAttributes(attributes);
      if (value != null) {
        builder.setData(value);
      }
      if (orderingKeySource == OrderingKeySource.KEY && key != null && !key.isEmpty()) {
        builder.setOrderingKey(key);
      } else if (orderingKeySource == OrderingKeySource.PARTITION) {
        builder.setOrderingKey(partition);
      }

      PubsubMessage message = builder.build();
      publishMessage(record.topic(), record.kafkaPartition(), message);
    }
  }

  private Iterable<? extends Header> getRecordHeaders(SinkRecord record) {
    ConnectHeaders headers = new ConnectHeaders();
    if (record.headers() != null) {
      int headerCount = 0;
      for (Header header : record.headers()) {
        if (headerCount > 100) {
          warnDroppedHeader(header.key(), "the message already has the most headers allowed");
        } else if (header.key().getBytes().length >= 257) {
          warnDroppedHeader(header.key(), "its name is longer than 256 bytes");
        } else if (String.valueOf(header.value()).getBytes().length >= 1025) {
          warnDroppedHeader(header.key(), "its value is longer than 1024 bytes");
        } else {
          headers.add(header);
          headerCount++;
        }
      }
    }
    return headers;
  }

  /**
   * Logs once per header name why a header is not published, without its value, so the log never
   * holds record data.
   */
  private void warnDroppedHeader(String name, String reason) {
    if (warnedHeaderNames.size() >= MAX_WARNED_HEADER_NAMES || !warnedHeaderNames.add(name)) {
      return;
    }
    String shown = name.length() > 64 ? name.substring(0, 64) + "..." : name;
    log.warn(
        "Not publishing header {} as an attribute: {}. The record is still published; this is"
            + " logged once per header name.",
        shown,
        reason);
  }

  private ByteString handleValue(Schema schema, Object value, Map<String, String> attributes) {
    if (value == null) {
      return null;
    }
    if (value instanceof ByteString) {
      return (ByteString) value;
    }
    if (schema != null && schema.type().isPrimitive()) {
      ConnectSchema.validateValue(schema, value);
    }
    if (value instanceof byte[]) {
      return ByteString.copyFrom((byte[]) value);
    }
    if (value instanceof ByteBuffer) {
      return ByteString.copyFrom((ByteBuffer) value);
    }
    // A top-level struct or map, including a JSON object read without a schema, with a field named
    // by messageBodyName is split into the body and attributes; without one, the whole value is
    // the body as JSON. Attributes are null for the body field itself.
    if (attributes != null && value instanceof Struct) {
      return handleStruct((Struct) value, attributes);
    }
    if (attributes != null && value instanceof Map) {
      return handleMap(schema, (Map<?, ?>) value, attributes);
    }
    // Strings, numbers, booleans, decimals and dates as text; nested structs, maps and arrays as
    // JSON. The same text as in attributes.
    return ByteString.copyFromUtf8(toAttributeValue(schema, value));
  }

  /**
   * Splits a struct: the field named by messageBodyName becomes the body and the others become
   * attributes. A struct without that field is published whole, as JSON in the body.
   */
  private ByteString handleStruct(Struct struct, Map<String, String> attributes) {
    if (struct.schema().field(messageBodyName) == null) {
      return toJson(struct.schema(), struct);
    }
    ByteString msgBody = null;
    for (Field f : struct.schema().fields()) {
      Object val = struct.get(f);
      if (val == null) {
        if (!f.schema().isOptional()) {
          throw new DataException("Struct message missing required field " + f.name());
        } else {
          continue;
        }
      }
      if (f.name().equals(messageBodyName)) {
        msgBody = handleValue(f.schema(), val, null);
      } else {
        attributes.put(f.name(), toAttributeValue(f.schema(), val));
      }
    }
    if (msgBody != null) {
      return msgBody;
    } else {
      return ByteString.EMPTY;
    }
  }

  /**
   * Splits a map the way a struct is split: the entry named by messageBodyName becomes the body and
   * the others become attributes. A map without that entry is published whole, as JSON in the
   * body. The schema is null for schemaless JSON.
   */
  private ByteString handleMap(Schema schema, Map<?, ?> map, Map<String, String> attributes) {
    if (!map.containsKey(messageBodyName)) {
      return toJson(schema, map);
    }
    Schema valueSchema = schema == null ? null : schema.valueSchema();
    ByteString mapBody = null;
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (entry.getValue() == null) {
        continue;
      }
      if (entry.getKey().equals(messageBodyName)) {
        mapBody = handleValue(valueSchema, entry.getValue(), null);
      } else {
        if (valueSchema != null && valueSchema.type().isPrimitive()) {
          // Unlike Struct.put, a map doesn't check its values against the schema. handleValue
          // checks the body entry.
          ConnectSchema.validateValue(valueSchema, entry.getValue());
        }
        attributes.put(entry.getKey().toString(), toAttributeValue(valueSchema, entry.getValue()));
      }
    }
    if (mapBody != null) {
      return mapBody;
    } else {
      return ByteString.EMPTY;
    }
  }

  /**
   * Returns the text of a key or field for an attribute: JSON for structs, maps and arrays, ISO
   * 8601 in UTC for dates and times, base64 for bytes, and plain text for anything else.
   */
  private String toAttributeValue(Schema schema, Object value) {
    if (value instanceof Struct || value instanceof Map || value instanceof List) {
      return toJson(schema, value).toStringUtf8();
    }
    if (value instanceof java.util.Date) {
      return formatDate(schema, (java.util.Date) value);
    }
    if (value instanceof BigDecimal) {
      return ((BigDecimal) value).toPlainString();
    }
    try {
      return Values.convertToString(schema, value);
    } catch (DataException e) {
      // Not a Kafka Connect type, so there is no standard text for it.
      return value.toString();
    }
  }

  /**
   * Formats the Date, Time and Timestamp logical types as Values.convertToString() does, but in
   * UTC: Values uses the worker's time zone and still ends the text with Z.
   */
  private static String formatDate(Schema schema, java.util.Date value) {
    String name = schema == null ? null : schema.name();
    String pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    if (org.apache.kafka.connect.data.Date.LOGICAL_NAME.equals(name)) {
      pattern = "yyyy-MM-dd";
    } else if (Time.LOGICAL_NAME.equals(name)) {
      pattern = "HH:mm:ss.SSS'Z'";
    }
    SimpleDateFormat format = new SimpleDateFormat(pattern);
    format.setTimeZone(TimeZone.getTimeZone("UTC"));
    return format.format(value);
  }

  private ByteString toJson(Schema schema, Object value) {
    return ByteString.copyFrom(jsonConverter.fromConnectData(cpsTopic, schema, value));
  }

  private static JsonConverter createJsonConverter() {
    Map<String, Object> config = new HashMap<>();
    config.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
    config.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
    JsonConverter converter = new JsonConverter();
    converter.configure(config, false);
    return converter;
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> partitionOffsets) {
    log.debug("Flushing...");
    // Process results of all the outstanding futures specified by each TopicPartition.
    for (Map.Entry<TopicPartition, OffsetAndMetadata> partitionOffset :
        partitionOffsets.entrySet()) {
      log.trace("Received flush for partition " + partitionOffset.getKey().toString());
      Map<Integer, OutstandingFuturesForPartition> outstandingFuturesForTopic =
          allOutstandingFutures.get(partitionOffset.getKey().topic());
      if (outstandingFuturesForTopic == null) {
        continue;
      }
      OutstandingFuturesForPartition outstandingFutures =
          outstandingFuturesForTopic.get(partitionOffset.getKey().partition());
      if (outstandingFutures == null) {
        continue;
      }
      try {
        ApiFutures.allAsList(outstandingFutures.futures).get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        outstandingFutures.futures.clear();
      }
    }
    allOutstandingFutures.clear();
  }

  /** Publish all the messages in a partition and store the Future's for each publish request. */
  private void publishMessage(String topic, Integer partition, PubsubMessage message) {
    addPendingMessageFuture(topic, partition, publisher.publish(message));
  }

  private void addPendingMessageFuture(String topic, Integer partition, ApiFuture<String> future) {
    // Get a map containing all futures per partition for the passed in topic.
    Map<Integer, OutstandingFuturesForPartition> outstandingFuturesForTopic =
        allOutstandingFutures.get(topic);
    if (outstandingFuturesForTopic == null) {
      outstandingFuturesForTopic = new HashMap<>();
      allOutstandingFutures.put(topic, outstandingFuturesForTopic);
    }
    // Get the object containing the outstanding futures for this topic and partition..
    OutstandingFuturesForPartition outstandingFutures = outstandingFuturesForTopic.get(partition);
    if (outstandingFutures == null) {
      outstandingFutures = new OutstandingFuturesForPartition();
      outstandingFuturesForTopic.put(partition, outstandingFutures);
    }
    outstandingFutures.futures.add(future);
  }

  private void createPublisher() {
    ProjectTopicName fullTopic = ProjectTopicName.of(cpsProject, cpsTopic);

    BatchingSettings.Builder batchingSettings =
        BatchingSettings.newBuilder()
            .setDelayThreshold(Duration.ofMillis(maxDelayThresholdMs))
            .setElementCountThreshold(maxBufferSize)
            .setRequestByteThreshold(maxBufferBytes);

    if (useFlowControl()) {
      batchingSettings.setFlowControlSettings(
          FlowControlSettings.newBuilder()
              .setMaxOutstandingRequestBytes(maxOutstandingRequestBytes)
              .setMaxOutstandingElementCount(maxOutstandingMessages)
              .setLimitExceededBehavior(FlowController.LimitExceededBehavior.Block)
              .build());
    }

    com.google.cloud.pubsub.v1.Publisher.Builder builder =
        com.google.cloud.pubsub.v1.Publisher.newBuilder(fullTopic)
            .setBatchingSettings(batchingSettings.build())
            .setRetrySettings(
                RetrySettings.newBuilder()
                    // All values that are not configurable come from the defaults for the publisher
                    // client library.
                    .setTotalTimeout(Duration.ofMillis(maxTotalTimeoutMs))
                    .setMaxRpcTimeout(Duration.ofMillis(maxRequestTimeoutMs))
                    .setInitialRetryDelay(Duration.ofMillis(5))
                    .setRetryDelayMultiplier(2)
                    .setMaxRetryDelay(Duration.ofMillis(Long.MAX_VALUE))
                    .setInitialRpcTimeout(Duration.ofSeconds(10))
                    .setRpcTimeoutMultiplier(2)
                    .build())
            .setExecutorProvider(FixedExecutorProvider.create(getSystemExecutor()));

    // Configure endpoint, credentials and channel based on whether we're using emulator or
    // production
    String endpoint = ConnectorUtils.getPubsubEndpoint(useEmulator, cpsEndpoint);
    if (useEmulator) {
      builder
          .setCredentialsProvider(com.google.api.gax.core.NoCredentialsProvider.create())
          .setChannelProvider(
              com.google.api.gax.grpc.InstantiatingGrpcChannelProvider.newBuilder()
                  .setEndpoint(endpoint)
                  .setChannelConfigurator(channel -> channel.usePlaintext())
                  .build());
    } else {
      builder.setCredentialsProvider(gcpCredentialsProvider).setEndpoint(endpoint);
    }
    if (orderingKeySource != OrderingKeySource.NONE) {
      builder.setEnableMessageOrdering(true);
    }
    if (enableCompression) {
      builder.setEnableCompression(true);
      builder.setCompressionBytesThreshold(compressionBytesThreshold);
    }
    try {
      publisher = builder.build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private boolean useFlowControl() {
    // only enable flow control if at least one flow control config has been set
    return maxOutstandingRequestBytes
            != CloudPubSubSinkConnector.DEFAULT_MAX_OUTSTANDING_REQUEST_BYTES
        || maxOutstandingMessages != CloudPubSubSinkConnector.DEFAULT_MAX_OUTSTANDING_MESSAGES;
  }

  @Override
  public void stop() {
    log.info("Stopping CloudPubSubSinkTask");

    if (publisher != null) {
      log.info("Shutting down PubSub publisher");
      try {
        publisher.shutdown();
        boolean terminated =
            publisher.awaitTermination(maxShutdownTimeoutMs, TimeUnit.MILLISECONDS);
        if (!terminated) {
          log.warn(
              String.format(
                  "PubSub publisher did not terminate cleanly in %d ms", maxShutdownTimeoutMs));
        }
      } catch (Exception e) {
        // There is not much we can do here besides logging it as an error
        log.error("An exception occurred while shutting down PubSub publisher", e);
      }
    }
  }
}
