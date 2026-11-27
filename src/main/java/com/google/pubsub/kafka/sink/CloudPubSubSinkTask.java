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
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController;
import com.google.api.gax.core.FixedExecutorProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.pubsub.kafka.common.ConnectorCredentialsProvider;
import com.google.pubsub.kafka.common.ConnectorUtils;
import com.google.pubsub.kafka.sink.CloudPubSubSinkConnector.BehaviorOnError;
import com.google.pubsub.kafka.sink.CloudPubSubSinkConnector.OrderingKeySource;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
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

  // Maps each topic partition, as the worker commits its offsets, to the publishes that flush()
  // has not handled yet.
  private final Map<TopicPartition, List<PendingPublish>> pendingPublishes = new HashMap<>();
  // Writes structs, maps and arrays as plain JSON, with decimals as numbers.
  private final JsonConverter jsonConverter = createJsonConverter();
  private String cpsProject;
  private String cpsTopic;
  private String fullTopicName;
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
  private int retryBackoffMs;
  private int maxShutdownTimeoutMs;
  private boolean includeMetadata;
  private boolean includeHeaders;
  private OrderingKeySource orderingKeySource;
  private BehaviorOnError behaviorOnError;
  private boolean enableCompression;
  private long compressionBytesThreshold;
  private ConnectorCredentialsProvider gcpCredentialsProvider;
  private com.google.cloud.pubsub.v1.Publisher publisher;
  private ErrantRecordReporter reporter;
  // Set once a failure stops the task. Throwing from flush() only makes the worker rewind and send
  // the records again, so put() and flush() keep throwing this to fail the task instead.
  private ConnectException fatalError;
  private boolean originalTopicSupported = true;
  // True when messageBodyName is set, so struct and map values are split into body and attributes.
  private boolean splitBody;
  // The record being converted, so a warning can say where it is.
  private SinkRecord currentRecord;

  /** A message handed to the publisher, and the record it was built from. */
  private static class PendingPublish {
    private final SinkRecord record;
    private final PubsubMessage message;
    private final ApiFuture<String> future;

    PendingPublish(SinkRecord record, PubsubMessage message, ApiFuture<String> future) {
      this.record = record;
      this.message = message;
      this.future = future;
    }
  }

  /** How flush() handles a publish that failed. */
  private enum PublishFailure {
    /** Pub/Sub rejected the request the message was sent in. */
    INVALID_MESSAGE,
    /**
     * The publisher retried a temporary error until the total timeout ran out. Nothing is wrong
     * with the message, so it stops the task instead of going to the dead letter queue.
     */
    OUT_OF_RETRIES,
    /** Never sent because an earlier message with the same ordering key failed. */
    CANCELLED,
    /** Not specific to the message, such as a missing topic or permission. */
    FATAL
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
    fullTopicName = ProjectTopicName.format(cpsProject, cpsTopic);
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
    retryBackoffMs = (Integer) validatedProps.get(CloudPubSubSinkConnector.RETRY_BACKOFF_MS);
    maxShutdownTimeoutMs =
        (Integer) validatedProps.get(CloudPubSubSinkConnector.MAX_SHUTDOWN_TIMEOUT_MS);
    messageBodyName = (String) validatedProps.get(CloudPubSubSinkConnector.CPS_MESSAGE_BODY_NAME);
    splitBody = messageBodyName != null && !messageBodyName.trim().isEmpty();
    includeMetadata = (Boolean) validatedProps.get(CloudPubSubSinkConnector.PUBLISH_KAFKA_METADATA);
    includeHeaders = (Boolean) validatedProps.get(CloudPubSubSinkConnector.PUBLISH_KAFKA_HEADERS);
    orderingKeySource =
        OrderingKeySource.getEnum(
            (String) validatedProps.get(CloudPubSubSinkConnector.ORDERING_KEY_SOURCE));
    behaviorOnError =
        BehaviorOnError.getEnum(
            (String) validatedProps.get(CloudPubSubSinkConnector.BEHAVIOR_ON_ERROR));
    enableCompression = (Boolean) validatedProps.get(CloudPubSubSinkConnector.ENABLE_COMPRESSION);
    compressionBytesThreshold =
        (Long) validatedProps.get(CloudPubSubSinkConnector.COMPRESSION_BYTES_THRESHOLD);
    gcpCredentialsProvider = ConnectorCredentialsProvider.fromConfig(validatedProps);
    reporter = getErrantRecordReporter();
    if (publisher == null) {
      // Only do this if we did not use the constructor.
      createPublisher();
    }
    log.info("Start CloudPubSubSinkTask");
  }

  /** Returns the worker's dead letter queue reporter, or null if it has none. */
  private ErrantRecordReporter getErrantRecordReporter() {
    if (context == null) {
      return null;
    }
    try {
      return context.errantRecordReporter();
    } catch (NoSuchMethodError | NoClassDefFoundError e) {
      // Connect before 2.6 has no dead letter queue for connectors.
      return null;
    }
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    throwIfFailed();
    forgetPublished();
    log.debug("Received " + sinkRecords.size() + " messages to send to CPS.");
    for (SinkRecord record : sinkRecords) {
      log.trace("Received record: " + record.toString());
      PubsubMessage message;
      try {
        message = buildMessage(record);
      } catch (RuntimeException e) {
        // A DataException explains itself; for anything else the exception class says what broke.
        String reason = e instanceof DataException ? e.getMessage() : e.toString();
        // The worker waits for dead letter queue writes made during put() before it commits.
        handleFailedRecord(record, "it cannot be made into a valid Pub/Sub message: " + reason, e);
        continue;
      }
      if (message == null) {
        log.warn("Message received with no value and no attributes. Not publishing message");
        continue;
      }
      publishMessage(record, message);
    }
  }

  /** Builds the message for a record, or returns null if the record has nothing to publish. */
  private PubsubMessage buildMessage(SinkRecord record) {
    Map<String, String> attributes = new HashMap<>();
    currentRecord = record;
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
        if (recordAttributes.contains(header.key())) {
          warnDroppedHeader(
              record,
              header.key(),
              "the message already has an attribute with that name; the record is published"
                  + " without the header");
          continue;
        }
        attributes.put(header.key(), headerText(header));
      }
    }
    if (attributes.isEmpty() && (value == null || value.isEmpty())) {
      return null;
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
    return message;
  }

  private Iterable<? extends Header> getRecordHeaders(SinkRecord record) {
    ConnectHeaders headers = new ConnectHeaders();
    if (record.headers() != null) {
      int headerCount = 0;
      for (Header header : record.headers()) {
        if (header.value() == null) {
          continue;
        }
        if (headerCount > 100) {
          warnDroppedHeader(
              record,
              header.key(),
              "the record already has 101 headers, the most the connector publishes; Pub/Sub"
                  + " rejects messages with more than 100 attributes");
        } else if (header.key().getBytes(StandardCharsets.UTF_8).length >= 257) {
          warnDroppedHeader(
              record,
              header.key(),
              "its name is longer than 256 bytes; the record is published without the header");
        } else if (headerText(header).getBytes(StandardCharsets.UTF_8).length >= 1025) {
          warnDroppedHeader(
              record,
              header.key(),
              "its value is longer than 1024 bytes; the record is published without the header");
        } else {
          headers.add(header);
          headerCount++;
        }
      }
    }
    return headers;
  }

  /** Logs that a record has no messageBodyName field, which may be a typo. */
  private void warnMissingBodyField() {
    log.warn(
        "The record at topic {}, partition {}, offset {} has no field named {} (messageBodyName),"
            + " so its message body is empty and all its fields are published as attributes.",
        currentRecord.topic(),
        currentRecord.kafkaPartition(),
        currentRecord.kafkaOffset(),
        messageBodyName);
  }

  /**
   * Logs why a header is not published, with where the record is but not its key, value or the
   * header's value, so the log never holds record data.
   */
  private static void warnDroppedHeader(SinkRecord record, String name, String reason) {
    String shown = name.length() > 64 ? name.substring(0, 64) + "..." : name;
    log.warn(
        "Not publishing header {} of the record at topic {}, partition {}, offset {} as an"
            + " attribute: {}.",
        shown,
        record.topic(),
        record.kafkaPartition(),
        record.kafkaOffset(),
        reason);
  }

  /**
   * Returns the text of a header for its attribute. Kafka Connect's header converter has already
   * turned the header into a typed value, which Values writes back as text: on a worker in UTC,
   * dates, JSON arrays and objects come out as they were sent. A value of a class Kafka Connect
   * doesn't know, for example from a custom transform, falls back to its own text.
   */
  private static String headerText(Header header) {
    try {
      return Values.convertToString(header.schema(), header.value());
    } catch (DataException e) {
      return header.value().toString();
    }
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
    // A top-level struct or map, including a JSON object read without a schema, is published whole
    // as JSON when messageBodyName is not set, and split into body and attributes when it is.
    // Attributes are null for the body field itself.
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
   * Publishes a struct whole, as JSON, when messageBodyName is not set. Otherwise splits it: the
   * field named by messageBodyName becomes the body and the others become attributes; without that
   * field, the body is empty.
   */
  private ByteString handleStruct(Struct struct, Map<String, String> attributes) {
    if (!splitBody) {
      return toJson(struct.schema(), struct);
    }
    if (struct.schema().field(messageBodyName) == null) {
      warnMissingBodyField();
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
   * Handles a map the way a struct is handled: published whole, as JSON, when messageBodyName is
   * not set; otherwise the entry named by messageBodyName becomes the body and the others become
   * attributes. The schema is null for schemaless JSON.
   */
  private ByteString handleMap(Schema schema, Map<?, ?> map, Map<String, String> attributes) {
    if (!splitBody) {
      return toJson(schema, map);
    }
    if (!map.containsKey(messageBodyName)) {
      warnMissingBodyField();
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
          // checks the body entry. The key is left out of the error: it is record data, and the
          // error is logged when the record is skipped or sent to the dead letter queue.
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
      if (schema != null) {
        // The value doesn't match its schema. Fail with Connect's standard error, which names the
        // expected type but, unlike the Values error, not the value itself.
        ConnectSchema.validateValue(schema, value);
      }
      // Not a Kafka Connect type and no schema, so there is no standard text for it.
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
    throwIfFailed();
    List<PendingPublish> publishes = new ArrayList<>();
    for (TopicPartition partition : partitionOffsets.keySet()) {
      log.trace("Received flush for partition " + partition.toString());
      List<PendingPublish> publishesForPartition = pendingPublishes.remove(partition);
      if (publishesForPartition != null) {
        publishes.addAll(publishesForPartition);
      }
    }
    try {
      List<Future<Void>> deadLetterWrites = new ArrayList<>();
      handlePublishResults(publishes, deadLetterWrites);
      waitForDeadLetterWrites(deadLetterWrites);
    } catch (RuntimeException e) {
      fatalError = e instanceof ConnectException ? (ConnectException) e : new ConnectException(e);
      throw fatalError;
    }
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    // flush() has already handled these partitions, unless the worker lost them without a commit.
    // Their records are then redelivered to the task that takes the partitions over.
    for (TopicPartition partition : partitions) {
      pendingPublishes.remove(partition);
    }
  }

  private void throwIfFailed() {
    if (fatalError != null) {
      throw fatalError;
    }
  }

  /**
   * Forgets the publishes that already succeeded, so that their records and messages are not kept
   * until the next flush().
   */
  private void forgetPublished() {
    for (List<PendingPublish> publishes : pendingPublishes.values()) {
      publishes.removeIf(publish -> publish.future.isDone() && failureOf(publish.future) == null);
    }
  }

  @VisibleForTesting
  int pendingPublishCount() {
    int count = 0;
    for (List<PendingPublish> publishes : pendingPublishes.values()) {
      count += publishes.size();
    }
    return count;
  }

  /**
   * Waits for the publishes and applies {@code behavior.on.error} to the ones that failed. Returns
   * normally once every failure is handled, so that the offsets of the published records and of the
   * failed ones can both be committed.
   */
  private void handlePublishResults(
      List<PendingPublish> publishes, List<Future<Void>> deadLetterWrites) {
    waitForAll(publishes);
    List<PendingPublish> failed = new ArrayList<>();
    for (PendingPublish publish : publishes) {
      if (failureOf(publish.future) != null) {
        failed.add(publish);
      }
    }
    if (failed.isEmpty()) {
      return;
    }

    // An error that is not specific to one record stops the task before any record is skipped.
    // So does running out of retries: Pub/Sub was unavailable or throttling, nothing was wrong with
    // the records, and the restarted task publishes them again from the last committed offset.
    for (PendingPublish publish : failed) {
      Throwable error = failureOf(publish.future);
      PublishFailure failure = classify(publish, error);
      if (failure == PublishFailure.FATAL || failure == PublishFailure.OUT_OF_RETRIES) {
        throw fatal(publish, error);
      }
    }

    // A failed publish pauses its ordering key in the publisher, which then cancels every later
    // message with that key until the key is resumed.
    Set<String> pausedOrderingKeys = new HashSet<>();
    for (PendingPublish publish : failed) {
      String orderingKey = publish.message.getOrderingKey();
      if (!orderingKey.isEmpty() && pausedOrderingKeys.add(orderingKey)) {
        publisher.resumePublish(orderingKey);
      }
    }

    // Pub/Sub rejects a whole request for one bad message without saying which one, so every
    // message in that request is handled the same way.
    for (PendingPublish publish : failed) {
      Throwable error = failureOf(publish.future);
      handleFailedPublish(publish, failureReason(publish, error), error, deadLetterWrites);
    }
  }

  private String failureReason(PendingPublish publish, Throwable error) {
    PublishFailure failure = classify(publish, error);
    switch (failure) {
      case INVALID_MESSAGE:
        return "Pub/Sub rejected the publish request this record was sent in, which can hold up to "
            + maxBufferSize
            + " messages; this record may not be the one at fault: "
            + describe(error);
      case CANCELLED:
        return "it was not sent because an earlier message with the same ordering key failed";
      default:
        // Fatal failures and running out of retries stop the task before records are handled.
        throw new IllegalStateException("No record-level reason for a " + failure + " failure");
    }
  }

  private void handleFailedPublish(
      PendingPublish publish, String reason, Throwable error, List<Future<Void>> deadLetterWrites) {
    Future<Void> deadLetterWrite = handleFailedRecord(publish.record, reason, error);
    if (deadLetterWrite != null) {
      deadLetterWrites.add(deadLetterWrite);
    }
  }

  /**
   * Applies {@code behavior.on.error} to a record that could not be published. Returns the dead
   * letter queue write, or null if the record was not sent to the dead letter queue.
   */
  private Future<Void> handleFailedRecord(SinkRecord record, String reason, Throwable error) {
    String message =
        "Failed to publish " + describe(record) + " to " + fullTopicName + ": " + reason;
    switch (behaviorOnError) {
      case LOG:
        if (reporter == null) {
          log.warn(message + ". No dead letter queue is configured, so the record is skipped.");
          return null;
        }
        log.warn(message + ". Sending the record to the dead letter queue.");
        // Report the record exactly as the worker passed it, so that the worker writes the original
        // bytes and tracks the write under the topic partition the record was consumed from.
        return reporter.report(record, new DataException(message, error));
      case IGNORE:
        log.debug(message + ". Skipping the record.", error);
        return null;
      default:
        throw new DataException(message, error);
    }
  }

  private ConnectException fatal(PendingPublish publish, Throwable error) {
    String reason =
        classify(publish, error) == PublishFailure.OUT_OF_RETRIES
            ? "publishing still failed after retrying for up to "
                + CloudPubSubSinkConnector.MAX_TOTAL_TIMEOUT_MS
                + "="
                + maxTotalTimeoutMs
                + " ms ("
                + CloudPubSubSinkConnector.RETRY_BACKOFF_MS
                + "="
                + retryBackoffMs
                + "): "
                + describe(error)
                + ". The error is not specific to this record, so the task is stopping; when it"
                + " restarts, it publishes again from the last committed offset."
            : describe(error)
                + ". The error is not specific to this record, so the task is stopping.";
    return new ConnectException(
        "Failed to publish " + describe(publish.record) + " to " + fullTopicName + ": " + reason,
        error);
  }

  /** Describes where a record came from, without any of its data. */
  private String describe(SinkRecord record) {
    TopicPartition partition = originalTopicPartition(record);
    long offset = originalTopicSupported ? record.originalKafkaOffset() : record.kafkaOffset();
    return "the record at topic "
        + partition.topic()
        + ", partition "
        + partition.partition()
        + ", offset "
        + offset;
  }

  /** Describes a publish error, starting with its gRPC code if it has one. */
  static String describe(Throwable error) {
    if (!(error instanceof ApiException)) {
      return error.toString();
    }
    // The cause is the gRPC error, whose message is the code and Pub/Sub's description.
    String code = ((ApiException) error).getStatusCode().getCode().toString();
    Throwable cause = error.getCause() != null ? error.getCause() : error;
    String description = cause.getMessage() == null ? "" : cause.getMessage();
    return description.startsWith(code) ? description : code + ": " + description;
  }

  private static PublishFailure classify(PendingPublish publish, Throwable error) {
    if (error instanceof CancellationException) {
      // The publisher cancels a message only when an earlier one with the same ordering key failed.
      // Any other cancellation is unexpected, so it stops the task.
      return publish.message.getOrderingKey().isEmpty()
          ? PublishFailure.FATAL
          : PublishFailure.CANCELLED;
    }
    if (error instanceof ApiException) {
      ApiException apiError = (ApiException) error;
      if (apiError.getStatusCode().getCode() == StatusCode.Code.INVALID_ARGUMENT) {
        return PublishFailure.INVALID_MESSAGE;
      }
      if (apiError.isRetryable()) {
        return PublishFailure.OUT_OF_RETRIES;
      }
    }
    return PublishFailure.FATAL;
  }

  /** Returns the error a completed publish failed with, or null if it succeeded. */
  private static Throwable failureOf(ApiFuture<String> future) {
    try {
      future.get();
      return null;
    } catch (ExecutionException e) {
      return e.getCause() != null ? e.getCause() : e;
    } catch (CancellationException e) {
      return e;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConnectException("Interrupted while waiting for publishes to Cloud Pub/Sub", e);
    }
  }

  /** Waits until every publish has completed, successfully or not. */
  private static void waitForAll(List<PendingPublish> publishes) {
    List<ApiFuture<String>> futures = new ArrayList<>();
    for (PendingPublish publish : publishes) {
      futures.add(publish.future);
    }
    try {
      ApiFutures.successfulAsList(futures).get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConnectException("Interrupted while waiting for publishes to Cloud Pub/Sub", e);
    } catch (ExecutionException e) {
      throw new ConnectException("Failed to wait for publishes to Cloud Pub/Sub", e);
    }
  }

  /**
   * Waits for the dead letter queue writes made during flush(). The worker only waits for the ones
   * made during put(), and an offset must not be committed for a record that reached neither
   * Pub/Sub nor the dead letter queue.
   */
  private static void waitForDeadLetterWrites(List<Future<Void>> deadLetterWrites) {
    for (Future<Void> deadLetterWrite : deadLetterWrites) {
      try {
        deadLetterWrite.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while writing to the dead letter queue", e);
      } catch (ExecutionException e) {
        throw new ConnectException(
            "Failed to write a record to the dead letter queue", e.getCause());
      }
    }
  }

  /** Publishes a message and keeps it until flush() handles the result. */
  private void publishMessage(SinkRecord record, PubsubMessage message) {
    PendingPublish publish = new PendingPublish(record, message, publisher.publish(message));
    pendingPublishes
        .computeIfAbsent(originalTopicPartition(record), partition -> new ArrayList<>())
        .add(publish);
  }

  /**
   * Returns the topic partition the worker commits the record's offset for: the one the record was
   * consumed from, before any SMT changed it. Connect before 3.6 does not expose it, so this falls
   * back to the record's own topic partition there.
   */
  private TopicPartition originalTopicPartition(SinkRecord record) {
    if (originalTopicSupported) {
      try {
        return new TopicPartition(record.originalTopic(), record.originalKafkaPartition());
      } catch (NoSuchMethodError e) {
        originalTopicSupported = false;
      }
    }
    return new TopicPartition(record.topic(), record.kafkaPartition());
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
            .setRetrySettings(buildRetrySettings())
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

  @VisibleForTesting
  RetrySettings buildRetrySettings() {
    return RetrySettings.newBuilder()
        // The other values that are not configurable come from the defaults of earlier versions of
        // the publisher client library.
        .setTotalTimeout(Duration.ofMillis(maxTotalTimeoutMs))
        .setMaxRpcTimeout(Duration.ofMillis(maxRequestTimeoutMs))
        .setInitialRetryDelay(Duration.ofMillis(retryBackoffMs))
        .setRetryDelayMultiplier(2)
        // Without a cap, the waits double until one can outlast an outage that has already ended,
        // or the rest of the total timeout.
        .setMaxRetryDelay(
            Duration.ofMillis(
                Math.max(CloudPubSubSinkConnector.MAX_RETRY_DELAY_MS, retryBackoffMs)))
        .setInitialRpcTimeout(Duration.ofSeconds(10))
        .setRpcTimeoutMultiplier(2)
        .build();
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
