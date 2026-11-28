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

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.common.annotations.VisibleForTesting;
import com.google.iam.v1.TestIamPermissionsRequest;
import com.google.pubsub.kafka.common.ConnectorCredentialsProvider;
import com.google.pubsub.kafka.common.ConnectorUtils;
import com.google.pubsub.kafka.common.Version;
import com.google.pubsub.v1.ProjectTopicName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SinkConnector} that writes messages to a specified topic in <a
 * href="https://cloud.google.com/pubsub">Google Cloud Pub/Sub</a>.
 */
public class CloudPubSubSinkConnector extends SinkConnector {

  private static final Logger log = LoggerFactory.getLogger(CloudPubSubSinkConnector.class);

  public static final String MAX_BUFFER_SIZE_CONFIG = "maxBufferSize";
  public static final String MAX_BUFFER_BYTES_CONFIG = "maxBufferBytes";
  public static final String MAX_OUTSTANDING_REQUEST_BYTES = "maxOutstandingRequestBytes";
  public static final String MAX_OUTSTANDING_MESSAGES = "maxOutstandingMessages";
  public static final String MAX_DELAY_THRESHOLD_MS = "delayThresholdMs";
  public static final String MAX_REQUEST_TIMEOUT_MS = "maxRequestTimeoutMs";
  public static final String MAX_TOTAL_TIMEOUT_MS = "maxTotalTimeoutMs";
  public static final String MAX_SHUTDOWN_TIMEOUT_MS = "maxShutdownTimeoutMs";
  public static final String ENABLE_COMPRESSION = "enableCompression";
  public static final String COMPRESSION_BYTES_THRESHOLD = "compressionBytesThreshold";
  public static final int DEFAULT_MAX_BUFFER_SIZE = 100;
  public static final long DEFAULT_MAX_BUFFER_BYTES = 9500000L;
  public static final int DEFAULT_DELAY_THRESHOLD_MS = 100;
  public static final int DEFAULT_REQUEST_TIMEOUT_MS = 10000;
  public static final int DEFAULT_TOTAL_TIMEOUT_MS = 60000;
  public static final int DEFAULT_SHUTDOWN_TIMEOUT_MS = 60000;
  public static final long DEFAULT_MAX_OUTSTANDING_REQUEST_BYTES = Long.MAX_VALUE;
  public static final long DEFAULT_MAX_OUTSTANDING_MESSAGES = Long.MAX_VALUE;
  public static final String CPS_MESSAGE_BODY_NAME = "messageBodyName";
  public static final String DEFAULT_MESSAGE_BODY_NAME = "";
  public static final String PUBLISH_KAFKA_METADATA = "metadata.publish";
  public static final String PUBLISH_KAFKA_HEADERS = "headers.publish";
  public static final String ORDERING_KEY_SOURCE = "orderingKeySource";
  public static final String DEFAULT_ORDERING_KEY_SOURCE = "none";
  public static final String BEHAVIOR_ON_ERROR = "behavior.on.error";
  public static final String DEFAULT_BEHAVIOR_ON_ERROR = "fail";
  public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
  public static final int DEFAULT_RETRY_BACKOFF_MS = 5;
  // The longest wait between two retries of a failed publish, unless retry.backoff.ms is longer.
  public static final int MAX_RETRY_DELAY_MS = 10000;
  public static final boolean DEFAULT_ENABLE_COMPRESSION = false;
  public static final long DEFAULT_COMPRESSION_BYTES_THRESHOLD = 240L;

  // The permission needed to publish, which the roles/pubsub.publisher role grants.
  static final String PUBLISH_PERMISSION = "pubsub.topics.publish";
  static final long PERMISSION_CHECK_TIMEOUT_MS = 10000;
  // A config provider reference, such as ${file:/path:key}, that the worker could not resolve.
  private static final Pattern CONFIG_PROVIDER_REFERENCE = Pattern.compile("\\$\\{.+}");
  // The credentials settings, in the order ConnectorCredentialsProvider.fromConfig() prefers them.
  // The settings that only accept a service account key.
  private static final List<String> SERVICE_ACCOUNT_CONFIGS =
      Arrays.asList(
          ConnectorUtils.GCP_SA_CREDENTIALS_FILE_PATH_CONFIG,
          ConnectorUtils.GCP_SA_CREDENTIALS_JSON_CONFIG);
  private static final List<String> CREDENTIALS_CONFIGS =
      Arrays.asList(
          ConnectorUtils.GCP_SA_CREDENTIALS_FILE_PATH_CONFIG,
          ConnectorUtils.GCP_SA_CREDENTIALS_JSON_CONFIG,
          ConnectorUtils.GCP_CREDENTIALS_FILE_PATH_CONFIG,
          ConnectorUtils.GCP_CREDENTIALS_JSON_CONFIG);

  /** Defines the accepted values for the {@link #ORDERING_KEY_SOURCE}. */
  public enum OrderingKeySource {
    NONE("none"),
    KEY("key"),
    PARTITION("partition");

    private String value;

    OrderingKeySource(String value) {
      this.value = value;
    }

    public String toString() {
      return value;
    }

    public static OrderingKeySource getEnum(String value) {
      if (value.equals("none")) {
        return OrderingKeySource.NONE;
      } else if (value.equals("key")) {
        return OrderingKeySource.KEY;
      } else if (value.equals("partition")) {
        return OrderingKeySource.PARTITION;
      } else {
        return null;
      }
    }

    /** Validator class for {@link CloudPubSubSinkConnector.OrderingKeySource}. */
    public static class Validator implements ConfigDef.Validator {

      @Override
      public void ensureValid(String name, Object o) {
        String value = (String) o;
        if (!value.equals(CloudPubSubSinkConnector.OrderingKeySource.NONE.toString())
            && !value.equals(CloudPubSubSinkConnector.OrderingKeySource.KEY.toString())
            && !value.equals(CloudPubSubSinkConnector.OrderingKeySource.PARTITION.toString())) {
          throw new ConfigException(
              "Valid values for "
                  + CloudPubSubSinkConnector.ORDERING_KEY_SOURCE
                  + " are "
                  + Arrays.toString(OrderingKeySource.values()));
        }
      }
    }
  }

  /** Defines the accepted values for the {@link #BEHAVIOR_ON_ERROR}. */
  public enum BehaviorOnError {
    FAIL("fail"),
    LOG("log"),
    IGNORE("ignore");

    private String value;

    BehaviorOnError(String value) {
      this.value = value;
    }

    public String toString() {
      return value;
    }

    /** Returns the value with the given name, ignoring case, or null if there is none. */
    public static BehaviorOnError getEnum(String value) {
      for (BehaviorOnError behavior : BehaviorOnError.values()) {
        if (behavior.value.equalsIgnoreCase(value)) {
          return behavior;
        }
      }
      return null;
    }
  }

  /** Asks Pub/Sub whether credentials may publish to a topic. */
  @VisibleForTesting
  interface PublishPermissionCheck {
    boolean canPublish(String endpoint, CredentialsProvider credentials, String topic)
        throws IOException, ExecutionException, InterruptedException, TimeoutException;
  }

  private final PublishPermissionCheck publishPermissionCheck;
  private Map<String, String> props;

  public CloudPubSubSinkConnector() {
    this(CloudPubSubSinkConnector::canPublish);
  }

  @VisibleForTesting
  CloudPubSubSinkConnector(PublishPermissionCheck publishPermissionCheck) {
    this.publishPermissionCheck = publishPermissionCheck;
  }

  @Override
  public String version() {
    return Version.version();
  }

  @Override
  public void start(Map<String, String> props) {
    this.props = props;
    log.info("Started the CloudPubSubSinkConnector.");
  }

  @Override
  public Class<? extends Task> taskClass() {
    return CloudPubSubSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    // Each task will get the exact same configuration. Delegate all config validation to the task.
    ArrayList<Map<String, String>> configs = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      Map<String, String> config = new HashMap<>(props);
      configs.add(config);
    }
    return configs;
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef()
        .define(
            ConnectorUtils.CPS_PROJECT_CONFIG,
            Type.STRING,
            Importance.HIGH,
            "The project containing the topic to which to publish.")
        .define(
            ConnectorUtils.CPS_TOPIC_CONFIG,
            Type.STRING,
            Importance.HIGH,
            "The topic to which to publish.")
        .define(
            MAX_BUFFER_SIZE_CONFIG,
            Type.INT,
            DEFAULT_MAX_BUFFER_SIZE,
            ConfigDef.Range.between(1, Integer.MAX_VALUE),
            Importance.MEDIUM,
            "The maximum number of messages that can be received for the messages on a topic "
                + "partition before publishing them to Cloud Pub/Sub.")
        .define(
            MAX_BUFFER_BYTES_CONFIG,
            Type.LONG,
            DEFAULT_MAX_BUFFER_BYTES,
            ConfigDef.Range.between(1, DEFAULT_MAX_BUFFER_BYTES),
            Importance.MEDIUM,
            "The maximum number of bytes that can be received for the messages on a topic "
                + "partition before publishing the messages to Cloud Pub/Sub.")
        .define(
            MAX_OUTSTANDING_REQUEST_BYTES,
            Type.LONG,
            DEFAULT_MAX_OUTSTANDING_REQUEST_BYTES,
            Importance.MEDIUM,
            "The maximum outstanding bytes from incomplete requests before the task blocks.")
        .define(
            MAX_OUTSTANDING_MESSAGES,
            Type.LONG,
            DEFAULT_MAX_OUTSTANDING_MESSAGES,
            Importance.MEDIUM,
            "The maximum outstanding incomplete messages before the task blocks.")
        .define(
            MAX_DELAY_THRESHOLD_MS,
            Type.INT,
            DEFAULT_DELAY_THRESHOLD_MS,
            ConfigDef.Range.between(1, Integer.MAX_VALUE),
            Importance.MEDIUM,
            "The maximum amount of time to wait after receiving the first message in a batch for a "
                + "before publishing the messages to Cloud Pub/Sub.")
        .define(
            MAX_REQUEST_TIMEOUT_MS,
            Type.INT,
            DEFAULT_REQUEST_TIMEOUT_MS,
            ConfigDef.Range.between(10000, Integer.MAX_VALUE),
            Importance.MEDIUM,
            "The maximum amount of time to wait for a single publish request to Cloud Pub/Sub.")
        .define(
            MAX_TOTAL_TIMEOUT_MS,
            Type.INT,
            DEFAULT_TOTAL_TIMEOUT_MS,
            ConfigDef.Range.between(10000, Integer.MAX_VALUE),
            Importance.MEDIUM,
            "The maximum amount of time to wait for a publish to complete (including retries) in "
                + "Cloud Pub/Sub. If it still fails, the task stops, whatever \""
                + BEHAVIOR_ON_ERROR
                + "\" is, and publishes again from the last committed offset when it restarts.")
        .define(
            RETRY_BACKOFF_MS,
            Type.INT,
            DEFAULT_RETRY_BACKOFF_MS,
            ConfigDef.Range.between(1, Integer.MAX_VALUE),
            Importance.LOW,
            "How long to wait before the first retry of a failed publish to Cloud Pub/Sub. Each "
                + "later retry waits twice as long as the one before, up to 10 seconds (or this "
                + "value, if it is longer), until \""
                + MAX_TOTAL_TIMEOUT_MS
                + "\" runs out.")
        .define(
            MAX_SHUTDOWN_TIMEOUT_MS,
            Type.INT,
            DEFAULT_SHUTDOWN_TIMEOUT_MS,
            ConfigDef.Range.between(10000, Integer.MAX_VALUE),
            Importance.MEDIUM,
            "The maximum amount of time to wait for a publisher to shutdown when stopping task "
                + "in Kafka Connect.")
        .define(
            PUBLISH_KAFKA_METADATA,
            Type.BOOLEAN,
            false,
            Importance.MEDIUM,
            "When true, include the Kafka topic, partition, offset, and timestamp as message "
                + "attributes when a message is published to Cloud Pub/Sub.")
        .define(
            PUBLISH_KAFKA_HEADERS,
            Type.BOOLEAN,
            false,
            Importance.MEDIUM,
            "When true, include any headers as attributes when a message is published to Cloud Pub/Sub.")
        .define(
            CPS_MESSAGE_BODY_NAME,
            Type.STRING,
            DEFAULT_MESSAGE_BODY_NAME,
            Importance.MEDIUM,
            "For a struct or map value, the name of the top-level field or key whose value becomes "
                + "the Pub/Sub message body; the other fields become attributes. When empty (the "
                + "default), the whole value is published as JSON in the message body.")
        .define(
            ConnectorUtils.GCP_CREDENTIALS_FILE_PATH_CONFIG,
            Type.STRING,
            "",
            Importance.HIGH,
            "Due to a potential security vulnerability, prefer "
                + ConnectorUtils.GCP_SA_CREDENTIALS_FILE_PATH_CONFIG)
        .define(
            ConnectorUtils.GCP_SA_CREDENTIALS_FILE_PATH_CONFIG,
            Type.STRING,
            "",
            Importance.HIGH,
            "Path to the GCP Service Account (SA) JSON key file.")
        .define(
            ConnectorUtils.GCP_CREDENTIALS_JSON_CONFIG,
            Type.STRING,
            "",
            Importance.HIGH,
            "Due to a potential security vulnerability, prefer "
                + ConnectorUtils.GCP_SA_CREDENTIALS_JSON_CONFIG)
        .define(
            ConnectorUtils.GCP_SA_CREDENTIALS_JSON_CONFIG,
            Type.STRING,
            "",
            Importance.HIGH,
            "The literal JSON content of the GCP Service Account (SA) key, passed as a string.")
        .define(
            ORDERING_KEY_SOURCE,
            Type.STRING,
            DEFAULT_ORDERING_KEY_SOURCE,
            new OrderingKeySource.Validator(),
            Importance.MEDIUM,
            "What to use to populate the Pub/Sub message ordering key. Possible values are "
                + "\"none\", \"key\", or \"partition\".")
        .define(
            BEHAVIOR_ON_ERROR,
            Type.STRING,
            DEFAULT_BEHAVIOR_ON_ERROR,
            ConfigDef.CaseInsensitiveValidString.in(
                BehaviorOnError.FAIL.toString(),
                BehaviorOnError.LOG.toString(),
                BehaviorOnError.IGNORE.toString()),
            Importance.MEDIUM,
            "What to do with a record that cannot be published to Cloud Pub/Sub, because it cannot "
                + "be converted or Pub/Sub rejects it. \"fail\" stops the task. \"log\" logs the "
                + "error and sends the record to the dead letter queue, if one is configured. "
                + "\"ignore\" skips the record. Errors that are not specific to a record, such as "
                + "a missing topic or permission, or publishing that still fails after \""
                + MAX_TOTAL_TIMEOUT_MS
                + "\", always stop the task.")
        .define(
            ENABLE_COMPRESSION,
            Type.BOOLEAN,
            DEFAULT_ENABLE_COMPRESSION,
            Importance.MEDIUM,
            "When \"true\", use gRPC Gzip compression on publish requests before sending them "
                + "to Cloud Pub/Sub.")
        .define(
            COMPRESSION_BYTES_THRESHOLD,
            Type.LONG,
            DEFAULT_COMPRESSION_BYTES_THRESHOLD,
            Importance.MEDIUM,
            "The number of bytes at which to compress a request when publishing to  "
                + "Cloud Pub/Sub. Only takes effect if \"enableCompression\" is \"true\".")
        .define(
            ConnectorUtils.CPS_ENDPOINT,
            Type.STRING,
            ConnectorUtils.CPS_DEFAULT_ENDPOINT,
            new ConnectorUtils.CpsEndpointValidator(),
            Importance.LOW,
            "The Pub/Sub endpoint to use.")
        .define(
            ConnectorUtils.CPS_USE_EMULATOR,
            Type.BOOLEAN,
            false,
            Importance.LOW,
            "When true, use the Pub/Sub emulator instead of the production service.");
  }

  /**
   * Validates the configuration when the connector is created or changed. Besides the checks of
   * each setting, this checks that the credentials can be read and asks Pub/Sub whether they may
   * publish to the topic, so that these mistakes fail now rather than once records arrive.
   */
  @Override
  public Config validate(Map<String, String> connectorConfigs) {
    Config config = super.validate(connectorConfigs);
    Map<String, ConfigValue> values = new HashMap<>();
    for (ConfigValue value : config.configValues()) {
      values.put(value.name(), value);
    }
    ConfigValue retryBackoff = values.get(RETRY_BACKOFF_MS);
    ConfigValue totalTimeout = values.get(MAX_TOTAL_TIMEOUT_MS);
    if (retryBackoff.errorMessages().isEmpty()
        && totalTimeout.errorMessages().isEmpty()
        && (Integer) retryBackoff.value() >= (Integer) totalTimeout.value()) {
      retryBackoff.addErrorMessage(
          RETRY_BACKOFF_MS
              + " must be less than "
              + MAX_TOTAL_TIMEOUT_MS
              + ", otherwise a failed publish is never retried.");
    }
    // The emulator needs no credentials.
    if (!Boolean.TRUE.equals(values.get(ConnectorUtils.CPS_USE_EMULATOR).value())) {
      validateCredentialsAndPermission(values);
    }
    return config;
  }

  /**
   * Checks that the credentials can be read, then asks Pub/Sub whether they may publish to the
   * topic.
   */
  private void validateCredentialsAndPermission(Map<String, ConfigValue> values) {
    Map<String, Object> credentialsConfigs = new HashMap<>();
    String credentialsConfig = null;
    for (String name : CREDENTIALS_CONFIGS) {
      ConfigValue value = values.get(name);
      if (!value.errorMessages().isEmpty()) {
        return;
      }
      String text = value.value() == null ? "" : value.value().toString();
      credentialsConfigs.put(name, text);
      if (credentialsConfig == null && !text.isEmpty()) {
        credentialsConfig = name;
      }
    }
    if (ConnectorUtils.GCP_CREDENTIALS_FILE_PATH_CONFIG.equals(credentialsConfig)
        && !credentialsConfigs
            .get(ConnectorUtils.GCP_CREDENTIALS_JSON_CONFIG)
            .toString()
            .isEmpty()) {
      String message =
          "Set only one of "
              + ConnectorUtils.GCP_CREDENTIALS_FILE_PATH_CONFIG
              + " and "
              + ConnectorUtils.GCP_CREDENTIALS_JSON_CONFIG
              + ".";
      values.get(ConnectorUtils.GCP_CREDENTIALS_FILE_PATH_CONFIG).addErrorMessage(message);
      values.get(ConnectorUtils.GCP_CREDENTIALS_JSON_CONFIG).addErrorMessage(message);
      return;
    }
    if (credentialsConfig != null
        && CONFIG_PROVIDER_REFERENCE
            .matcher(credentialsConfigs.get(credentialsConfig).toString())
            .matches()) {
      // Only a worker with that config provider can read the credentials.
      return;
    }
    if (credentialsConfig == null) {
      // The task falls back to the worker's application default credentials, which validation
      // does not use: they are the worker's identity, not the connector's.
      return;
    }
    ConnectorCredentialsProvider credentials =
        ConnectorCredentialsProvider.fromConfig(credentialsConfigs);
    try {
      credentials.getCredentials();
    } catch (IOException | RuntimeException e) {
      // The exception's message is left out, because it can quote the key.
      values
          .get(credentialsConfig)
          .addErrorMessage(unreadableCredentialsMessage(credentialsConfig));
      return;
    }
    if (!SERVICE_ACCOUNT_CONFIGS.contains(credentialsConfig)) {
      // Keys in the other settings can make the client fetch a token from a URL, file or program
      // that they name, which validation does not do. Service account keys in these settings sign
      // their own tokens.
      return;
    }
    ConfigValue project = values.get(ConnectorUtils.CPS_PROJECT_CONFIG);
    ConfigValue topic = values.get(ConnectorUtils.CPS_TOPIC_CONFIG);
    ConfigValue endpoint = values.get(ConnectorUtils.CPS_ENDPOINT);
    if (!project.errorMessages().isEmpty()
        || !topic.errorMessages().isEmpty()
        || !endpoint.errorMessages().isEmpty()) {
      return;
    }
    String topicName = ProjectTopicName.format((String) project.value(), (String) topic.value());
    ConfigValue credentialsValue = values.get(credentialsConfig);
    String cannotCheck =
        "Could not check whether the credentials can publish to " + topicName + ": ";
    try {
      if (!publishPermissionCheck.canPublish((String) endpoint.value(), credentials, topicName)) {
        topic.addErrorMessage(
            "The credentials cannot publish to "
                + topicName
                + ": the topic does not exist, or they lack the "
                + PUBLISH_PERMISSION
                + " permission, which the roles/pubsub.publisher role grants.");
      }
    } catch (ExecutionException | RuntimeException e) {
      Throwable error = e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
      StatusCode.Code code =
          error instanceof ApiException ? ((ApiException) error).getStatusCode().getCode() : null;
      if (code == StatusCode.Code.UNAUTHENTICATED) {
        credentialsValue.addErrorMessage(
            "Pub/Sub did not accept the credentials. The key may have been deleted or disabled.");
      } else if (code == StatusCode.Code.NOT_FOUND) {
        topic.addErrorMessage("Topic " + topicName + " does not exist.");
      } else if (code == StatusCode.Code.PERMISSION_DENIED) {
        // Also returned when the Pub/Sub API is disabled or a VPC Service Controls perimeter
        // blocks the request.
        topic.addErrorMessage(
            "Pub/Sub denied the check whether the credentials can publish to "
                + topicName
                + ": "
                + CloudPubSubSinkTask.describe(error));
      } else {
        // A temporary error, such as Pub/Sub being unavailable, says nothing about the
        // configuration. The task stops if publishing fails.
        log.warn(
            cannotCheck + CloudPubSubSinkTask.describe(error) + ". Accepting the configuration.");
      }
    } catch (TimeoutException e) {
      log.warn(
          cannotCheck
              + "Pub/Sub at "
              + endpoint.value()
              + " did not answer within "
              + PERMISSION_CHECK_TIMEOUT_MS / 1000
              + " seconds. Accepting the configuration.");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warn(cannotCheck + "the check was interrupted. Accepting the configuration.");
    } catch (IOException e) {
      // Reading the credentials failed, which was already checked above.
      credentialsValue.addErrorMessage(unreadableCredentialsMessage(credentialsConfig));
    }
  }

  private static String unreadableCredentialsMessage(String credentialsConfig) {
    switch (credentialsConfig) {
      case ConnectorUtils.GCP_SA_CREDENTIALS_FILE_PATH_CONFIG:
      case ConnectorUtils.GCP_CREDENTIALS_FILE_PATH_CONFIG:
        return "Could not read a Google Cloud credentials key from the file that "
            + credentialsConfig
            + " names.";
      case ConnectorUtils.GCP_SA_CREDENTIALS_JSON_CONFIG:
        return "The value of " + credentialsConfig + " is not a valid service account key.";
      default:
        return "The value of "
            + credentialsConfig
            + " is not a valid Google Cloud credentials key.";
    }
  }

  /** Asks Pub/Sub whether the credentials have the publish permission on the topic. */
  private static boolean canPublish(String endpoint, CredentialsProvider credentials, String topic)
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    TopicAdminSettings settings =
        TopicAdminSettings.newBuilder()
            .setEndpoint(endpoint)
            .setCredentialsProvider(credentials)
            .build();
    try (TopicAdminClient client = TopicAdminClient.create(settings)) {
      // Unlike reading the topic, which roles/pubsub.publisher does not allow, this call needs no
      // permission of its own.
      TestIamPermissionsRequest request =
          TestIamPermissionsRequest.newBuilder()
              .setResource(topic)
              .addPermissions(PUBLISH_PERMISSION)
              .build();
      return client
          .testIamPermissionsCallable()
          .futureCall(request)
          .get(PERMISSION_CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS)
          .getPermissionsList()
          .contains(PUBLISH_PERMISSION);
    }
  }

  @Override
  public void stop() {}
}
