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
import static org.junit.Assert.assertTrue;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.pubsub.kafka.common.ConnectorCredentialsProvider;
import com.google.pubsub.kafka.common.ConnectorUtils;
import com.google.pubsub.kafka.sink.CloudPubSubSinkConnector.BehaviorOnError;
import com.google.pubsub.kafka.sink.CloudPubSubSinkConnector.PublishPermissionCheck;
import io.grpc.Status;
import java.io.IOException;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.Before;
import org.junit.Test;

/** Tests for {@link CloudPubSubSinkConnector}. */
public class CloudPubSubSinkConnectorTest {

  private static final int NUM_TASKS = 10;
  private static final String CPS_PROJECT = "hello-project";
  private static final String CPS_TOPIC = "world";
  private static final String TOPIC_NAME = "projects/hello-project/topics/world";
  // Credentials that can be read without a network call.
  private static final String USER_KEY =
      "{\"type\":\"authorized_user\",\"client_id\":\"id\",\"client_secret\":\"secret\","
          + "\"refresh_token\":\"token\"}";
  // A service account key with a freshly generated private key, readable without a network call.
  private static final String SA_KEY = serviceAccountKey();
  // Credentials that would fetch a token from the URL they name.
  private static final String EXTERNAL_ACCOUNT_KEY =
      "{\"type\":\"external_account\",\"audience\":\"//iam.googleapis.com/projects/1/locations/"
          + "global/workloadIdentityPools/pool/providers/provider\",\"subject_token_type\":"
          + "\"urn:ietf:params:oauth:token-type:jwt\",\"token_url\":"
          + "\"https://sts.googleapis.com/v1/token\",\"credential_source\":"
          + "{\"url\":\"http://169.254.169.254/token\"}}";

  private CloudPubSubSinkConnector connector;
  private Map<String, String> props;
  // What the permission check answers, in place of Pub/Sub.
  private PublishPermissionCheck permissionCheck;
  private final List<String> checkedTopics = new ArrayList<>();

  @Before
  public void setup() {
    permissionCheck = (endpoint, credentials, topic) -> true;
    connector =
        new CloudPubSubSinkConnector(
            (endpoint, credentials, topic) -> {
              checkedTopics.add(endpoint + " " + topic);
              return permissionCheck.canPublish(endpoint, credentials, topic);
            });
    props = new HashMap<>();
    props.put(ConnectorUtils.CPS_PROJECT_CONFIG, CPS_PROJECT);
    props.put(ConnectorUtils.CPS_TOPIC_CONFIG, CPS_TOPIC);
  }

  @Test
  public void testTaskConfigs() {
    connector.start(props);
    List<Map<String, String>> taskConfigs = connector.taskConfigs(NUM_TASKS);
    assertEquals(taskConfigs.size(), NUM_TASKS);
    for (int i = 0; i < taskConfigs.size(); ++i) {
      assertEquals(taskConfigs.get(i), props);
    }
  }

  @Test
  public void testTaskClass() {
    assertEquals(CloudPubSubSinkTask.class, connector.taskClass());
  }

  @Test
  public void testBehaviorOnErrorDefaultsToFail() {
    Map<String, Object> parsed = connector.config().parse(props);
    assertEquals("fail", parsed.get(CloudPubSubSinkConnector.BEHAVIOR_ON_ERROR));
  }

  @Test
  public void testBehaviorOnErrorIgnoresCase() {
    props.put(CloudPubSubSinkConnector.BEHAVIOR_ON_ERROR, "LOG");
    Map<String, Object> parsed = connector.config().parse(props);
    assertEquals(
        BehaviorOnError.LOG,
        BehaviorOnError.getEnum((String) parsed.get(CloudPubSubSinkConnector.BEHAVIOR_ON_ERROR)));
  }

  @Test(expected = ConfigException.class)
  public void testInvalidBehaviorOnError() {
    props.put(CloudPubSubSinkConnector.BEHAVIOR_ON_ERROR, "dlq");
    connector.config().parse(props);
  }

  @Test
  public void testRetryBackoffMsDefault() {
    Map<String, Object> parsed = connector.config().parse(props);
    assertEquals(5, parsed.get(CloudPubSubSinkConnector.RETRY_BACKOFF_MS));
  }

  @Test(expected = ConfigException.class)
  public void testRetryBackoffMsMustBePositive() {
    props.put(CloudPubSubSinkConnector.RETRY_BACKOFF_MS, "0");
    connector.config().parse(props);
  }

  @Test
  public void testValidateRejectsRetryBackoffNotLessThanTotalTimeout() {
    props.put(CloudPubSubSinkConnector.RETRY_BACKOFF_MS, "60000");
    Config config = connector.validate(props);
    assertEquals(1, errorsFor(config, CloudPubSubSinkConnector.RETRY_BACKOFF_MS).size());
  }

  @Test
  public void testValidateAcceptsDefaultRetrySettings() {
    Config config = connector.validate(props);
    assertTrue(errorsFor(config, CloudPubSubSinkConnector.RETRY_BACKOFF_MS).isEmpty());
  }

  /** Tests that validate() asks Pub/Sub whether the credentials may publish to the topic. */
  @Test
  public void testValidateChecksPublishPermission() {
    props.put(ConnectorUtils.GCP_SA_CREDENTIALS_JSON_CONFIG, SA_KEY);
    Config config = connector.validate(props);
    assertEquals(
        Collections.singletonList(ConnectorUtils.CPS_DEFAULT_ENDPOINT + " " + TOPIC_NAME),
        checkedTopics);
    assertEquals(Collections.emptyList(), allErrors(config));
  }

  @Test
  public void testValidateRejectsTopicTheCredentialsCannotPublishTo() {
    props.put(ConnectorUtils.GCP_SA_CREDENTIALS_JSON_CONFIG, SA_KEY);
    permissionCheck = (endpoint, credentials, topic) -> false;
    assertEquals(
        Collections.singletonList(
            "The credentials cannot publish to "
                + TOPIC_NAME
                + ": the topic does not exist, or they lack the pubsub.topics.publish permission, "
                + "which the roles/pubsub.publisher role grants."),
        errorsFor(connector.validate(props), ConnectorUtils.CPS_TOPIC_CONFIG));
  }

  @Test
  public void testValidateReportsRejectedCredentials() {
    props.put(ConnectorUtils.GCP_SA_CREDENTIALS_JSON_CONFIG, SA_KEY);
    permissionCheck =
        (endpoint, credentials, topic) -> {
          throw new ExecutionException(apiError(Status.Code.UNAUTHENTICATED));
        };
    Config config = connector.validate(props);
    assertEquals(1, errorsFor(config, ConnectorUtils.GCP_SA_CREDENTIALS_JSON_CONFIG).size());
    assertTrue(errorsFor(config, ConnectorUtils.CPS_TOPIC_CONFIG).isEmpty());
  }

  @Test
  public void testValidateReportsMissingTopic() {
    props.put(ConnectorUtils.GCP_SA_CREDENTIALS_JSON_CONFIG, SA_KEY);
    permissionCheck =
        (endpoint, credentials, topic) -> {
          throw new ExecutionException(apiError(Status.Code.NOT_FOUND));
        };
    assertEquals(
        Collections.singletonList("Topic " + TOPIC_NAME + " does not exist."),
        errorsFor(connector.validate(props), ConnectorUtils.CPS_TOPIC_CONFIG));
  }

  /** Tests that a denied check is an error; a disabled API or a VPC-SC perimeter also gives it. */
  @Test
  public void testValidateReportsPermissionDenied() {
    props.put(ConnectorUtils.GCP_SA_CREDENTIALS_JSON_CONFIG, SA_KEY);
    permissionCheck =
        (endpoint, credentials, topic) -> {
          throw new ExecutionException(apiError(Status.Code.PERMISSION_DENIED));
        };
    assertEquals(
        Collections.singletonList(
            "Pub/Sub denied the check whether the credentials can publish to "
                + TOPIC_NAME
                + ": PERMISSION_DENIED from Pub/Sub"),
        errorsFor(connector.validate(props), ConnectorUtils.CPS_TOPIC_CONFIG));
  }

  /** Tests that a temporary Pub/Sub error does not reject the configuration. */
  @Test
  public void testValidateAcceptsOnTemporaryErrors() {
    props.put(ConnectorUtils.GCP_SA_CREDENTIALS_JSON_CONFIG, SA_KEY);
    for (Status.Code code :
        Arrays.asList(
            Status.Code.UNAVAILABLE,
            Status.Code.INTERNAL,
            Status.Code.DEADLINE_EXCEEDED,
            Status.Code.RESOURCE_EXHAUSTED)) {
      checkedTopics.clear();
      permissionCheck =
          (endpoint, credentials, topic) -> {
            throw new ExecutionException(apiError(code));
          };
      assertEquals(code.toString(), Collections.emptyList(), allErrors(connector.validate(props)));
      assertEquals(1, checkedTopics.size());
    }
  }

  @Test
  public void testValidateAcceptsUnansweredCheck() {
    props.put(ConnectorUtils.GCP_SA_CREDENTIALS_JSON_CONFIG, SA_KEY);
    permissionCheck =
        (endpoint, credentials, topic) -> {
          throw new TimeoutException();
        };
    assertEquals(Collections.emptyList(), allErrors(connector.validate(props)));
    assertEquals(1, checkedTopics.size());
  }

  /** Tests that a key in the legacy settings is read, but not used to call Pub/Sub. */
  @Test
  public void testValidateDoesNotCheckPermissionWithLegacyKeySetting() {
    props.put(ConnectorUtils.GCP_CREDENTIALS_JSON_CONFIG, USER_KEY);
    assertEquals(Collections.emptyList(), allErrors(connector.validate(props)));
    assertTrue(checkedTopics.isEmpty());
  }

  /** Tests that validation does not fall back to the worker's own credentials. */
  @Test
  public void testValidateDoesNotCheckPermissionWithoutCredentials() {
    assertEquals(Collections.emptyList(), allErrors(connector.validate(props)));
    assertTrue(checkedTopics.isEmpty());
  }

  /** Tests that a key that cannot be read is an error that does not quote the key. */
  @Test
  public void testValidateRejectsMalformedKey() {
    props.put(ConnectorUtils.GCP_CREDENTIALS_JSON_CONFIG, "not a key");
    assertEquals(
        Collections.singletonList(
            "The value of gcp.credentials.json is not a valid Google Cloud credentials key."),
        errorsFor(connector.validate(props), ConnectorUtils.GCP_CREDENTIALS_JSON_CONFIG));
    assertTrue(checkedTopics.isEmpty());
  }

  @Test
  public void testValidateRejectsOtherKeyInServiceAccountSetting() {
    props.put(ConnectorUtils.GCP_SA_CREDENTIALS_JSON_CONFIG, USER_KEY);
    assertEquals(
        Collections.singletonList(
            "The value of gcp.sa.credentials.json is not a valid service account key."),
        errorsFor(connector.validate(props), ConnectorUtils.GCP_SA_CREDENTIALS_JSON_CONFIG));
  }

  @Test
  public void testValidateRejectsBothLegacyCredentialSettings() {
    props.put(ConnectorUtils.GCP_CREDENTIALS_FILE_PATH_CONFIG, "/key.json");
    props.put(ConnectorUtils.GCP_CREDENTIALS_JSON_CONFIG, USER_KEY);
    Config config = connector.validate(props);
    assertEquals(1, errorsFor(config, ConnectorUtils.GCP_CREDENTIALS_FILE_PATH_CONFIG).size());
    assertEquals(1, errorsFor(config, ConnectorUtils.GCP_CREDENTIALS_JSON_CONFIG).size());
  }

  @Test
  public void testValidateSkipsCredentialChecksWithEmulator() {
    props.put(ConnectorUtils.CPS_USE_EMULATOR, "true");
    props.put(ConnectorUtils.GCP_CREDENTIALS_JSON_CONFIG, "not a key");
    assertEquals(Collections.emptyList(), allErrors(connector.validate(props)));
    assertTrue(checkedTopics.isEmpty());
  }

  /** Tests that a key behind a config provider this worker does not have is not checked. */
  @Test
  public void testValidateSkipsUnresolvedConfigProviderReference() {
    props.put(
        ConnectorUtils.GCP_CREDENTIALS_JSON_CONFIG,
        "${file:/mnt/secrets/connector.properties:gcp.credentials.json}");
    assertEquals(Collections.emptyList(), allErrors(connector.validate(props)));
    assertTrue(checkedTopics.isEmpty());
  }

  /** Tests that validation does not use credentials that fetch a token from a URL they name. */
  @Test
  public void testValidateDoesNotUseExternalAccountCredentials() {
    props.put(ConnectorUtils.GCP_CREDENTIALS_JSON_CONFIG, EXTERNAL_ACCOUNT_KEY);
    assertEquals(Collections.emptyList(), allErrors(connector.validate(props)));
    assertTrue(checkedTopics.isEmpty());
  }

  /** Returns a service account key whose private key is generated for the test. */
  private static String serviceAccountKey() {
    try {
      KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
      generator.initialize(2048);
      byte[] encoded = generator.generateKeyPair().getPrivate().getEncoded();
      String privateKey =
          "-----BEGIN PRIVATE KEY-----\\n"
              + Base64.getEncoder().encodeToString(encoded)
              + "\\n-----END PRIVATE KEY-----\\n";
      return "{\"type\":\"service_account\",\"project_id\":\"hello-project\","
          + "\"private_key_id\":\"id\",\"private_key\":\""
          + privateKey
          + "\",\"client_email\":\"sa@hello-project.iam.gserviceaccount.com\","
          + "\"client_id\":\"1\"}";
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }

  private static ApiException apiError(Status.Code code) {
    return ApiExceptionFactory.createException(
        new Exception(code + " from Pub/Sub"), GrpcStatusCode.of(code), false);
  }

  /**
   * Tests that a credentials provider class is checked but not used by validation, because it can
   * need an environment that only the task has.
   */
  @Test
  public void testValidateAcceptsCredentialsProviderClassWithoutCheckingPermission() {
    props.put(
        ConnectorUtils.GCP_CREDENTIALS_PROVIDER_CLASS_CONFIG, StubCredentials.class.getName());
    assertEquals(Collections.emptyList(), allErrors(connector.validate(props)));
    assertTrue(checkedTopics.isEmpty());
  }

  @Test
  public void testValidateRejectsMissingCredentialsProviderClass() {
    props.put(ConnectorUtils.GCP_CREDENTIALS_PROVIDER_CLASS_CONFIG, "com.example.Missing");
    List<String> errors =
        errorsFor(connector.validate(props), ConnectorUtils.GCP_CREDENTIALS_PROVIDER_CLASS_CONFIG);
    assertEquals(1, errors.size());
    assertTrue(errors.get(0), errors.get(0).contains("could not be loaded"));
    assertTrue(checkedTopics.isEmpty());
  }

  @Test
  public void testValidateRejectsCredentialsProviderClassOfWrongType() {
    props.put(ConnectorUtils.GCP_CREDENTIALS_PROVIDER_CLASS_CONFIG, String.class.getName());
    List<String> errors =
        errorsFor(connector.validate(props), ConnectorUtils.GCP_CREDENTIALS_PROVIDER_CLASS_CONFIG);
    assertEquals(1, errors.size());
    assertTrue(errors.get(0), errors.get(0).contains("must extend"));
  }

  /**
   * Tests that a configurable credentials class is configured with the original settings, including
   * ones the connector does not define, and is preferred over a key.
   */
  @Test
  public void testCredentialsProviderClassIsConfiguredWithOriginals() throws IOException {
    props.put(
        ConnectorUtils.GCP_CREDENTIALS_PROVIDER_CLASS_CONFIG, StubCredentials.class.getName());
    props.put(ConnectorUtils.GCP_SA_CREDENTIALS_JSON_CONFIG, USER_KEY);
    props.put("customer.gcp.service.account", "customer@example.iam.gserviceaccount.com");
    Map<String, Object> parsed = connector.config().parse(props);

    Credentials credentials =
        ConnectorCredentialsProvider.fromConfig(parsed, props).getCredentials();

    assertTrue(credentials instanceof StubCredentials);
    assertEquals(
        "customer@example.iam.gserviceaccount.com",
        ((StubCredentials) credentials).configs.get("customer.gcp.service.account"));
  }

  @Test
  public void testCredentialsProviderClassCanBeCredentialsProvider() throws IOException {
    Map<String, Object> config = new HashMap<>();
    config.put(ConnectorUtils.GCP_CREDENTIALS_PROVIDER_CLASS_CONFIG, StubProvider.class.getName());

    Credentials credentials = ConnectorCredentialsProvider.fromConfig(config).getCredentials();

    assertTrue(credentials instanceof GoogleCredentials);
    assertEquals("stub-token", ((GoogleCredentials) credentials).getAccessToken().getTokenValue());
  }

  /** Configurable credentials that record the settings they are configured with. */
  public static class StubCredentials extends GoogleCredentials implements Configurable {
    Map<String, ?> configs;

    @Override
    public void configure(Map<String, ?> configs) {
      this.configs = configs;
    }
  }

  /** A credentials provider that returns a fixed access token. */
  public static class StubProvider implements CredentialsProvider {
    @Override
    public Credentials getCredentials() {
      return GoogleCredentials.create(new AccessToken("stub-token", null));
    }
  }

  private static List<String> allErrors(Config config) {
    List<String> errors = new ArrayList<>();
    for (ConfigValue value : config.configValues()) {
      for (String error : value.errorMessages()) {
        errors.add(value.name() + ": " + error);
      }
    }
    return errors;
  }

  private static List<String> errorsFor(Config config, String name) {
    for (ConfigValue value : config.configValues()) {
      if (value.name().equals(name)) {
        return value.errorMessages();
      }
    }
    throw new AssertionError("No config value for " + name);
  }
}
