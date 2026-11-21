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

import com.google.pubsub.kafka.common.ConnectorUtils;
import com.google.pubsub.kafka.sink.CloudPubSubSinkConnector.BehaviorOnError;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.Before;
import org.junit.Test;

/** Tests for {@link CloudPubSubSinkConnector}. */
public class CloudPubSubSinkConnectorTest {

  private static final int NUM_TASKS = 10;
  private static final String CPS_PROJECT = "hello";
  private static final String CPS_TOPIC = "world";

  private CloudPubSubSinkConnector connector;
  private Map<String, String> props;

  @Before
  public void setup() {
    connector = new CloudPubSubSinkConnector();
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

  private static List<String> errorsFor(Config config, String name) {
    for (ConfigValue value : config.configValues()) {
      if (value.name().equals(name)) {
        return value.errorMessages();
      }
    }
    throw new AssertionError("No config value for " + name);
  }
}
