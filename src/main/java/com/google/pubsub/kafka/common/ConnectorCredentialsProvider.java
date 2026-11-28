/*
 * Copyright 2018 Google Inc.
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
package com.google.pubsub.kafka.common;

import com.google.api.gax.core.CredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.Configurable;

public class ConnectorCredentialsProvider implements CredentialsProvider {
  private static final List<String> GCP_SCOPE =
      Arrays.asList("https://www.googleapis.com/auth/cloud-platform");

  CredentialsProvider impl;

  private ConnectorCredentialsProvider(CredentialsProvider impl) {
    this.impl = impl;
  }

  public static ConnectorCredentialsProvider fromConfig(Map<String, Object> config) {
    return fromConfig(config, config);
  }

  /**
   * Like {@link #fromConfig(Map)}, but a credentials provider class set in {@code config} is
   * configured with {@code originals}, so it also sees settings that the connector does not define.
   */
  public static ConnectorCredentialsProvider fromConfig(
      Map<String, Object> config, Map<String, ?> originals) {
    Object providerClass = config.get(ConnectorUtils.GCP_CREDENTIALS_PROVIDER_CLASS_CONFIG);
    if (providerClass != null && !providerClass.toString().trim().isEmpty()) {
      return fromProviderClass(providerClass.toString().trim(), originals);
    }
    // If both the `GCP_SA_CREDENTIALS_*` and `GCP_CREDENTIALS_FILE_*` properties are set,
    // give preference to the `GCP_SA_CREDENTIALS_*` variants.
    String credentialsSAPath =
        config.get(ConnectorUtils.GCP_SA_CREDENTIALS_FILE_PATH_CONFIG).toString();
    String credentialsSAJson = config.get(ConnectorUtils.GCP_SA_CREDENTIALS_JSON_CONFIG).toString();

    if (!credentialsSAPath.isEmpty()) {
      return ConnectorCredentialsProvider.getServiceAccountFromFile(credentialsSAPath);
    } else if (!credentialsSAJson.isEmpty()) {
      return ConnectorCredentialsProvider.getServiceAccountFromJson(credentialsSAJson);
    }

    String credentialsPath = config.get(ConnectorUtils.GCP_CREDENTIALS_FILE_PATH_CONFIG).toString();
    String credentialsJson = config.get(ConnectorUtils.GCP_CREDENTIALS_JSON_CONFIG).toString();
    if (!credentialsPath.isEmpty()) {
      if (!credentialsJson.isEmpty()) {
        throw new IllegalArgumentException(
            "May not set both "
                + ConnectorUtils.GCP_CREDENTIALS_FILE_PATH_CONFIG
                + " and "
                + ConnectorUtils.GCP_CREDENTIALS_JSON_CONFIG);
      }
      return ConnectorCredentialsProvider.fromFile(credentialsPath);
    } else if (!credentialsJson.isEmpty()) {
      return ConnectorCredentialsProvider.fromJson(credentialsJson);
    }

    return ConnectorCredentialsProvider.fromDefault();
  }

  public static ConnectorCredentialsProvider getServiceAccountFromFile(String credentialsSAPath) {
    return new ConnectorCredentialsProvider(
        () ->
            ServiceAccountCredentials.fromStream(new FileInputStream(credentialsSAPath))
                .createWithUseJwtAccessWithScope(true)
                .createScoped(GCP_SCOPE));
  }

  public static ConnectorCredentialsProvider getServiceAccountFromJson(String credentialsSAJson) {
    return new ConnectorCredentialsProvider(
        () ->
            ServiceAccountCredentials.fromStream(
                    new ByteArrayInputStream(credentialsSAJson.getBytes()))
                .createWithUseJwtAccessWithScope(true)
                .createScoped(GCP_SCOPE));
  }

  /**
   * Prefer {@link #getServiceAccountFromFile(String)} instead due to a potential security risk. See
   * {@see <a
   * href="https://cloud.google.com/docs/authentication/external/externally-sourced-credentials">documentation</a>}
   * for more details. This method does not validate the credential configuration. The security risk
   * occurs when a credential configuration is accepted from a source that is not under your control
   * and used without validation on your side.
   */
  public static ConnectorCredentialsProvider fromFile(String credentialPath) {
    return new ConnectorCredentialsProvider(
        () ->
            GoogleCredentials.fromStream(new FileInputStream(credentialPath))
                .createScoped(GCP_SCOPE));
  }

  /**
   * Prefer {@link #getServiceAccountFromJson(String)} instead due to a potential security risk. See
   * {@see <a
   * href="https://cloud.google.com/docs/authentication/external/externally-sourced-credentials">documentation</a>}
   * for more details. This method does not validate the credential configuration. The security risk
   * occurs when a credential configuration is accepted from a source that is not under your control
   * and used without validation on your side.
   */
  public static ConnectorCredentialsProvider fromJson(String credentialsJson) {
    return new ConnectorCredentialsProvider(
        () ->
            GoogleCredentials.fromStream(new ByteArrayInputStream(credentialsJson.getBytes()))
                .createScoped(GCP_SCOPE));
  }

  /**
   * Returns credentials from a class named by {@link
   * ConnectorUtils#GCP_CREDENTIALS_PROVIDER_CLASS_CONFIG}. The class must have a public no-arg
   * constructor and either extend {@link GoogleCredentials} or implement {@link
   * CredentialsProvider}. If it implements {@link Configurable}, it is configured with {@code
   * originals} before use.
   */
  public static ConnectorCredentialsProvider fromProviderClass(
      String className, Map<String, ?> originals) {
    return new ConnectorCredentialsProvider(
        () -> {
          Object provider;
          try {
            provider = loadProviderClass(className).getConstructor().newInstance();
          } catch (ReflectiveOperationException e) {
            throw new IOException("Could not create credentials provider " + className, e);
          }
          if (provider instanceof Configurable) {
            ((Configurable) provider).configure(originals);
          }
          if (provider instanceof GoogleCredentials) {
            return (GoogleCredentials) provider;
          }
          return ((CredentialsProvider) provider).getCredentials();
        });
  }

  /**
   * Loads a credentials provider class and checks that {@link #fromProviderClass(String, Map)} can
   * use it.
   *
   * @throws IllegalArgumentException if the class cannot be found or cannot be used
   */
  public static Class<?> loadProviderClass(String className) {
    Class<?> providerClass;
    try {
      providerClass =
          Class.forName(className, true, ConnectorCredentialsProvider.class.getClassLoader());
    } catch (ClassNotFoundException | LinkageError e) {
      throw new IllegalArgumentException("Class " + className + " could not be loaded.", e);
    }
    if (!GoogleCredentials.class.isAssignableFrom(providerClass)
        && !CredentialsProvider.class.isAssignableFrom(providerClass)) {
      throw new IllegalArgumentException(
          "Class "
              + className
              + " must extend "
              + GoogleCredentials.class.getName()
              + " or implement "
              + CredentialsProvider.class.getName()
              + ".");
    }
    try {
      providerClass.getConstructor();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          "Class " + className + " must have a public no-arg constructor.", e);
    }
    return providerClass;
  }

  public static ConnectorCredentialsProvider fromDefault() {
    return new ConnectorCredentialsProvider(
        () -> GoogleCredentials.getApplicationDefault().createScoped(GCP_SCOPE));
  }

  @Override
  public Credentials getCredentials() throws IOException {
    return impl.getCredentials();
  }
}
