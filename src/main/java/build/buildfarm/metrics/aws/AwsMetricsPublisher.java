// Copyright 2020 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.metrics.aws;

import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.metrics.AbstractMetricsPublisher;
import build.buildfarm.v1test.MetricsConfig;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.amazonaws.services.sns.AmazonSNSAsync;
import com.amazonaws.services.sns.AmazonSNSAsyncClientBuilder;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.util.StringUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.longrunning.Operation;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AwsMetricsPublisher extends AbstractMetricsPublisher {
  private static final Logger logger = Logger.getLogger(AwsMetricsPublisher.class.getName());
  private static AmazonSNSAsync snsClient;

  private String snsTopicOperations;
  private String accessKeyId = null;
  private String secretKey = null;
  private String region;
  private int snsClientMaxConnections;

  public AwsMetricsPublisher(MetricsConfig metricsConfig) {
    super(metricsConfig.getClusterId());
    snsTopicOperations = metricsConfig.getAwsMetricsConfig().getOperationsMetricsTopic();
    region = metricsConfig.getAwsMetricsConfig().getRegion();
    getAwsSecret(metricsConfig.getAwsMetricsConfig().getSecretName());
    snsClientMaxConnections = metricsConfig.getAwsMetricsConfig().getSnsClientMaxConnections();
    if (!StringUtils.isNullOrEmpty(snsTopicOperations)
        && snsClientMaxConnections > 0
        && !StringUtils.isNullOrEmpty(accessKeyId)
        && !StringUtils.isNullOrEmpty(secretKey)
        && !StringUtils.isNullOrEmpty(region)) {
      snsClient = initSnsClient();
    }
  }

  @Override
  public void publishRequestMetadata(Operation operation, RequestMetadata requestMetadata) {
    try {
      if (snsClient != null) {
        snsClient.publishAsync(
            new PublishRequest(
                snsTopicOperations,
                formatRequestMetadataToJson(populateRequestMetadata(operation, requestMetadata))),
            new AsyncHandler<PublishRequest, PublishResult>() {
              @Override
              public void onError(Exception e) {
                logger.log(Level.WARNING, "Could not publish metrics data to SNS.", e);
              }

              @Override
              public void onSuccess(PublishRequest request, PublishResult publishResult) {}
            });
      }
    } catch (Exception e) {
      logger.log(
          Level.WARNING,
          String.format("Could not publish request metadata to SNS for %s.", operation.getName()),
          e);
    }
  }

  private AmazonSNSAsync initSnsClient() {
    logger.log(Level.INFO, "Initializing SNS Client.");
    return AmazonSNSAsyncClientBuilder.standard()
        .withRegion(region)
        .withClientConfiguration(
            new ClientConfiguration().withMaxConnections(snsClientMaxConnections))
        .withCredentials(
            new AWSStaticCredentialsProvider(
                new AWSCredentials() {
                  @Override
                  public String getAWSAccessKeyId() {
                    return accessKeyId;
                  }

                  @Override
                  public String getAWSSecretKey() {
                    return secretKey;
                  }
                }))
        .build();
  }

  @Override
  public void publishMetric(String metricName, Object metricValue) {
    throw new UnsupportedOperationException();
  }

  private void getAwsSecret(String secretName) {
    AWSSecretsManager client = AWSSecretsManagerClientBuilder.standard().withRegion(region).build();
    GetSecretValueRequest getSecretValueRequest =
        new GetSecretValueRequest().withSecretId(secretName);
    GetSecretValueResult getSecretValueResult = null;
    try {
      getSecretValueResult = client.getSecretValue(getSecretValueRequest);
    } catch (Exception e) {
      logger.log(Level.SEVERE, String.format("Could not get secret %s from AWS.", secretName));
      return;
    }
    String secret = null;
    if (getSecretValueResult.getSecretString() != null) {
      secret = getSecretValueResult.getSecretString();
    } else {
      secret =
          new String(Base64.getDecoder().decode(getSecretValueResult.getSecretBinary()).array());
    }

    if (secret != null) {
      try {
        final ObjectMapper objectMapper = new ObjectMapper();
        final HashMap<String, String> secretMap = objectMapper.readValue(secret, HashMap.class);
        accessKeyId = secretMap.get("access_key");
        secretKey = secretMap.get("secret_key");
      } catch (IOException e) {
        logger.log(Level.SEVERE, String.format("Could not parse secret %s from AWS", secretName));
      }
    }
  }
}
