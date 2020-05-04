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

package build.buildfarm.common.metrics.aws;

import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.metrics.AbstractMetricsPublisher;
import build.buildfarm.v1test.MetricsConfig;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNSAsync;
import com.amazonaws.services.sns.AmazonSNSAsyncClientBuilder;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.util.StringUtils;
import com.google.longrunning.Operation;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AwsMetricsPublisher extends AbstractMetricsPublisher {
  private static final Logger logger = Logger.getLogger(AwsMetricsPublisher.class.getName());
  private static AmazonSNSAsync snsClient;

  private String snsTopicOperations;
  private String awsAccessKeyId;
  private String awsSecretKey;
  private int snsClientMaxConnections;

  public AwsMetricsPublisher(MetricsConfig metricsConfig) {
    super(metricsConfig.getClusterId());
    snsTopicOperations = metricsConfig.getAwsMetricsConfig().getOperationsMetricsTopic();
    awsAccessKeyId = metricsConfig.getAwsMetricsConfig().getAwsAccessKeyId();
    awsSecretKey = metricsConfig.getAwsMetricsConfig().getAwsSecretKey();
    snsClientMaxConnections = metricsConfig.getAwsMetricsConfig().getSnsClientMaxConnections();
    if (!StringUtils.isNullOrEmpty(snsTopicOperations)
      && snsClientMaxConnections > 0
      && !StringUtils.isNullOrEmpty(awsAccessKeyId)
      && !StringUtils.isNullOrEmpty(awsSecretKey)) {
      snsClient = initSnsClient();
    }
  }

  public AwsMetricsPublisher() {
    super();
  }

  @Override
  public void publishRequestMetadata(Operation operation, RequestMetadata requestMetadata) {
    try {
      if (snsClient != null) {
        snsClient.publishAsync(new PublishRequest(snsTopicOperations, formatRequestMetadataToJson(populateRequestMetadata(operation, requestMetadata))), new AsyncHandler<PublishRequest, PublishResult>() {
          @Override
          public void onError(Exception e) {
            logger.log(Level.WARNING, "Could not publish metrics data to SNS.", e);
          }
          @Override
          public void onSuccess(PublishRequest request, PublishResult publishResult) { }
        });
      }
    } catch (Exception e) {
      logger.log(Level.WARNING, String.format("Could not publish request metadata to SNS for %s.", operation.getName()), e);
    }
  }

  private AmazonSNSAsync initSnsClient() {
    logger.log(Level.INFO, "Initializing SNS Client.");
    return
      AmazonSNSAsyncClientBuilder.standard()
        .withRegion(Regions.US_EAST_1)
        .withClientConfiguration(
          new ClientConfiguration()
            .withMaxConnections(snsClientMaxConnections))
        .withCredentials(new AWSStaticCredentialsProvider(new AWSCredentials() {
          @Override
          public String getAWSAccessKeyId() {
            return awsAccessKeyId;
          }

          @Override
          public String getAWSSecretKey() {
            return awsSecretKey;
          }
        }))
        .build();
  }

  @Override
  public void publishMetric(String metricName, Object metricValue) {
    throw new UnsupportedOperationException();
  }
}
