// Copyright 2021 The Bazel Authors. All rights reserved.
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

package build.buildfarm.common.s3;

import build.buildfarm.v1test.AwsSecret;
import build.buildfarm.v1test.S3BucketConfig;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.logging.Level;
import lombok.extern.java.Log;

/**
 * @class S3Bucket
 * @brief An AWS S3 bucket abstraction for uploading / downloading blobs.
 * @details This uses configuration to fetch secret information and obtain the S3 bucket. If the
 *     bucket does not already exist remotely, it is created upon construction.
 */
@Log
public class S3Bucket {
  /**
   * @field s3
   * @brief The S3 client used for interacting with S3 buckets
   */
  private final AmazonS3 s3;

  /**
   * @field bucket
   * @brief The established S3 bucket used internally.
   */
  private final Bucket bucket;

  /**
   * @brief Constructor.
   * @details Construct an S3 bucket for uploading / downloading blobs.
   * @param config Configuration information for establishing the bucket.
   */
  S3Bucket(S3BucketConfig config) {
    // Establish client and bucket.
    s3 = createS3Client(config);
    bucket = createBucket(s3, config.getName());
  }

  /**
   * @brief Constructor.
   * @details Construct an S3 bucket with existing bucket and s3 instance.
   * @param s3 An established S3 instance
   * @param bucket An established bucket instance
   */
  S3Bucket(AmazonS3 s3, Bucket bucket) {
    this.s3 = s3;
    this.bucket = bucket;
  }

  /**
   * @brief Upload a blob into the bucket.
   * @details If the the blob already exists at key name, it will be overwritten.
   * @param keyName Key name for storing the blob and looking it up later.
   * @param filePath The file to upload.
   * @return whether the file was uploaded.
   */
  public boolean put(String keyName, String filePath) {
    try {
      s3.putObject(bucket.getName(), keyName, new File(filePath));
    } catch (AmazonServiceException e) {
      log.log(Level.SEVERE, String.format("Blob could not be uploaded: %s", e.getErrorMessage()));
      return false;
    }
    return true;
  }

  /**
   * @brief Download a blob from the bucket.
   * @details If the blob does not exist, no file is created
   * @param keyName Key name to lookup the blob.
   * @param filePath The file to save the blob to.
   */
  public void get(String keyName, String filePath) {
    try {
      S3Object o = s3.getObject(bucket.getName(), keyName);
      S3ObjectInputStream s3is = o.getObjectContent();
      FileOutputStream fos = new FileOutputStream(new File(filePath));
      byte[] read_buf = new byte[1024];
      int read_len;
      while ((read_len = s3is.read(read_buf)) > 0) {
        fos.write(read_buf, 0, read_len);
      }
      s3is.close();
      fos.close();
    } catch (AmazonServiceException e) {
      log.log(
          Level.SEVERE,
          String.format("Service exception while downloading blob: %s", e.getErrorMessage()));
    } catch (FileNotFoundException e) {
      log.log(Level.SEVERE, String.format("Blob does not exist in bucket: %s", e.getMessage()));
    } catch (IOException e) {
      log.log(
          Level.SEVERE,
          String.format("Failure creating file from downloaded blob: %s", e.getMessage()));
    } catch (Exception e) {
      log.log(Level.SEVERE, String.format("Unknown Failure: %s", e.getMessage()));
    }
  }

  /**
   * @brief Establish a valid bucket regadless of whether it already exists in AWS or not.
   * @details The bucket will be created if it does not already exist.
   * @param s3 Client for obtaining bucket.
   * @param bucketName The name of the bucket as seen in AWS.
   * @return an established S3 bucket.
   * @note null is returned if the bucket could not be created.
   */
  private static Bucket createBucket(AmazonS3 s3, String bucketName) {
    // If the bucket already exists, find and return it.
    if (s3.doesBucketExistV2(bucketName)) {
      return getExistingBucket(s3, bucketName);
    }

    // Otherwise create the bucket.
    try {
      return s3.createBucket(bucketName);
    } catch (AmazonS3Exception e) {
      log.log(Level.SEVERE, e.getMessage());
    }

    // The bucket could not be created.
    return null;
  }

  /**
   * @brief Assuming the bucket already exists, find it, and return it.
   * @details If the bucket cannot be found, null is returned.
   * @param s3 Client for obtaining bucket.
   * @param bucketName The name of the bucket as seen in AWS.
   * @return an established S3 bucket.
   */
  private static Bucket getExistingBucket(AmazonS3 s3, String bucketName) {
    // return the bucket found by name
    for (Bucket b : s3.listBuckets()) {
      if (b.getName().equals(bucketName)) {
        return b;
      }
    }

    // The bucket could not be found.
    return null;
  }

  /**
   * @brief Create an AWS S3 client for interacting with the bucket.
   * @details Access information is fetched from a secret manager.
   * @param config Configuration information for establishing the client.
   * @return an established S3 client.
   */
  private static AmazonS3 createS3Client(S3BucketConfig config) {
    // obtain secrets
    AwsSecret secret = getAwsSecret(config.getRegion(), config.getSecretName());

    // create client
    return AmazonS3ClientBuilder.standard()
        .withRegion(config.getRegion())
        .withClientConfiguration(
            new ClientConfiguration().withMaxConnections(config.getMaxConnections()))
        .withCredentials(
            new AWSStaticCredentialsProvider(
                new AWSCredentials() {
                  @Override
                  public String getAWSAccessKeyId() {
                    return secret.getAccessKeyId();
                  }

                  @Override
                  public String getAWSSecretKey() {
                    return secret.getSecretKey();
                  }
                }))
        .build();
  }

  /**
   * @brief Obtain AWS secrets from the secret manager so the client can interact with the bucket.
   * @details If the secrets cannot be obtained, null is returned.
   * @param region The region for the secrets manager.
   * @param secretName The name where the secrets are stored.
   * @return AWS secrets for interacting with the bucket.
   */
  @SuppressWarnings("PMD.StringInstantiation")
  private static AwsSecret getAwsSecret(String region, String secretName) {
    // create secret manager for fetching secrets
    AWSSecretsManager client = AWSSecretsManagerClientBuilder.standard().withRegion(region).build();

    // fetch secrets by name
    GetSecretValueRequest getSecretValueRequest =
        new GetSecretValueRequest().withSecretId(secretName);
    GetSecretValueResult getSecretValueResult = null;
    try {
      getSecretValueResult = client.getSecretValue(getSecretValueRequest);
    } catch (Exception e) {
      log.log(Level.SEVERE, String.format("Could not get secret %s from AWS.", secretName));
    }

    // decode secret
    String secret;
    if (getSecretValueResult.getSecretString() != null) {
      secret = getSecretValueResult.getSecretString();
    } else {
      secret =
          new String(Base64.getDecoder().decode(getSecretValueResult.getSecretBinary()).array());
    }

    // extract access keys
    if (secret != null) {
      try {
        final ObjectMapper objectMapper = new ObjectMapper();
        final HashMap<String, String> secretMap = objectMapper.readValue(secret, HashMap.class);

        final String accessKeyId = secretMap.get("access_key");
        final String secretKey = secretMap.get("secret_key");
        return AwsSecret.newBuilder().setAccessKeyId(accessKeyId).setSecretKey(secretKey).build();
      } catch (IOException e) {
        log.log(Level.SEVERE, String.format("Could not parse secret %s from AWS", secretName));
      }
    }

    return null;
  }
}
