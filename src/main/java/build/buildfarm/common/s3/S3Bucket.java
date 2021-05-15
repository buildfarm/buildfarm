package build.buildfarm.common;

import build.buildfarm.v1test.AwsSecret;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Create an Amazon S3 bucket.
 *
 * <p>Aws credentials are fetched via the secret's manager Follows example in creating bucket:
 * https://github.com/awsdocs/aws-doc-sdk-examples/blob/cd17c6e47381d078c00c409d6630d36a547c2c10/java/example_code/s3/src/main/java/aws/example/s3/CreateBucket.java
 */
public class S3Bucket {

  private static final Logger logger = Logger.getLogger(S3Bucket.class.getName());
  private AmazonS3 s3;
  private Bucket bucket;

  S3Bucket(String region) {

    loadState(region);
  }

  private void loadState(String region) {

    // the S3 client for interacting with the bucket
    s3 = createS3Client(region);

    // the aws bucket object
    bucket = createBucket("name", region);
  }

  private Bucket createBucket(String bucket_name, String region) {

    // If the bucket already exists, find and return it.
    if (s3.doesBucketExistV2(bucket_name)) {
      Bucket b = getExistingBucket(bucket_name, region);
      return b;
    }

    // Otherwise create the bucket.
    try {
      Bucket b = s3.createBucket(bucket_name);
      return b;
    } catch (AmazonS3Exception e) {
      System.err.println(e.getErrorMessage());
    }

    // The bucket could not be created.
    return null;
  }

  private Bucket getExistingBucket(String bucket_name, String region) {

    // return the bucket found by name
    for (Bucket b : s3.listBuckets()) {
      if (b.getName().equals(bucket_name)) {
        return b;
      }
    }

    // The bucket could not be found.
    return null;
  }

  private static AmazonS3 createS3Client(String region) {
    final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();
    return s3;
  }

  private AwsSecret getAwsSecret(String region, String secretName) {

    // create secret manager for fetching secrets
    AWSSecretsManager client = AWSSecretsManagerClientBuilder.standard().withRegion(region).build();

    // fetch secrets by name
    GetSecretValueRequest getSecretValueRequest =
        new GetSecretValueRequest().withSecretId(secretName);
    GetSecretValueResult getSecretValueResult = null;
    try {
      getSecretValueResult = client.getSecretValue(getSecretValueRequest);
    } catch (Exception e) {
      logger.log(Level.SEVERE, String.format("Could not get secret %s from AWS.", secretName));
    }

    // decode secret
    String secret = null;
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
        AwsSecret secretResult =
            AwsSecret.newBuilder().setAccessKeyId(accessKeyId).setSecretKey(secretKey).build();
        return secretResult;
      } catch (IOException e) {
        logger.log(Level.SEVERE, String.format("Could not parse secret %s from AWS", secretName));
      }
    }

    return null;
  }
}
