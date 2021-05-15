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
import java.util.List;

/**
 * Create an Amazon S3 bucket.
 *
 * <p>Aws credentials are fetched via the secret's manager Follows example in creating bucket:
 * https://github.com/awsdocs/aws-doc-sdk-examples/blob/cd17c6e47381d078c00c409d6630d36a547c2c10/java/example_code/s3/src/main/java/aws/example/s3/CreateBucket.java
 */
public class S3Bucket {

  final AmazonS3 s3;
  final Bucket bucket;

  S3Bucket(String region) {

    s3 = createS3Client(region);
    bucket = createBucket("name", region);
  }

  private Bucket createBucket(String bucket_name, String region) {
    Bucket b = null;
    if (s3.doesBucketExistV2(bucket_name)) {
      b = getExistingBucket(bucket_name, region);
    } else {
      try {
        b = s3.createBucket(bucket_name);
      } catch (AmazonS3Exception e) {
        System.err.println(e.getErrorMessage());
      }
    }
    return b;
  }

  private Bucket getExistingBucket(String bucket_name, String region) {
    Bucket named_bucket = null;
    List<Bucket> buckets = s3.listBuckets();
    for (Bucket b : buckets) {
      if (b.getName().equals(bucket_name)) {
        named_bucket = b;
      }
    }
    return named_bucket;
  }

  private static AmazonS3 createS3Client(String region) {
    final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();
    return s3;
  }

  private AwsSecret getAwsSecret(String region, String secretName) {
    AWSSecretsManager client = AWSSecretsManagerClientBuilder.standard().withRegion(region).build();
    GetSecretValueRequest getSecretValueRequest =
        new GetSecretValueRequest().withSecretId(secretName);
    GetSecretValueResult getSecretValueResult = null;
    try {
      getSecretValueResult = client.getSecretValue(getSecretValueRequest);
    } catch (Exception e) {
      // logger.log(Level.SEVERE, String.format("Could not get secret %s from AWS.", secretName));
    }
    String secret = null;
    if (getSecretValueResult.getSecretString() != null) {
      secret = getSecretValueResult.getSecretString();
    } else {
      secret =
          new String(Base64.getDecoder().decode(getSecretValueResult.getSecretBinary()).array());
    }

    AwsSecret secretResult = AwsSecret.newBuilder().build();
    if (secret != null) {
      try {
        final ObjectMapper objectMapper = new ObjectMapper();
        final HashMap<String, String> secretMap = objectMapper.readValue(secret, HashMap.class);
        final String accessKeyId = secretMap.get("access_key");
        final String secretKey = secretMap.get("secret_key");
      } catch (IOException e) {
        // logger.log(Level.SEVERE, String.format("Could not parse secret %s from AWS",
        // secretName));
      }
    }

    return null;
  }
}

