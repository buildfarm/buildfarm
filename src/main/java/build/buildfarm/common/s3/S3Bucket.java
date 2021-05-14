package aws.example.s3;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;

import java.util.List;

/**
 * Create an Amazon S3 bucket.
 * 
 * Aws credentials are fetched via the secret's manager
 * Follows example in creating bucket:
 * https://github.com/awsdocs/aws-doc-sdk-examples/blob/cd17c6e47381d078c00c409d6630d36a547c2c10/java/example_code/s3/src/main/java/aws/example/s3/CreateBucket.java
 */
public class S3Bucket {
    
    final AmazonS3 s3;
    
    S3Bucket(String region){
        
        s3 = createS3Client(region);
    }

    public Bucket createBucket(String bucket_name, String region) {
        //final AmazonS3 s3 = createS3Client(region);
        Bucket b = null;
        if (s3.doesBucketExistV2(bucket_name)) {
            System.out.format("Bucket %s already exists.\n", bucket_name);
            b = getBucket(bucket_name,region);
        } else {
            try {
                b = s3.createBucket(bucket_name);
            } catch (AmazonS3Exception e) {
                System.err.println(e.getErrorMessage());
            }
        }
        return b;
    }
    
    private Bucket getBucket(String bucket_name, String region) {
        //final AmazonS3 s3 = createS3Client(region);
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
}