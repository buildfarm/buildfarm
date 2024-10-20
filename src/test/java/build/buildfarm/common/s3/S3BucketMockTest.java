// Copyright 2020 The Buildfarm Authors. All rights reserved.
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

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import java.io.File;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * @class S3BucketMockTest
 * @brief An AWS S3 bucket abstraction for uploading / downloading blobs.
 * @details This uses configuration to fetch secret information and obtain the S3 bucket. If the
 *     bucket does not already exist remotely, it is created upon construction.
 */
@RunWith(JUnit4.class)
public class S3BucketMockTest {
  // Function under test: put
  // Reason for testing: test that put succeeds.
  // Failure explanation: put should return success.
  @Test
  public void push() throws Exception {
    // MOCK
    AmazonS3 s3 = mock(AmazonS3.class);
    Bucket bucket = mock(Bucket.class);

    // ARRANGE
    S3Bucket s3Bucket = new S3Bucket(s3, bucket);

    // ACT
    boolean success = s3Bucket.put("key", "file_path");

    // ASSERT
    assertThat(success).isTrue();
  }

  // Function under test: put
  // Reason for testing: test that put impl gets called
  // Failure explanation: Implementation not as expected.
  @Test
  public void putCalled() throws Exception {
    // MOCK
    AmazonS3 s3 = mock(AmazonS3.class);
    Bucket bucket = mock(Bucket.class);

    // ARRANGE
    S3Bucket s3Bucket = new S3Bucket(s3, bucket);

    // ACT
    s3Bucket.put("key", "file_path");

    // ASSERT
    verify(s3, times(1)).putObject(null, "key", new File("file_path"));
  }

  // Function under test: get
  // Reason for testing: test that get impl gets called
  // Failure explanation: Implementation not as expected.
  @Test
  public void getCalled() throws Exception {
    // MOCK
    AmazonS3 s3 = mock(AmazonS3.class);
    Bucket bucket = mock(Bucket.class);

    // ARRANGE
    S3Bucket s3Bucket = new S3Bucket(s3, bucket);

    // ACT
    s3Bucket.get("key", "file_path");

    // ASSERT
    verify(s3, times(1)).getObject(null, "key");
  }
}
