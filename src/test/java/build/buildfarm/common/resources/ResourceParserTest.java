// Copyright 2022 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.common.resources;

import static com.google.common.truth.Truth.assertThat;

import build.bazel.remote.execution.v2.Compressor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ResourceParserTest {
  // Function under test: constructor
  // Reason for testing: code coverage
  // Failure explanation: Although static methods are generally used, this tests that the class can
  // be constructed.  Without it, the test coverage shows incompleteness.
  @Test
  @SuppressWarnings("unused")
  public void DefaultConstruction() {
    // ARRANGE
    ResourceParser parser = new ResourceParser();
  }

  // Function under test: getResourceType, parseUploadBlobRequest
  // Reason for testing: valid URI example for upload blob requests
  // Failure explanation: parser failed to identify URI correctly
  @Test
  public void IdentifyUploadBlobRequestExample1() {
    // ARRANGE
    // without instance name
    // without metadata
    String resourceName =
        "uploads/00000000-0000-0000-0000-000000000000/blobs/0000000000000000000000000000000000000000000000000000000000000000/123";

    // ACT
    Resource.TypeCase type = ResourceParser.getResourceType(resourceName);

    // ASSERT
    assertThat(type).isEqualTo(Resource.TypeCase.UPLOAD_BLOB_REQUEST);

    // ACT
    UploadBlobRequest request = ResourceParser.parseUploadBlobRequest(resourceName);

    // ASSERT
    assertThat(request.getInstanceName()).isEqualTo("");
    assertThat(request.getUuid()).isEqualTo("00000000-0000-0000-0000-000000000000");
    assertThat(request.getBlob().getCompressor()).isEqualTo(Compressor.Value.IDENTITY);
    assertThat(request.getBlob().getDigest().getHash())
        .isEqualTo("0000000000000000000000000000000000000000000000000000000000000000");
    assertThat(request.getBlob().getDigest().getSize()).isEqualTo(123);
    assertThat(request.getMetadata()).isEqualTo("");
  }

  // Function under test: getResourceType, parseUploadBlobRequest
  // Reason for testing: valid URI example for upload blob requests
  // Failure explanation: parser failed to identify URI correctly
  @Test
  public void IdentifyUploadBlobRequestExample2() {
    // ARRANGE
    // with instance name
    // without metadata
    String resourceName =
        "instance_name/uploads/00000000-0000-0000-0000-000000000000/blobs/0000000000000000000000000000000000000000000000000000000000000000/123";

    // ACT
    Resource.TypeCase type = ResourceParser.getResourceType(resourceName);

    // ASSERT
    assertThat(type).isEqualTo(Resource.TypeCase.UPLOAD_BLOB_REQUEST);

    // ACT
    UploadBlobRequest request = ResourceParser.parseUploadBlobRequest(resourceName);

    // ASSERT
    assertThat(request.getInstanceName()).isEqualTo("instance_name");
    assertThat(request.getUuid()).isEqualTo("00000000-0000-0000-0000-000000000000");
    assertThat(request.getBlob().getCompressor()).isEqualTo(Compressor.Value.IDENTITY);
    assertThat(request.getBlob().getDigest().getHash())
        .isEqualTo("0000000000000000000000000000000000000000000000000000000000000000");
    assertThat(request.getBlob().getDigest().getSize()).isEqualTo(123);
    assertThat(request.getMetadata()).isEqualTo("");
  }

  // Function under test: getResourceType, parseUploadBlobRequest
  // Reason for testing: valid URI example for upload blob requests
  // Failure explanation: parser failed to identify URI correctly
  @Test
  public void IdentifyUploadBlobRequestExample3() {
    // ARRANGE
    // without instance name
    // with metadata
    String resourceName =
        "uploads/00000000-0000-0000-0000-000000000000/blobs/0000000000000000000000000000000000000000000000000000000000000000/123/metadata";

    // ACT
    Resource.TypeCase type = ResourceParser.getResourceType(resourceName);

    // ASSERT
    assertThat(type).isEqualTo(Resource.TypeCase.UPLOAD_BLOB_REQUEST);

    // ACT
    UploadBlobRequest request = ResourceParser.parseUploadBlobRequest(resourceName);

    // ASSERT
    assertThat(request.getInstanceName()).isEqualTo("");
    assertThat(request.getUuid()).isEqualTo("00000000-0000-0000-0000-000000000000");
    assertThat(request.getBlob().getCompressor()).isEqualTo(Compressor.Value.IDENTITY);
    assertThat(request.getBlob().getDigest().getHash())
        .isEqualTo("0000000000000000000000000000000000000000000000000000000000000000");
    assertThat(request.getBlob().getDigest().getSize()).isEqualTo(123);
    assertThat(request.getMetadata()).isEqualTo("metadata");
  }

  // Function under test: getResourceType, parseUploadBlobRequest
  // Reason for testing: valid URI example for upload blob requests
  // Failure explanation: parser failed to identify URI correctly
  @Test
  public void IdentifyUploadBlobRequestExample4() {
    // ARRANGE
    // with instance name
    // with metadata
    String resourceName =
        "instance_name/uploads/00000000-0000-0000-0000-000000000000/blobs/0000000000000000000000000000000000000000000000000000000000000000/123/metadata";

    // ACT
    Resource.TypeCase type = ResourceParser.getResourceType(resourceName);

    // ASSERT
    assertThat(type).isEqualTo(Resource.TypeCase.UPLOAD_BLOB_REQUEST);

    // ACT
    UploadBlobRequest request = ResourceParser.parseUploadBlobRequest(resourceName);

    // ASSERT
    assertThat(request.getInstanceName()).isEqualTo("instance_name");
    assertThat(request.getUuid()).isEqualTo("00000000-0000-0000-0000-000000000000");
    assertThat(request.getBlob().getCompressor()).isEqualTo(Compressor.Value.IDENTITY);
    assertThat(request.getBlob().getDigest().getHash())
        .isEqualTo("0000000000000000000000000000000000000000000000000000000000000000");
    assertThat(request.getBlob().getDigest().getSize()).isEqualTo(123);
    assertThat(request.getMetadata()).isEqualTo("metadata");
  }

  // Function under test: getResourceType, parseUploadBlobRequest
  // Reason for testing: valid URI example for upload blob requests
  // Failure explanation: parser failed to identify URI correctly
  @Test
  public void IdentifyUploadBlobRequestExample5() {
    // ARRANGE
    // multipath instance name
    String resourceName =
        "this/instance/name/is/multipart/uploads/00000000-0000-0000-0000-000000000000/blobs/0000000000000000000000000000000000000000000000000000000000000000/123";

    // ACT
    Resource.TypeCase type = ResourceParser.getResourceType(resourceName);

    // ASSERT
    assertThat(type).isEqualTo(Resource.TypeCase.UPLOAD_BLOB_REQUEST);

    // ACT
    UploadBlobRequest request = ResourceParser.parseUploadBlobRequest(resourceName);

    // ASSERT
    assertThat(request.getInstanceName()).isEqualTo("this/instance/name/is/multipart");
    assertThat(request.getUuid()).isEqualTo("00000000-0000-0000-0000-000000000000");
    assertThat(request.getBlob().getCompressor()).isEqualTo(Compressor.Value.IDENTITY);
    assertThat(request.getBlob().getDigest().getHash())
        .isEqualTo("0000000000000000000000000000000000000000000000000000000000000000");
    assertThat(request.getBlob().getDigest().getSize()).isEqualTo(123);
    assertThat(request.getMetadata()).isEqualTo("");
  }

  // Function under test: getResourceType, parseUploadBlobRequest
  // Reason for testing: valid URI example for upload blob requests
  // Failure explanation: parser failed to identify URI correctly
  @Test
  public void IdentifyUploadBlobRequestExample6() {
    // ARRANGE
    // multipath metadata
    String resourceName =
        "uploads/00000000-0000-0000-0000-000000000000/blobs/0000000000000000000000000000000000000000000000000000000000000000/123/this/metadata/is/multipart.cc";

    // ACT
    Resource.TypeCase type = ResourceParser.getResourceType(resourceName);

    // ASSERT
    assertThat(type).isEqualTo(Resource.TypeCase.UPLOAD_BLOB_REQUEST);

    // ACT
    UploadBlobRequest request = ResourceParser.parseUploadBlobRequest(resourceName);

    // ASSERT
    assertThat(request.getInstanceName()).isEqualTo("");
    assertThat(request.getUuid()).isEqualTo("00000000-0000-0000-0000-000000000000");
    assertThat(request.getBlob().getCompressor()).isEqualTo(Compressor.Value.IDENTITY);
    assertThat(request.getBlob().getDigest().getHash())
        .isEqualTo("0000000000000000000000000000000000000000000000000000000000000000");
    assertThat(request.getBlob().getDigest().getSize()).isEqualTo(123);
    assertThat(request.getMetadata()).isEqualTo("this/metadata/is/multipart.cc");
  }

  // Function under test: getResourceType, parseUploadBlobRequest
  // Reason for testing: valid URI example for upload blob requests
  // Failure explanation: parser failed to identify URI correctly
  @Test
  public void IdentifyUploadBlobRequestExample7() {
    // ARRANGE
    // multipath instance name
    // multipath metadata
    String resourceName =
        "this/instance/name/is/multipart/uploads/00000000-0000-0000-0000-000000000000/blobs/0000000000000000000000000000000000000000000000000000000000000000/123/this/metadata/is/multipart.cc";

    // ACT
    Resource.TypeCase type = ResourceParser.getResourceType(resourceName);

    // ASSERT
    assertThat(type).isEqualTo(Resource.TypeCase.UPLOAD_BLOB_REQUEST);

    // ACT
    UploadBlobRequest request = ResourceParser.parseUploadBlobRequest(resourceName);

    // ASSERT
    assertThat(request.getInstanceName()).isEqualTo("this/instance/name/is/multipart");
    assertThat(request.getUuid()).isEqualTo("00000000-0000-0000-0000-000000000000");
    assertThat(request.getBlob().getCompressor()).isEqualTo(Compressor.Value.IDENTITY);
    assertThat(request.getBlob().getDigest().getHash())
        .isEqualTo("0000000000000000000000000000000000000000000000000000000000000000");
    assertThat(request.getBlob().getDigest().getSize()).isEqualTo(123);
    assertThat(request.getMetadata()).isEqualTo("this/metadata/is/multipart.cc");
  }

  // Function under test: getResourceType, parseUploadBlobRequest
  // Reason for testing: valid URI example for upload blob requests
  // Failure explanation: parser failed to identify URI correctly
  @Test
  public void IdentifyUploadBlobRequestExample8() {
    // ARRANGE
    // compression example
    String resourceName =
        "this/instance/name/is/multipart/uploads/00000000-0000-0000-0000-000000000000/compressed-blobs/zstd/0000000000000000000000000000000000000000000000000000000000000000/123/this/metadata/is/multipart.cc";

    // ACT
    Resource.TypeCase type = ResourceParser.getResourceType(resourceName);

    // ASSERT
    assertThat(type).isEqualTo(Resource.TypeCase.UPLOAD_BLOB_REQUEST);

    // ACT
    UploadBlobRequest request = ResourceParser.parseUploadBlobRequest(resourceName);

    // ASSERT
    assertThat(request.getInstanceName()).isEqualTo("this/instance/name/is/multipart");
    assertThat(request.getUuid()).isEqualTo("00000000-0000-0000-0000-000000000000");
    assertThat(request.getBlob().getCompressor()).isEqualTo(Compressor.Value.ZSTD);
    assertThat(request.getBlob().getDigest().getHash())
        .isEqualTo("0000000000000000000000000000000000000000000000000000000000000000");
    assertThat(request.getBlob().getDigest().getSize()).isEqualTo(123);
    assertThat(request.getMetadata()).isEqualTo("this/metadata/is/multipart.cc");
  }

  // Function under test: getResourceType, parseUploadBlobRequest
  // Reason for testing: valid URI example for upload blob requests
  // Failure explanation: parser failed to identify URI correctly
  @Test
  public void IdentifyUploadBlobRequestExample9() {
    // ARRANGE
    // compression example (still no compression)
    String resourceName =
        "this/instance/name/is/multipart/uploads/00000000-0000-0000-0000-000000000000/compressed-blobs/identity/0000000000000000000000000000000000000000000000000000000000000000/123/this/metadata/is/multipart.cc";

    // ACT
    Resource.TypeCase type = ResourceParser.getResourceType(resourceName);

    // ASSERT
    assertThat(type).isEqualTo(Resource.TypeCase.UPLOAD_BLOB_REQUEST);

    // ACT
    UploadBlobRequest request = ResourceParser.parseUploadBlobRequest(resourceName);

    // ASSERT
    assertThat(request.getInstanceName()).isEqualTo("this/instance/name/is/multipart");
    assertThat(request.getUuid()).isEqualTo("00000000-0000-0000-0000-000000000000");
    assertThat(request.getBlob().getCompressor()).isEqualTo(Compressor.Value.IDENTITY);
    assertThat(request.getBlob().getDigest().getHash())
        .isEqualTo("0000000000000000000000000000000000000000000000000000000000000000");
    assertThat(request.getBlob().getDigest().getSize()).isEqualTo(123);
    assertThat(request.getMetadata()).isEqualTo("this/metadata/is/multipart.cc");
  }

  // Function under test: getResourceType, parseUploadBlobRequest
  // Reason for testing: valid URI example for upload blob requests
  // Failure explanation: parser failed to identify URI correctly
  @Test
  public void IdentifyUploadBlobRequestExample10() {
    // ARRANGE
    // metadata might contain keywords
    String resourceName =
        "uploads/00000000-0000-0000-0000-000000000000/compressed-blobs/identity/0000000000000000000000000000000000000000000000000000000000000000/123/this/has/keywords/blobs/compressed-blobs/operations";

    // ACT
    Resource.TypeCase type = ResourceParser.getResourceType(resourceName);

    // ASSERT
    assertThat(type).isEqualTo(Resource.TypeCase.UPLOAD_BLOB_REQUEST);

    // ACT
    UploadBlobRequest request = ResourceParser.parseUploadBlobRequest(resourceName);

    // ASSERT
    assertThat(request.getInstanceName()).isEqualTo("");
    assertThat(request.getUuid()).isEqualTo("00000000-0000-0000-0000-000000000000");
    assertThat(request.getBlob().getCompressor()).isEqualTo(Compressor.Value.IDENTITY);
    assertThat(request.getBlob().getDigest().getHash())
        .isEqualTo("0000000000000000000000000000000000000000000000000000000000000000");
    assertThat(request.getBlob().getDigest().getSize()).isEqualTo(123);
    assertThat(request.getMetadata())
        .isEqualTo("this/has/keywords/blobs/compressed-blobs/operations");
  }

  // Function under test: getResourceType, parseDownloadBlobRequest
  // Reason for testing: valid URI example for download blob requests
  // Failure explanation: parser failed to identify URI correctly
  @Test
  public void IdentifyDownloadBlobRequestExample1() {
    // ARRANGE
    // no instance name
    String resourceName =
        "blobs/0000000000000000000000000000000000000000000000000000000000000000/123";

    // ACT
    Resource.TypeCase type = ResourceParser.getResourceType(resourceName);

    // ASSERT
    assertThat(type).isEqualTo(Resource.TypeCase.DOWNLOAD_BLOB_REQUEST);

    // ACT
    DownloadBlobRequest request = ResourceParser.parseDownloadBlobRequest(resourceName);

    // ASSERT
    assertThat(request.getInstanceName()).isEqualTo("");
    assertThat(request.getBlob().getCompressor()).isEqualTo(Compressor.Value.IDENTITY);
    assertThat(request.getBlob().getDigest().getHash())
        .isEqualTo("0000000000000000000000000000000000000000000000000000000000000000");
    assertThat(request.getBlob().getDigest().getSize()).isEqualTo(123);
  }

  // Function under test: getResourceType, parseDownloadBlobRequest
  // Reason for testing: valid URI example for download blob requests
  // Failure explanation: parser failed to identify URI correctly
  @Test
  public void IdentifyDownloadBlobRequestExample2() {
    // ARRANGE
    // with instance name
    String resourceName =
        "instance_name/blobs/0000000000000000000000000000000000000000000000000000000000000000/123";

    // ACT
    Resource.TypeCase type = ResourceParser.getResourceType(resourceName);

    // ASSERT
    assertThat(type).isEqualTo(Resource.TypeCase.DOWNLOAD_BLOB_REQUEST);

    // ACT
    DownloadBlobRequest request = ResourceParser.parseDownloadBlobRequest(resourceName);

    // ASSERT
    assertThat(request.getInstanceName()).isEqualTo("instance_name");
    assertThat(request.getBlob().getCompressor()).isEqualTo(Compressor.Value.IDENTITY);
    assertThat(request.getBlob().getDigest().getHash())
        .isEqualTo("0000000000000000000000000000000000000000000000000000000000000000");
    assertThat(request.getBlob().getDigest().getSize()).isEqualTo(123);
  }

  // Function under test: getResourceType, parseDownloadBlobRequest
  // Reason for testing: valid URI example for download blob requests
  // Failure explanation: parser failed to identify URI correctly
  @Test
  public void IdentifyDownloadBlobRequestExample3() {
    // ARRANGE
    // with multi-segment instance name
    String resourceName =
        "multi-segment/instance/name/blobs/0000000000000000000000000000000000000000000000000000000000000000/123";

    // ACT
    Resource.TypeCase type = ResourceParser.getResourceType(resourceName);

    // ASSERT
    assertThat(type).isEqualTo(Resource.TypeCase.DOWNLOAD_BLOB_REQUEST);

    // ACT
    DownloadBlobRequest request = ResourceParser.parseDownloadBlobRequest(resourceName);

    // ASSERT
    assertThat(request.getInstanceName()).isEqualTo("multi-segment/instance/name");
    assertThat(request.getBlob().getCompressor()).isEqualTo(Compressor.Value.IDENTITY);
    assertThat(request.getBlob().getDigest().getHash())
        .isEqualTo("0000000000000000000000000000000000000000000000000000000000000000");
    assertThat(request.getBlob().getDigest().getSize()).isEqualTo(123);
  }

  // Function under test: getResourceType, parseDownloadBlobRequest
  // Reason for testing: valid URI example for download blob requests
  // Failure explanation: parser failed to identify URI correctly
  @Test
  public void IdentifyDownloadBlobRequestExample4() {
    // ARRANGE
    // compression example
    String resourceName =
        "multi-segment/instance/name/compressed-blobs/zstd/0000000000000000000000000000000000000000000000000000000000000000/123";

    // ACT
    Resource.TypeCase type = ResourceParser.getResourceType(resourceName);

    // ASSERT
    assertThat(type).isEqualTo(Resource.TypeCase.DOWNLOAD_BLOB_REQUEST);

    // ACT
    DownloadBlobRequest request = ResourceParser.parseDownloadBlobRequest(resourceName);

    // ASSERT
    assertThat(request.getInstanceName()).isEqualTo("multi-segment/instance/name");
    assertThat(request.getBlob().getCompressor()).isEqualTo(Compressor.Value.ZSTD);
    assertThat(request.getBlob().getDigest().getHash())
        .isEqualTo("0000000000000000000000000000000000000000000000000000000000000000");
    assertThat(request.getBlob().getDigest().getSize()).isEqualTo(123);
  }

  // Function under test: getResourceType, parseDownloadBlobRequest
  // Reason for testing: valid URI example for download blob requests
  // Failure explanation: parser failed to identify URI correctly
  @Test
  public void IdentifyDownloadBlobRequestExample5() {
    // ARRANGE
    // compression example (still no compression)
    String resourceName =
        "multi-segment/instance/name/compressed-blobs/identity/0000000000000000000000000000000000000000000000000000000000000000/123";

    // ACT
    Resource.TypeCase type = ResourceParser.getResourceType(resourceName);

    // ASSERT
    assertThat(type).isEqualTo(Resource.TypeCase.DOWNLOAD_BLOB_REQUEST);

    // ACT
    DownloadBlobRequest request = ResourceParser.parseDownloadBlobRequest(resourceName);

    // ASSERT
    assertThat(request.getInstanceName()).isEqualTo("multi-segment/instance/name");
    assertThat(request.getBlob().getCompressor()).isEqualTo(Compressor.Value.IDENTITY);
    assertThat(request.getBlob().getDigest().getHash())
        .isEqualTo("0000000000000000000000000000000000000000000000000000000000000000");
    assertThat(request.getBlob().getDigest().getSize()).isEqualTo(123);
  }
}
