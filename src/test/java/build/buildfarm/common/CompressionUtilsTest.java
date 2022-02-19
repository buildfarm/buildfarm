// Copyright 2022 The Bazel Authors. All rights reserved.
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

package build.buildfarm.common;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import java.nio.charset.StandardCharsets;

/**
 * @class CompressionUtilsTest
 * @brief tests compression utility functions.
 */
@RunWith(JUnit4.class)
public class CompressionUtilsTest {
  // Function under test: zstdCompress
  // Reason for testing: compression creates new data
  // Failure explanation: compression either failed or did not change data.
  @Test
  public void compressionCreatesNewData() throws Exception {
    // ARRANGE
    String data = "Hello World";

    // ACT
    String result = CompressionUtils.zstdCompress(data);

    // ASSERT
    assertThat(data).isNotEqualTo(result);
  }

  // Function under test: zstdCompress, zstdDecompress
  // Reason for testing: compression to decompression is idempotent.
  // Failure explanation: compression & decompression are not acting idempotent.
  @Test
  public void compressionDecompressionIdempotent() throws Exception {
    // ARRANGE
    String data = "Hello World";

    // ACT
    String result = CompressionUtils.zstdDecompress(CompressionUtils.zstdCompress(data));

    // ASSERT
    assertThat(data).isEqualTo(result);
  }

  // Function under test: zstdCompress, zstdDecompress
  // Reason for testing: empty string can be compressed & uncompressed.
  // Failure explanation: empty string is failure edge case.
  @Test
  public void emptyStringIdempotent() throws Exception {
    // ARRANGE
    String data = "";

    // ACT
    String result = CompressionUtils.zstdDecompress(CompressionUtils.zstdCompress(data));

    // ASSERT
    assertThat(data).isEqualTo(result);
  }
  
  // Function under test: zstdCompress
  // Reason for testing: Test command line compatibility.
  // Failure explanation: The results are not the same as running standard CLI tools.
  @Test
  public void cliCompatibility() throws Exception {
    // ARRANGE
    String data = "a";

    // ACT
    // echo -n "Hello World" | zstd --stdout | base64
    String result = CompressionUtils.zstdCompress(data);

    // ASSERT
    assertThat(result).isEqualTo("KLUv/QRYWQAASGVsbG8gV29ybGTCWyQZ");
  }
}
