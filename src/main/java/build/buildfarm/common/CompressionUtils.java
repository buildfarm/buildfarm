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

import com.github.luben.zstd.Zstd;
import java.util.Base64;

/**
 * @class CompressionUtils
 * @brief Utilities for compressing data
 * @details These utilities are used to convert between lossless compression formats
 */
public class CompressionUtils {
  /**
   * @brief Compress a string with zstandard compression.
   * @details Usually done with byte buffers. Not for performance, but testing underlying
   *     implementation.
   * @param data The string to compress
   * @return The string in compressed format.
   * @note Suggested return identifier: compressedData
   */
  public static String zstdCompress(String data) {
    byte[] compressed = Zstd.compress(data.getBytes());
    return Base64.getEncoder().encodeToString(compressed);
  }

  /**
   * @brief Decompress a zstandard compressed string.
   * @details Usually done with byte buffers. Not for performance, but testing underlying
   *     implementation.
   * @param data The string to decompress
   * @return The string in decompressed format.
   * @note Suggested return identifier: data
   */
  public static String zstdDecompress(String compressedData) {
    byte[] src = Base64.getDecoder().decode(compressedData);
    int size = (int) Zstd.decompressedSize(src);
    byte[] data = Zstd.decompress(src, size);
    return new String(data);
  }
}
