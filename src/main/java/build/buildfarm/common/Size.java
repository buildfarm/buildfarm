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

package build.buildfarm.common;

/**
 * @class Size
 * @brief Utilities related to file sizes.
 * @details Contains converters between different time units.
 */
public class Size {
  /**
   * @brief Kb to bytes.
   * @details Kb to bytes.
   * @param sizeKb Size in KB to convert.
   * @return The number of bytes converted from KB.
   * @note Suggested return identifier: bytes.
   */
  public static long kbToBytes(long sizeKb) {
    return sizeKb * 1024;
  }

  /**
   * @brief Bytes to kb.
   * @details Bytes to kb.
   * @param bytes Size in bytes to convert.
   * @return Size in KB converted from bytes.
   * @note Suggested return identifier: sizeKb.
   */
  public static long bytesToKb(long bytes) {
    return bytes / 1024;
  }

  /**
   * @brief Mb to bytes.
   * @details Mb to bytes.
   * @param sizeMb Size in MB to convert.
   * @return The number of bytes converted from MB.
   * @note Suggested return identifier: bytes.
   */
  public static long mbToBytes(long sizeMb) {
    return sizeMb * 1024 * 1024;
  }

  /**
   * @brief Bytes to mb.
   * @details Bytes to mb.
   * @param bytes Size in bytes to convert.
   * @return Size in MB converted from bytes.
   * @note Suggested return identifier: sizeMb.
   */
  public static long bytesToMb(long bytes) {
    return bytes / 1024 / 1024;
  }

  /**
   * @brief Gb to bytes.
   * @details Gb to bytes.
   * @param sizeGb Size in GB to convert.
   * @return The number of bytes converted from GB.
   * @note Suggested return identifier: bytes.
   */
  public static long gbToBytes(long sizeGb) {
    return sizeGb * 1024 * 1024 * 1024;
  }

  /**
   * @brief Bytes to gb.
   * @details Bytes to gb.
   * @param bytes Size in bytes to convert.
   * @return Size in GB converted from bytes.
   * @note Suggested return identifier: sizeGb.
   */
  public static long bytesToGb(long bytes) {
    return bytes / 1024 / 1024 / 1024;
  }
}
