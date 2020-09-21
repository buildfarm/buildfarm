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

package build.buildfarm.worker;

///
/// @class   DiskLimits
/// @brief   Disk resource limitations imposed on specific actions.
/// @details These resource limitations are often specified by the client
///          (via: exec_properties), but ultimately validated and decided by
///          the server. Restricting disk space can be beneficial in
///          preventing actions from using too much disk space and affecting
///          the performance of the worker and neighboring actions.
///
public class DiskLimits {

  ///
  /// @field   limit
  /// @brief   Whether or not we perform disk limiting on the action.
  /// @details Depending on the server implementation, we may skip applying any
  ///          restrictions on disk usage.
  ///
  public boolean limit;

  ///
  /// @field   min_mb
  /// @brief   The minimum amount of disk space required.
  /// @details Client can suggest this though exec_properties.
  /// @note    units: megabyte
  ///
  public int min_mb;

  ///
  /// @field   max_mb
  /// @brief   The maximum amount of disk space required.
  /// @details Client can suggest this though exec_properties.
  /// @note    units: megabyte
  ///
  public int max_mb;
}
