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
/// @class   MemLimits
/// @brief   Memory resource limitations imposed on specific actions.
/// @details These resource limitations are often specified by the client
///          (via: exec_properties), but ultimately validated and decided by
///          the server. Restricting RAM usage can be beneficial in preventing
///          hungry actions from bogging down the worker and affecting
///          neighboring actions that may be sharing the same hardware.
///
public class MemLimits {

  ///
  /// @field   limit
  /// @brief   Whether or not we perform memory limiting on the action.
  /// @details Depending on the server implementation, we may skip applying any
  ///          restrictions on memory usage.
  ///
  public boolean limit;

  ///
  /// @field   min_gb
  /// @brief   The minimum number of RAM required.
  /// @details Client can suggest this though exec_properties.
  /// @note    units: gigabyte
  ///
  public int min_gb;

  ///
  /// @field   max_gb
  /// @brief   The maximum RAM required.
  /// @details Client can suggest this though exec_properties.
  /// @note    units: gigabyte
  ///
  public int max_gb;
}
