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
/// @class   CpuLimits
/// @brief   CPU resource limitations imposed on specific actions.
/// @details These resource limitations are often specified by the client
///          (via: exec_properties), but ultimately validated and decided by
///          the server. Restricting core count can be beneficial to
///          preventing hungry actions from bogging down the worker and
///          affecting neighboring actions that may be sharing the same
///          hardware. More importantly, being able to apply restrictions
///          allows for a more efficient utilization of shared compute across
///          different workers and action profiles. One might also consider
///          even using higher core machine to take on highly parallel
///          actions, while allowing lower core machines to take on single
///          threaded actions. These restrictions will ultimately encourage
///          action writers to implement their actions more efficiently or opt
///          for local execution as an alternative.
///
public class CpuLimits {

  ///
  /// @field   limit
  /// @brief   Whether or not we perform CPU core limiting on the action.
  /// @details Depending on the server implementation, we may skip applying any
  ///          restrictions to core usage.
  ///
  public boolean limit;

  ///
  /// @field   min
  /// @brief   The minimum CPU cores required.
  /// @details Client can suggest this though exec_properties.
  ///
  public int min;

  ///
  /// @field   max
  /// @brief   The maximum CPU cores required.
  /// @details Client can suggest this though exec_properties.
  ///
  public int max;
}
