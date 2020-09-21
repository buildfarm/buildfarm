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
/// @class   GpuLimits
/// @brief   GPU resource limitations imposed on specific actions.
/// @details These resource limitations are often specified by the client
///          (via: exec_properties), but ultimately validated and decided by
///          the server. Restricting GPU availability is beneficial as they
///          are not often used by the majority of actions nor available on
///          certain workers. Particular workers generally select GPU work
///          based on the configuration of "worker platform properties" and
///          the configuration of the OperationQueue. However, if a worker
///          does take on a GPU action it may choose to further restrict GPU
///          resources.
///
public class GpuLimits {

  ///
  /// @field   limit
  /// @brief   Whether or not we perform GPU limiting on the action.
  /// @details Depending on the server implementation, we may skip applying any
  ///          restrictions on GPU usage.
  ///
  public boolean limit;

  ///
  /// @field   min
  /// @brief   The minimum number of GPUs required.
  /// @details Client can suggest this though exec_properties.
  ///
  public int min;

  ///
  /// @field   max
  /// @brief   The maximum GPUs required.
  /// @details Client can suggest this though exec_properties.
  ///
  public int max;
}
