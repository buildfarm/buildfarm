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
/// @class   NetworkLimits
/// @brief   Network limitations imposed on specific actions.
/// @details These resource limitations are often specified by the client
///          (via: exec_properties), but ultimately validated and decided by
///          the server. Restricting network access is common for unit tests
///          as it can be a source of flakiness (ex: multiple tests trying to
///          open the same port). Applying these restrictions can encourage
///          test writers to mock network related functionality.
///
public class NetworkLimits {

  ///
  /// @field   restrict
  /// @brief   Whether or not to restrict network access for the action.
  /// @details Depending on the server implementation, we may skip applying any
  ///          network restrictions.
  ///
  public boolean restrict;
}
