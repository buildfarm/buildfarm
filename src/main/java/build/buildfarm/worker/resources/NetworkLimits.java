// Copyright 2021 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.worker.resources;

import java.util.ArrayList;

/**
 * @class NetworkLimits
 * @brief Network resource limitations imposed on specific actions.
 * @details These resource limitations are often specified by the client (via: exec_properties), but
 *     ultimately validated and decided by the server. Restricting network access can make actions
 *     more reliable. Even unit tests that access localhost can conflict when similar tests are
 *     doing the same.
 */
public class NetworkLimits {
  /**
   * @field blockNetwork
   * @brief Whether or not to block network access.
   * @details Depending on the server implementation, we may skip applying network restrictions.
   */
  public boolean blockNetwork = false;

  /**
   * @field fakeHostname
   * @brief Make hostname 'localhost' during sandbox execution.
   * @details Under the sandbox, indicate that the hostname should be 'localhost'.
   */
  public boolean fakeHostname = false;

  /**
   * @field description
   * @brief Description explaining why settings were chosen.
   * @details This can be used to debug execution behavior.
   */
  public final ArrayList<String> description = new ArrayList<>();
}
