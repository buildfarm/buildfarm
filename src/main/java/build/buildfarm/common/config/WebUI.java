// Copyright 2023 The Bazel Authors. All rights reserved.
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

package build.buildfarm.common.config;

import lombok.Data;

/**
 * @class WebUI
 * @brief Settings for buildfarm's web UI.
 * @details Buildfarm provides a web frontend for developers to introspect builds.
 */
@Data
public class WebUI {
  /**
   * @field enable
   * @brief Whether to enable the web frontend.
   * @details When disabled there will be no ports opened or routes available.
   */
  public boolean enable = false;

  /**
   * @field port
   * @brief HTTP port for the web frontend.
   * @details 8080 is useful for local testing since port 80 requires sudo. We choose the following
   *     default since many ports are blocked in upstream CI.
   */
  public String port = "8982";
}
