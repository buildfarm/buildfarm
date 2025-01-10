// Copyright 2023 The Buildfarm Authors. All rights reserved.
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

import java.util.ArrayList;
import java.util.List;
import lombok.Data;

/**
 * @class Sandbox Settings
 * @brief Settings used by the worker when deciding to use the sandbox as an execution wrapper.
 * @details Using the sandbox can be configurable by the client via exec_properties. However,
 *     sometimes it is preferred to enable it via buildfarm config to prevent users from running
 *     actions outside the sandbox.
 */
@Data
public class SandboxSettings {
  /**
   * @field alwaysUseSandbox
   * @brief Whether or not to always use the sandbox when running actions.
   * @details It may be preferred to enforce sandbox usage than rely on client selection.
   */
  private boolean alwaysUseSandbox = false;

  /**
   * @field alwaysUseAsNobody
   * @brief Whether or not to always use the as-nobody wrapper when running actions.
   * @details It may be preferred to enforce this wrapper instead of relying on client selection.
   */
  private boolean alwaysUseAsNobody = false;

  /**
   * @field alwaysUseCgroups
   * @brief Whether or not to use cgroups when sandboxing actions.
   * @details It may be preferred to enforce cgroup usage.
   */
  private boolean alwaysUseCgroups = true;

  /**
   * @field alwaysUseTmpFs
   * @brief Whether or not to always use tmpfs when using the sandbox.
   * @details It may be preferred to enforce sandbox usage than rely on client selection.
   */
  private boolean alwaysUseTmpFs = false;

  /**
   * @field additionalWritePaths
   * @brief Additional paths the sandbox is allowed to write to.
   * @details Suggestions may include: /tmp, /dev/shm
   */
  private List<String> additionalWritePaths = new ArrayList<>();

  /**
   * @field tmpFsPaths
   * @brief Additional paths the sandbox uses for tmpfs
   * @details Suggestions may include: /tmp
   */
  private List<String> tmpFsPaths = new ArrayList<>();

  /**
   * @field selectForBlockNetwork
   * @brief If the action requires "block network" use the sandbox to fulfill this request.
   * @details Otherwise, there may be no alternative solution and the "block network" request will
   *     be ignored / implemented differently.
   */
  private boolean selectForBlockNetwork = false;

  /**
   * @field selectForTmpFs
   * @brief If the action requires "tmpfs" use the sandbox to fulfill this request.
   * @details Otherwise, there may be no alternative solution and the "tmpfs" request will be
   *     ignored / implemented differently.
   */
  private boolean selectForTmpFs = false;
}
