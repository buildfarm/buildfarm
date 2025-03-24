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

package build.buildfarm.common.config;

import lombok.Data;

/**
 * @class Execution Wrappers
 * @brief Execution wrappers understood and used by buildfarm.
 * @details These are the program names chosen when indicated through execution properties which
 *     wrappers to use. Users can still configure their own unique execution wrappers as execution
 *     policies in the worker configuration file.
 */
@Data
public class ExecutionWrappers {
  /**
   * @field cgroups2
   * @brief The program to use when running actions under cgroups v2.
   * @details This program is expected to be packaged with the worker image.
   */
  private String cgroups2 = "/app/build_buildfarm/cgexec-wrapper";

  /**
   * @field cgroups1
   * @brief The program to use when running actions under cgroups v1.
   * @details This program is expected to be packaged with the worker image.
   */
  @Deprecated(forRemoval = true)
  private String cgroups1 = "/usr/bin/cgexec";

  /**
   * @field unshare
   * @brief The program to use when desiring to unshare namespaces from the action.
   * @details This program is expected to be packaged with the worker image.
   */
  private String unshare = "/usr/bin/unshare";

  /**
   * @field linuxSandbox
   * @brief The program to use when running actions under bazel's sandbox.
   * @details This program is expected to be packaged with the worker image.
   */
  private String linuxSandbox = "/app/build_buildfarm/linux-sandbox";

  /**
   * @field asNobody
   * @brief The program to use when running actions as "as-nobody".
   * @details This program is expected to be packaged with the worker image. The linux-sandbox is
   *     also capable of doing what this standalone programs does and may be chosen instead.
   */
  private String asNobody = "/app/build_buildfarm/as-nobody";

  /**
   * @field processWrapper
   * @brief The program to use when running actions under bazel's process-wrapper
   * @details This program is expected to be packaged with the worker image.
   */
  private String processWrapper = "/app/build_buildfarm/process-wrapper";

  /**
   * @field skipSleep
   * @brief The program to use when running actions under bazel's skip sleep wrapper.
   * @details This program is expected to be packaged with the worker image.
   */
  private String skipSleep = "/app/build_buildfarm/skip_sleep";

  /**
   * @field skipSleepPreload
   * @brief The shared object that the skip sleep wrapper uses to spoof syscalls.
   * @details The shared object needs passed to the program which will LD_PRELOAD it.
   */
  private String skipSleepPreload = "/app/build_buildfarm/skip_sleep_preload.so";

  /**
   * @field delay
   * @brief The program to used to timeshift actions when running under skip_sleep.
   * @details This program is expected to be packaged with the worker image. Warning: This wrapper
   *     is only intended to be used with skip_sleep.
   */
  private String delay = "/app/build_buildfarm/delay.sh";
}
