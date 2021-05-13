// Copyright 2021 The Bazel Authors. All rights reserved.
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
 * @class Execution Wrappers
 * @brief Execution wrappers understood and used by buildfarm.
 * @details These are the program names chosen when indicated through execution properties which
 *     wrappers to use. Users can still configure their own unique execution wrappers as execution
 *     policies in the worker configuration file.
 */
public class ExecutionWrappers {

  /**
   * @field CGROUPS
   * @brief The program to use when running actions under cgroups.
   * @details This program is expected to be packaged with the worker image.
   */
  public static final String CGROUPS = "/usr/bin/cgexec";

  /**
   * @field UNSHARE
   * @brief The program to use when desiring to unshare namespaces from the action.
   * @details This program is expected to be packaged with the worker image.
   */
  public static final String UNSHARE = "/usr/bin/unshare";

  /**
   * @field LINUX_SANDBOX
   * @brief The program to use when running actions under bazel's sandbox.
   * @details This program is expected to be packaged with the worker image.
   */
  public static final String LINUX_SANDBOX = "/app/buildfarm/linux-sandbox";

  /**
   * @field AS_NOBODY
   * @brief The program to use when running actions as "as-nobody".
   * @details This program is expected to be packaged with the worker image. The linux-sandbox is
   *     also capable of doing what this standalone programs does and may be chosen instead.
   */
  public static final String AS_NOBODY = "/app/buildfarm/as-nobody";

  /**
   * @field PROCESS_WRAPPER
   * @brief The program to use when running actions under bazel's process-wrapper
   * @details This program is expected to be packaged with the worker image.
   */
  public static final String PROCESS_WRAPPER = "/app/buildfarm/process-wrapper";
}
