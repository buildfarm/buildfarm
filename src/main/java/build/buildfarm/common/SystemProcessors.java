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

package build.buildfarm.common;

/**
 * @class SystemProcessors
 * @brief Abstraction for getting information about the system processors.
 * @details We've found that using java's Runtime.getRuntime().availableProcessors() utility does
 *     not always accurately reflect the amount of processors available. In some cases it returns 1
 *     due to containerization and virtualization. For example, if you are using k8s with containerd
 *     you might see this method give back 1 based on your particular deployment. There are other
 *     implementations such as OSHI that use JNA to acquire the native operating system and hardware
 *     information which is often be more suitable for buildfarm. For consistency in buildfarm
 *     deriving configuration values, and allocating its own thread pools, it's best to source the
 *     processor count from the same place. This abstracts implementation on how to derive processor
 *     count based on config and environment.
 */
public class SystemProcessors {
  /**
   * @brief Get the number of logical processors on the system.
   * @details Implementation decided by configuration.
   * @return Number of logical processors on the system.
   */
  public static long get() {
    return Runtime.getRuntime().availableProcessors()
  }
}
