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

package build.buildfarm.common;

import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.HardwareAbstractionLayer;

/**
 * @class SystemProcessors
 * @brief Abstraction for getting information about the system processors.
 * @details We've found that using java's Runtime.getRuntime().availableProcessors() utility does
 *     not always accurately reflect the amount of processors available. In some cases it returns 1
 *     due to containerization or virtualization. For example, if you are using k8s with containerd
 *     you might see this method give back 1 based on your particular deployment. There are other
 *     implementations such as OSHI that use JNA to acquire the native operating system and hardware
 *     information which is often more suitable for buildfarm. In order to provide consistency in
 *     deriving configuration values, and allocating thread pools, it's best to source the processor
 *     count from the same place. This abstracts implementation on how we derive processor count
 *     based on config and environment.
 */
public class SystemProcessors {
  /**
   * @field PROCESSOR_DERIVE
   * @brief Strategies for getting total processor counts.
   * @details Can be chosen in user configuration.
   */
  public enum PROCESSOR_DERIVE {
    JAVA_RUNTIME,
    OSHI
  }

  /**
   * @field cachedProcessorCount
   * @brief Cached processor count.
   * @details We cache the processor count since it won't change during runtime.
   */
  private static Integer cachedProcessorCount = null;

  /**
   * @brief Get the number of logical processors on the system.
   * @details Buildfarm will choose the best implementation.
   * @return Number of logical processors on the system.
   */
  public static int get() {
    // Cache the processor count since it won't change during runtime
    if (cachedProcessorCount == null) {
      cachedProcessorCount =
          Math.max(get(PROCESSOR_DERIVE.JAVA_RUNTIME), get(PROCESSOR_DERIVE.OSHI));
    }
    return cachedProcessorCount;
  }

  /**
   * @brief Get the number of logical processors on the system.
   * @details Implementation decided by configuration.
   * @return Number of logical processors on the system.
   */
  public static int get(PROCESSOR_DERIVE strategy) {
    switch (strategy) {
      case JAVA_RUNTIME:
        return getViaJavaRuntime();
      case OSHI:
        return getViaOSHI();
      default:
        return getViaJavaRuntime();
    }
  }

  /**
   * @brief Get the number of logical processors on the system through java runtime.
   * @details specific implementation.
   * @return Number of logical processors on the system.
   */
  private static int getViaJavaRuntime() {
    return Runtime.getRuntime().availableProcessors();
  }

  /**
   * @brief Get the number of logical processors on the system through OSHI.
   * @details specific implementation.
   * @return Number of logical processors on the system.
   */
  private static int getViaOSHI() {
    SystemInfo systemInfo = new SystemInfo();
    HardwareAbstractionLayer hardwareAbstractionLayer = systemInfo.getHardware();
    CentralProcessor centralProcessor = hardwareAbstractionLayer.getProcessor();
    return centralProcessor.getLogicalProcessorCount();
  }
}
