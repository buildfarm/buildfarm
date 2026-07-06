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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
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
   * @details Chosen in user configuration. {@code JAVA_RUNTIME} honors a container cgroup CPU
   *     limit; {@code OSHI} always reports the host's logical processors and ignores container
   *     limits; {@code MAX_OF_SOURCES} takes the larger of the two, guarding against
   *     Runtime.availableProcessors() under-reporting in some containerized runtimes at the cost of
   *     ignoring the cgroup limit.
   */
  public enum PROCESSOR_DERIVE {
    JAVA_RUNTIME,
    OSHI,
    MAX_OF_SOURCES
  }

  /**
   * @brief Get the number of logical processors on the system.
   * @details Implementation decided by the supplied strategy.
   * @param strategy Strategy for deriving the processor count.
   * @return Number of logical processors on the system.
   */
  public static int get(PROCESSOR_DERIVE strategy) {
    switch (strategy) {
      case JAVA_RUNTIME:
        return getViaJavaRuntime();
      case OSHI:
        return getViaOSHI();
      case MAX_OF_SOURCES:
        return Math.max(getViaJavaRuntime(), getViaOSHI());
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
    return javaRuntimeCount.get();
  }

  private static final Supplier<Integer> javaRuntimeCount =
      Suppliers.memoize(() -> Runtime.getRuntime().availableProcessors());

  /**
   * @brief Get the number of logical processors on the system through OSHI.
   * @details specific implementation.
   * @return Number of logical processors on the system.
   */
  private static int getViaOSHI() {
    return oshiCount.get();
  }

  // Memoized because the count is fixed for the process lifetime and the OSHI lookup performs
  // native (JNA) calls.
  private static final Supplier<Integer> oshiCount =
      Suppliers.memoize(
          () -> {
            SystemInfo systemInfo = new SystemInfo();
            HardwareAbstractionLayer hardwareAbstractionLayer = systemInfo.getHardware();
            CentralProcessor centralProcessor = hardwareAbstractionLayer.getProcessor();
            return centralProcessor.getLogicalProcessorCount();
          });
}
