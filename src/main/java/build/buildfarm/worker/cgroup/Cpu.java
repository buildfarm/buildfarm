// Copyright 2020 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.worker.cgroup;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;

public class Cpu extends Controller {

  private static final int CPU_GRANULARITY = 100_000; // microseconds (μS)

  Cpu(Group group) {
    super(group);
  }

  @Override
  public String getControllerName() {
    return "cpu";
  }

  @Deprecated(forRemoval = true)
  public int getShares() throws IOException {
    open();
    return readLong("cpu.shares");
  }

  /**
   * Represents how much CPU shares are allocated.
   *
   * @see <a
   *     href="https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html#weights">Cgroups
   *     v2: Weights</a>
   * @param shares how many vCPUs to give. a value of one means one CPU core.
   * @throws IOException
   */
  public void setShares(int shares) throws IOException {
    if (shares > 0) {
      open();
      // If you really have a 1000-core machine, please patch this =)
      checkArgument(
          shares < 1000,
          "for cgroups v1 and v2 compatibility, the argument is now the cpu cores to allocate.");
      if (Group.VERSION == CGroupVersion.CGROUPS_V2) {
        // cpu.weight in the range [1,10_000]
        // the default is 100, so we want to set something every time.
        writeInt("cpu.weight", shares);
      } else if (Group.VERSION == CGroupVersion.CGROUPS_V1) {
        writeInt("cpu.shares", shares * 1024);
      }
    }
  }

  /**
   * Set the maximum number of cores.
   *
   * @param cpuCores whole cores. 1 == 1 CPU core.
   */
  public void setMaxCpu(int cpuCores) throws IOException {
    open();
    if (Group.VERSION == CGroupVersion.CGROUPS_V2) {
      writeIntPair("cpu.max", cpuCores * CPU_GRANULARITY, CPU_GRANULARITY);
    } else if (Group.VERSION == CGroupVersion.CGROUPS_V1) {
      setCFSPeriodAndQuota(CPU_GRANULARITY, cpuCores * CPU_GRANULARITY);
    }
  }

  // CGroups v1
  @Deprecated(forRemoval = true)
  private void setCFSPeriodAndQuota(int periodMicroseconds, int quotaMicroseconds)
      throws IOException {
    writeInt("cpu.cfs_period_us", periodMicroseconds);
    writeInt("cpu.cfs_quota_us", quotaMicroseconds);
  }
}
