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

  /**
   * Represents how much CPU shares are allocated.
   *
   * @see <a
   *     href="https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html#weights">Cgroups
   *     v2: Weights</a>
   * @param shares
   * @throws IOException
   */
  public void setShares(int shares) throws IOException {
    if (shares > 0) {
      open();
      // cpu.weight in the range [1,1000]
      // the default is 100, so we want to set something every time.
      writeInt("cpu.weight", shares);
    }
  }

  /**
   * Set the maximum number of cores.
   *
   * @param cpuCores whole cores. 1 == 1 CPU core.
   */
  public void setMaxCpu(int cpuCores) throws IOException {
    open();
    writeIntPair("cpu.max", cpuCores * CPU_GRANULARITY, CPU_GRANULARITY);
  }
}
