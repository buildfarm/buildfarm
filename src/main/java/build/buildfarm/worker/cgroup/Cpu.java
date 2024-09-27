// Copyright 2020 The Bazel Authors. All rights reserved.
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
  Cpu(Group group) {
    super(group);
  }

  @Override
  public String getName() {
    return "cpu";
  }

  public int getShares() throws IOException {
    open();
    return readLong("cpu.shares");
  }

  public void setShares(int shares) throws IOException {
    open();
    writeInt("cpu.shares", shares);
  }

  public int getCFSPeriod() throws IOException {
    open();
    return readLong("cpu.cfs_period_us");
  }

  public void setCFSPeriod(int microseconds) throws IOException {
    open();
    writeInt("cpu.cfs_period_us", microseconds);
  }

  public void setCFSQuota(int microseconds) throws IOException {
    open();
    writeInt("cpu.cfs_quota_us", microseconds);
  }
}
