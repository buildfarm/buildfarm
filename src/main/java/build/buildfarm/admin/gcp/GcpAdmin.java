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

package build.buildfarm.admin.gcp;

import build.buildfarm.admin.Admin;
import build.buildfarm.v1test.GetHostsResult;

public class GcpAdmin implements Admin {
  @Override
  public void terminateHost(String hostId) {
    throw new UnsupportedOperationException("Not Implemented.");
  }

  @Override
  public void stopContainer(String hostId, String containerName) {
    throw new UnsupportedOperationException("Not Implemented.");
  }

  @Override
  public GetHostsResult getHosts(String filter, int ageInMinutes, String status) {
    throw new UnsupportedOperationException("Not Implemented.");
  }

  @Override
  public void scaleCluster(
      String scaleGroupName,
      Integer minHosts,
      Integer maxHosts,
      Integer targetHosts,
      Integer targetReservedHostsPercent) {
    throw new UnsupportedOperationException("Not Implemented.");
  }
}
