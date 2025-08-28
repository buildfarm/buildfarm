// Copyright 2025 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.worker.shard;

import build.buildfarm.worker.cgroup.CGroupVersion;
import build.buildfarm.worker.cgroup.CGroupVersionProvider;
import java.util.logging.Logger;

/**
 * Handles cgroup version detection for hybrid wrapper/Java implementation approach. V1 uses
 * wrapper-based approach, V2 uses Java implementation.
 */
public class CgroupVersionHandler {
  private static final Logger logger = Logger.getLogger(CgroupVersionHandler.class.getName());

  public enum CgroupVersion {
    VERSION_1,
    VERSION_2,
    NONE
  }

  private final CgroupVersion version;

  public CgroupVersionHandler() {
    this.version = detectVersion();
  }

  private CgroupVersion detectVersion() {
    try {
      CGroupVersion detectedVersion = new CGroupVersionProvider().get();
      switch (detectedVersion) {
        case CGROUPS_V1:
          return CgroupVersion.VERSION_1;
        case CGROUPS_V2:
          return CgroupVersion.VERSION_2;
        default:
          return CgroupVersion.NONE;
      }
    } catch (Exception e) {
      logger.warning("Failed to detect cgroup version: " + e.getMessage());
      return CgroupVersion.NONE;
    }
  }

  public CgroupVersion getVersion() {
    return version;
  }
}
