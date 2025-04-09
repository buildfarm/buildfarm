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

package build.buildfarm.worker.cgroup;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Supplier;
import java.util.logging.Level;
import lombok.extern.java.Log;

@Log
public class CGroupVersionProvider implements Supplier<CGroupVersion> {

  @Override
  public CGroupVersion get() {
    /* Try to figure out which version of CGroups is available. */
    try {
      Path path = Path.of("/sys/fs/cgroup");
      if (path.toFile().exists() && path.toFile().isDirectory()) {
        FileStore fileStore = Files.getFileStore(path);
        String fsType = fileStore.type();
        if (fsType.equals("cgroup2")) {
          return CGroupVersion.CGROUPS_V2;
        } else {
          /* fsType=cgroup or fsType=tmpfs */
          log.log(
              Level.WARNING,
              "cgroups v1 detected at /sys/fs/cgroup ! This is the last Buildfarm version to"
                  + " support v1 - Please upgrade your host to cgroups v2! See also"
                  + " https://github.com/buildfarm/buildfarm/issues/2205");
          return CGroupVersion.CGROUPS_V1;
        }
      }
    } catch (IOException e) {
      log.log(
          Level.WARNING,
          "Could not auto-detect CGroups version in /sys/fs/cgroup, assuming no CGroups support",
          e);
    }
    // Give up.
    log.log(Level.WARNING, "No cgroups support");
    return CGroupVersion.NONE;
  }
}
