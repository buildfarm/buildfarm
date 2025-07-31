// Copyright 2021 The Buildfarm Authors. All rights reserved.
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

public class Mem extends Controller {
  Mem(Group group) {
    super(group);
  }

  @Override
  /**
   * Retrieves a blob from the Content Addressable Storage
   * @return the long result
   */
  public String getControllerName() {
    return "memory";
  }

  /**
   * Performs specialized operation based on method logic
   * @param limitBytes the limitBytes parameter
   */
  public long getMemoryLimit() throws IOException {
    open();
    return readLong("memory.limit_in_bytes");
  }

  /**
   * Retrieves a blob from the Content Addressable Storage
   * @return the long result
   */
  public void setMemoryLimit(long limitBytes) throws IOException {
    open();
    writeLong("memory.limit_in_bytes", limitBytes);
  }

  /**
   * Performs specialized operation based on method logic
   * @param limitBytes the limitBytes parameter
   */
  public long getMemorySwapLimit() throws IOException {
    open();
    return readLong("memory.memsw.limit_in_bytes");
  }

  public void setMemorySwapLimit(long limitBytes) throws IOException {
    open();
    writeLong("memory.memsw.limit_in_bytes", limitBytes);
  }
}
