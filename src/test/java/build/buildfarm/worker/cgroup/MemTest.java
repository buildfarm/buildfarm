// Copyright 2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.worker.cgroup;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for the cgroups v2 memory controller. */
@RunWith(JUnit4.class)
public class MemTest {
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void memoryLimitsUseCgroupsV2PropertiesAndSupportLongValues() throws IOException {
    Path cgroupPath = temporaryFolder.newFolder("execution").toPath();
    Group group = mock(Group.class);
    when(group.getPath()).thenReturn(cgroupPath);
    Mem mem = new Mem(group);
    long memoryLimit = (long) Integer.MAX_VALUE + 1;
    long swapLimit = memoryLimit + 1;

    mem.setMemoryLimit(memoryLimit);
    mem.setMemorySwapLimit(swapLimit);

    assertThat(Files.readString(cgroupPath.resolve("memory.max"))).isEqualTo(memoryLimit + "\n");
    assertThat(Files.readString(cgroupPath.resolve("memory.swap.max"))).isEqualTo(swapLimit + "\n");
    assertThat(mem.getMemoryLimit()).isEqualTo(memoryLimit);
    assertThat(mem.getMemorySwapLimit()).isEqualTo(swapLimit);
  }
}
