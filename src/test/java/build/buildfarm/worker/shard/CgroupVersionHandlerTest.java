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

import static build.buildfarm.common.base.System.isWindows;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockedStatic;

/** Tests for {@link CgroupVersionHandler}. */
@RunWith(JUnit4.class)
public class CgroupVersionHandlerTest {
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private Path tempCgroupRoot;
  private CgroupVersionHandler handler;

  @Before
  public void setUp() throws IOException {
    Assume.assumeFalse(isWindows());
    tempCgroupRoot = tempFolder.newFolder("cgroup").toPath();
  }

  @Test
  public void testDetectCgroupV2() throws IOException {
    // Create v2 unified hierarchy markers
    Files.createFile(tempCgroupRoot.resolve("cgroup.controllers"));
    Files.createFile(tempCgroupRoot.resolve("cgroup.procs"));

    try (MockedStatic<Paths> mockedPaths = mockStatic(Paths.class)) {
      mockedPaths.when(() -> Paths.get("/sys/fs/cgroup")).thenReturn(tempCgroupRoot);
      handler = new CgroupVersionHandler();
      assertThat(handler.getVersion()).isEqualTo(CgroupVersionHandler.CgroupVersion.VERSION_2);
    }
  }

  @Test
  public void testDetectCgroupV1() throws IOException {
    // Create v1 controller directories
    Files.createDirectories(tempCgroupRoot.resolve("cpu"));
    Files.createDirectories(tempCgroupRoot.resolve("memory"));

    try (MockedStatic<Paths> mockedPaths = mockStatic(Paths.class)) {
      mockedPaths.when(() -> Paths.get("/sys/fs/cgroup")).thenReturn(tempCgroupRoot);
      handler = new CgroupVersionHandler();
      assertThat(handler.getVersion()).isEqualTo(CgroupVersionHandler.CgroupVersion.VERSION_1);
    }
  }

  @Test
  public void testMoveProcessNonExistentProcess() throws IOException {
    setupV2Environment();

    try (MockedStatic<Paths> mockedPaths = mockStatic(Paths.class);
        MockedStatic<Files> mockedFiles = mockStatic(Files.class)) {
      mockedPaths.when(() -> Paths.get("/sys/fs/cgroup")).thenReturn(tempCgroupRoot);
      mockedPaths
          .when(() -> Paths.get("/proc/9999"))
          .thenReturn(tempCgroupRoot.resolve("proc/9999"));

      // Mock process as non-existent
      mockedFiles
          .when(() -> Files.exists(any(Path.class)))
          .thenAnswer(
              invocation -> {
                Path path = invocation.getArgument(0);
                if (path.toString().contains("proc/9999")) return false;
                if (path.toString().contains("cgroup.controllers")
                    || path.toString().contains("cgroup.procs")) return true;
                return true;
              });

      handler = new CgroupVersionHandler();

      // Should handle non-existent process gracefully
      boolean result = handler.moveProcessToCgroup(9999, "test/hierarchy");
      assertThat(result).isTrue(); // Should succeed (process already gone)
    }
  }

  @Test
  public void testV1MissingEssentialController() throws IOException {
    // Setup v1 environment without memory controller
    Files.createDirectories(tempCgroupRoot.resolve("cpu"));
    Files.createFile(tempCgroupRoot.resolve("cpu/cgroup.procs"));
    // Note: no memory controller directory

    try (MockedStatic<Paths> mockedPaths = mockStatic(Paths.class)) {
      mockedPaths.when(() -> Paths.get("/sys/fs/cgroup")).thenReturn(tempCgroupRoot);
      mockedPaths
          .when(() -> Paths.get("/proc/1234"))
          .thenReturn(tempCgroupRoot.resolve("proc/1234"));

      handler = new CgroupVersionHandler();

      // Create mock process directory
      Files.createDirectories(tempCgroupRoot.resolve("proc/1234"));

      boolean result = handler.moveProcessToCgroup(1234, "test/hierarchy");
      assertThat(result).isFalse(); // Should fail due to missing essential controller
    }
  }

  private void setupV2Environment() throws IOException {
    // Create v2 unified hierarchy
    Files.createFile(tempCgroupRoot.resolve("cgroup.controllers"));
    Files.createFile(tempCgroupRoot.resolve("cgroup.procs"));
  }

  private void setupV1Environment() throws IOException {
    // Create v1 controller hierarchies
    Files.createDirectories(tempCgroupRoot.resolve("cpu"));
    Files.createDirectories(tempCgroupRoot.resolve("memory"));
    Files.createDirectories(tempCgroupRoot.resolve("cpuset"));
    Files.createDirectories(tempCgroupRoot.resolve("blkio"));

    // Create control files
    Files.createFile(tempCgroupRoot.resolve("cpu/cgroup.procs"));
    Files.createFile(tempCgroupRoot.resolve("memory/cgroup.procs"));
    Files.createFile(tempCgroupRoot.resolve("cpuset/cgroup.procs"));
    Files.createFile(tempCgroupRoot.resolve("blkio/cgroup.procs"));
  }
}
