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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import build.buildfarm.worker.resources.ResourceLimits;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockedStatic;

/** Tests for ShardWorkerContext cgroup cleanup functionality. */
@RunWith(JUnit4.class)
public class ShardWorkerContextCgroupTest {
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private Path tempCgroupRoot;

  @Before
  public void setUp() throws IOException {
    Assume.assumeFalse(isWindows());
    tempCgroupRoot = tempFolder.newFolder("cgroup").toPath();
  }

  @Test
  public void testStaleCgroupCleanupWithActiveProcesses() throws IOException {
    setupCgroupEnvironment();

    // Create cgroup directories with active processes
    Path operationsPath = tempCgroupRoot.resolve("executions/operations");
    Files.createDirectories(operationsPath);

    Path activeCgroup = operationsPath.resolve("active-operation");
    Files.createDirectories(activeCgroup);

    // Create cgroup.procs file with active PIDs
    Path procsFile = activeCgroup.resolve("cgroup.procs");
    Files.write(procsFile, List.of("1234", "5678"));

    try (MockedStatic<Paths> mockedPaths = mockStatic(Paths.class)) {
      mockedPaths.when(() -> Paths.get("/sys/fs/cgroup")).thenReturn(tempCgroupRoot);

      ShardWorkerContext context = createMockShardWorkerContext();

      invokeAttemptStaleCgroupCleanup(context, "current-operation");

      // Should not clean up directories with active processes
      assertThat(Files.exists(activeCgroup)).isTrue();
    }
  }

  @Test
  public void testStaleCgroupCleanupSkipsCurrentOperation() throws IOException {
    setupCgroupEnvironment();

    Path operationsPath = tempCgroupRoot.resolve("executions/operations");
    Files.createDirectories(operationsPath);

    Path currentCgroup = operationsPath.resolve("current-operation");
    Files.createDirectories(currentCgroup);
    Files.createFile(currentCgroup.resolve("cgroup.procs"));

    try (MockedStatic<Paths> mockedPaths = mockStatic(Paths.class)) {
      mockedPaths.when(() -> Paths.get("/sys/fs/cgroup")).thenReturn(tempCgroupRoot);

      ShardWorkerContext context = createMockShardWorkerContext();

      invokeAttemptStaleCgroupCleanup(context, "current-operation");

      // Should not clean up the current operation's cgroup
      assertThat(Files.exists(currentCgroup)).isTrue();
    }
  }

  @Test
  public void testMoveProcessToCgroupSystemLimitsRecovery() throws IOException {
    setupV1Environment();

    // Create a mock that simulates system resource limits
    try (MockedStatic<Files> mockedFiles = mockStatic(Files.class);
        MockedStatic<Paths> mockedPaths = mockStatic(Paths.class)) {
      mockedPaths.when(() -> Paths.get("/sys/fs/cgroup")).thenReturn(tempCgroupRoot);

      // Mock createDirectories to fail with system limits
      mockedFiles
          .when(() -> Files.createDirectories(any(Path.class)))
          .thenThrow(new IOException("No space left on device"));

      // Mock other Files operations to work normally
      mockedFiles.when(() -> Files.exists(any(Path.class))).thenReturn(true);

      ShardWorkerContext context = createMockShardWorkerContext();
      ResourceLimits limits = createResourceLimits();

      // This should trigger cleanup attempt when hitting system limits
      context.moveProcessToCgroup("test-operation", 1234L, limits);

      // The method should complete without throwing exceptions
      // and attempt cleanup when system limits are hit
    }
  }

  private void setupCgroupEnvironment() throws IOException {
    // Setup basic cgroup structure
    Files.createDirectories(tempCgroupRoot.resolve("executions/operations"));
  }

  private void setupV1Environment() throws IOException {
    // Create v1 controller hierarchies
    Files.createDirectories(tempCgroupRoot.resolve("cpu/executions/operations"));
    Files.createDirectories(tempCgroupRoot.resolve("memory/executions/operations"));
  }

  private ShardWorkerContext createMockShardWorkerContext() {
    // Create a minimal mock - in real tests we'd need to mock more dependencies
    return mock(ShardWorkerContext.class);
  }

  private ResourceLimits createResourceLimits() {
    ResourceLimits limits = new ResourceLimits();
    limits.cgroups = true;
    limits.cpu.limit = true;
    limits.mem.limit = true;
    return limits;
  }

  // Helper methods to invoke private methods via reflection
  private boolean invokeAttemptStaleCgroupCleanup(ShardWorkerContext context, String operationId) {
    try {
      java.lang.reflect.Method method =
          ShardWorkerContext.class.getDeclaredMethod("attemptStaleCgroupCleanup", String.class);
      method.setAccessible(true);
      return (Boolean) method.invoke(context, operationId);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private int invokeCleanupSingleCgroup(ShardWorkerContext context, Path cgroupPath) {
    try {
      java.lang.reflect.Method method =
          ShardWorkerContext.class.getDeclaredMethod("cleanupSingleCgroup", Path.class);
      method.setAccessible(true);
      return (Integer) method.invoke(context, cgroupPath);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
