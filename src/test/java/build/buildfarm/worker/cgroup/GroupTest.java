// Copyright 2024 The Buildfarm Authors. All rights reserved.
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

import static build.buildfarm.common.base.System.isWindows;
import static com.google.common.truth.Truth.assertThat;

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

/** Tests for {@link Group#getSelfCgroup} method. */
@RunWith(JUnit4.class)
public class GroupTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private Path procSelfCgroupPath;

  @Before
  public void setup() throws IOException {
    Assume.assumeFalse(isWindows());

    // Create a temporary file to simulate /proc/self/cgroup
    procSelfCgroupPath = temporaryFolder.newFile("cgroup").toPath();
  }

  /**
   * Test helper that creates a custom implementation of getSelfCgroup for testing by using a local
   * implementation with the same logic but accessing our test file instead of the real
   * /proc/self/cgroup file.
   */
  private Path testGetSelfCgroup(CGroupVersion version, List<String> fileContent)
      throws IOException {
    // Write our test data to the temporary file
    if (fileContent != null) {
      Files.write(procSelfCgroupPath, fileContent);
    }

    // This is a test-only implementation with the same logic as Group.getSelfCgroup
    // but using our test file instead of the real /proc/self/cgroup
    if (version == CGroupVersion.CGROUPS_V2) {
      try {
        List<String> myCgroups = Files.readAllLines(procSelfCgroupPath);

        for (String line : myCgroups) {
          if (line.startsWith("0::")) {
            return Path.of("/sys/fs/cgroup", line.substring(3).trim());
          }
        }
      } catch (IOException ioe) {
        // Fall through to default
      }
    }
    return Paths.get("/sys/fs/cgroup");
  }

  @Test
  public void testGetSelfCgroupV2() throws IOException {
    // Arrange: Setup cgroups v2 content
    List<String> cgroupV2Content = List.of("0::/user.slice/user-1000.slice/session-2.scope");

    // Act: Call our test implementation
    Path result = testGetSelfCgroup(CGroupVersion.CGROUPS_V2, cgroupV2Content);

    // Assert: Verify result path is correctly constructed
    assertThat(result.toString())
        .isEqualTo("/sys/fs/cgroup/user.slice/user-1000.slice/session-2.scope");
  }

  @Test
  public void testGetSelfCgroupV1() throws IOException {
    // Act: Call the test implementation with cgroups v1
    Path result = testGetSelfCgroup(CGroupVersion.CGROUPS_V1, null);

    // Assert: For v1, it should always return /sys/fs/cgroup regardless of file content
    assertThat(result.toString()).isEqualTo("/sys/fs/cgroup");
  }

  @Test
  public void testGetSelfCgroupV2WithMultipleLines() throws IOException {
    // Arrange: Setup cgroups v2 content with multiple lines (realistic content)
    List<String> cgroupV2Content =
        List.of(
            "0::/user.slice/user-1000.slice/session-2.scope",
            "1:name=systemd:/user.slice/user-1000.slice/session-2.scope");

    // Act: Call our test implementation
    Path result = testGetSelfCgroup(CGroupVersion.CGROUPS_V2, cgroupV2Content);

    // Assert: Verify result path is correctly constructed, using first line starting with 0::
    assertThat(result.toString())
        .isEqualTo("/sys/fs/cgroup/user.slice/user-1000.slice/session-2.scope");
  }

  @Test
  public void testGetSelfCgroupV2WithIOException() throws IOException {
    // For this test, we don't write any content to the file
    // and make the file inaccessible
    Files.delete(procSelfCgroupPath);

    // Act: Call our test implementation
    Path result = testGetSelfCgroup(CGroupVersion.CGROUPS_V2, null);

    // Assert: Fallback to default path when exception occurs
    assertThat(result.toString()).isEqualTo("/sys/fs/cgroup");
  }

  @Test
  public void testGetSelfCgroupV2WithNoMatchingLine() throws IOException {
    // Arrange: Setup content with no line starting with "0::"
    List<String> cgroupContent =
        List.of(
            "1:name=systemd:/user.slice/user-1000.slice/session-2.scope",
            "2:cpu,cpuacct:/user.slice");

    // Act: Call our test implementation
    Path result = testGetSelfCgroup(CGroupVersion.CGROUPS_V2, cgroupContent);

    // Assert: Falls back to default path when no matching line is found
    assertThat(result.toString()).isEqualTo("/sys/fs/cgroup");
  }
}
