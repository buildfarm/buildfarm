// Copyright 2026 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.common.io;

import static com.google.common.truth.Truth.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FileSystemMoverTest {
  @Test
  public void atomicMoverReplacesTargetAndConsumesSource() throws IOException {
    Path root = Files.createTempDirectory("atomic-mover-test");
    try {
      Path source = Files.writeString(root.resolve("source"), "new");
      Path target = Files.writeString(root.resolve("target"), "old");

      FileSystemMover.atomicMover().move(source, target);

      assertThat(Files.readString(target)).isEqualTo("new");
      assertThat(Files.exists(source)).isFalse();
    } finally {
      Directories.remove(root, Files.getFileStore(root));
    }
  }

  @Test
  public void hardLinkMoverReplacesTargetAndPreservesSource() throws IOException {
    Path root = Files.createTempDirectory("hard-link-mover-test");
    try {
      Path source = Files.writeString(root.resolve("source"), "new");
      Path target = Files.writeString(root.resolve("target"), "old");

      FileSystemMover.hardLinkMover().move(source, target);

      assertThat(Files.readString(target)).isEqualTo("new");
      assertThat(Files.isSameFile(source, target)).isTrue();
    } finally {
      Directories.remove(root, Files.getFileStore(root));
    }
  }
}
