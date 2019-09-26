// Copyright 2018 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

class UtilsTest {
  private Path root;

  protected UtilsTest(Path root) {
    this.root = root;
  }

  @Test
  public void removeDirectoryDeletesTree() throws IOException {
    Path tree = root.resolve("tree");
    Files.createDirectory(tree);
    Files.write(tree.resolve("file"), ImmutableList.of("Top level file"), StandardCharsets.UTF_8);
    Path subdir = tree.resolve("subdir");
    Files.createDirectory(subdir);
    Files.write(subdir.resolve("file"), ImmutableList.of("A file in a subdirectory"), StandardCharsets.UTF_8);

    Utils.removeDirectory(tree);

    assertThat(Files.exists(tree)).isFalse();
  }

  @RunWith(JUnit4.class)
  public static class NativeUtilsTest extends UtilsTest {
    public NativeUtilsTest() throws IOException {
      super(createTempDirectory());
    }
    
    private static Path createTempDirectory() throws IOException {
      if (Thread.interrupted()) {
        throw new RuntimeException(new InterruptedException());
      }
      Path path = Files.createTempDirectory("native-utils-test");
      return path;
    }
  }

  @RunWith(JUnit4.class)
  public static class OsXUtilsTest extends UtilsTest {
    public OsXUtilsTest() {
      super(Iterables.getFirst(
          Jimfs.newFileSystem(Configuration.osX()).getRootDirectories(),
          null));
    }
  }

  @RunWith(JUnit4.class)
  public static class UnixUtilsTest extends UtilsTest {
    public UnixUtilsTest() {
      super(Iterables.getFirst(
          Jimfs.newFileSystem(Configuration.unix()).getRootDirectories(),
          null));
    }
  }

  @RunWith(JUnit4.class)
  public static class WindowsUtilsTest extends UtilsTest {
    public WindowsUtilsTest() {
      super(Iterables.getFirst(
          Jimfs.newFileSystem(Configuration.windows()).getRootDirectories(),
          null));
    }
  }
}
