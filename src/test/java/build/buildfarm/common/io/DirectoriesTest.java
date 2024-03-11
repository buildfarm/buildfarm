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

package build.buildfarm.common.io;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

class DirectoriesTest {
  protected final Path root;
  protected FileStore fileStore;

  protected DirectoriesTest(Path root) {
    this.root = root;
  }

  @Before
  public void setUp() throws IOException {
    fileStore = Files.getFileStore(root);
  }

  @After
  public void tearDown() throws IOException {
    // restore write permissions
    if (Files.exists(root)) {
      Directories.enableAllWriteAccess(root, fileStore);
    }
    fileStore = null;
  }

  @Test
  public void removeDirectoryDeletesTree() throws IOException {
    Path tree = root.resolve("tree");
    Files.createDirectory(tree);
    Files.write(tree.resolve("file"), ImmutableList.of("Top level file"), StandardCharsets.UTF_8);
    Path subdir = tree.resolve("subdir");
    Files.createDirectory(subdir);
    Files.write(
        subdir.resolve("file"),
        ImmutableList.of("A file in a subdirectory"),
        StandardCharsets.UTF_8);

    Directories.remove(tree, fileStore);

    assertThat(Files.exists(tree)).isFalse();
  }

  @Test
  public void changePermissionsForDelete() throws IOException {
    // establish directory tree
    Path tree = root.resolve("tree2");
    Files.createDirectory(tree);
    Files.write(tree.resolve("file"), ImmutableList.of("Top level file"), StandardCharsets.UTF_8);
    Path subdir = tree.resolve("subdir");
    Files.createDirectory(subdir);
    Files.write(
        subdir.resolve("file"),
        ImmutableList.of("A file in a subdirectory"),
        StandardCharsets.UTF_8);

    // remove write permissions
    Directories.disableAllWriteAccess(tree, fileStore);

    // directories are able to be removed, because the algorithm
    // changes the write permissions before performing the delete.
    Directories.remove(tree, fileStore);
    assertThat(Files.exists(tree)).isFalse();
  }

  @RunWith(JUnit4.class)
  public static class NativeDirectoriesTest extends DirectoriesTest {
    public NativeDirectoriesTest() throws IOException {
      super(createTempDirectory());
    }

    private static Path createTempDirectory() throws IOException {
      if (Thread.interrupted()) {
        throw new RuntimeException(new InterruptedException());
      }
      return Files.createTempDirectory("native-utils-test");
    }

    @Test
    public void checkWriteDisabled() throws IOException {
      // establish directory tree
      Path tree = root.resolve("tree3");
      Files.createDirectory(tree);
      Files.write(tree.resolve("file"), ImmutableList.of("Top level file"), StandardCharsets.UTF_8);
      Path subdir = tree.resolve("subdir");
      Files.createDirectory(subdir);
      Files.write(
          subdir.resolve("file"),
          ImmutableList.of("A file in a subdirectory"),
          StandardCharsets.UTF_8);

      // check initial write conditions
      assertThat(Files.isWritable(tree)).isTrue();
      assertThat(Files.isWritable(subdir)).isTrue();

      // remove write permissions
      Directories.disableAllWriteAccess(tree, fileStore);

      // check that write conditions have changed
      // If the unit tests were run as root,
      // Java's isWritable will show true despite the correct permissions being set.
      // This can be seen when running unit tests in docker, or directly with sudo.
      if (!System.getProperty("user.name").equals("root")) {
        assertThat(Files.isWritable(tree)).isFalse();
        assertThat(Files.isWritable(subdir)).isFalse();
      }
    }
  }

  @RunWith(JUnit4.class)
  public static class OsXDirectoriesTest extends DirectoriesTest {
    public OsXDirectoriesTest() {
      super(
          Iterables.getFirst(
              Jimfs.newFileSystem(
                      Configuration.osX().toBuilder()
                          .setAttributeViews("basic", "owner", "posix", "unix")
                          .build())
                  .getRootDirectories(),
              null));
    }
  }

  @RunWith(JUnit4.class)
  public static class UnixDirectoriesTest extends DirectoriesTest {
    public UnixDirectoriesTest() {
      super(
          Iterables.getFirst(
              Jimfs.newFileSystem(
                      Configuration.unix().toBuilder()
                          .setAttributeViews("basic", "owner", "posix", "unix")
                          .build())
                  .getRootDirectories(),
              null));
    }
  }

  @RunWith(JUnit4.class)
  public static class WindowsDirectoriesTest extends DirectoriesTest {
    public WindowsDirectoriesTest() {
      super(
          Iterables.getFirst(
              Jimfs.newFileSystem(
                      Configuration.windows().toBuilder()
                          .setAttributeViews("basic", "owner", "dos", "acl", "posix", "user")
                          .build())
                  .getRootDirectories(),
              null));
    }
  }
}
