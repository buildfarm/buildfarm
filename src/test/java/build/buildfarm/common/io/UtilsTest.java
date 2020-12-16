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

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class UtilsTest {
  private Path root;

  private FileStore fileStore;

  @Before
  public void setUp() throws IOException {
    root = Files.createTempDirectory("native-cas-test");
    fileStore = Files.getFileStore(root);
  }

  @After
  public void tearDown() throws IOException {
    Directories.remove(root);
  }

  @Test
  public void fileKeysVerifySameFile() throws IOException {
    Path path = root.resolve("a");
    ByteString blob = ByteString.copyFromUtf8("content for a");
    Files.write(path, blob.toByteArray());
    Files.createLink(root.resolve("b"), path);

    List<NamedFileKey> files = Utils.listDirentSorted(root, fileStore);
    assertThat(files.size()).isEqualTo(2);
    Object firstKey = files.get(0).getFileKey();
    Object secondKey = files.get(1).getFileKey();
    assertThat(firstKey).isEqualTo(secondKey);
  }

  @Test
  public void fileKeysVerifyDifferentFiles() throws IOException {
    Path pathA = root.resolve("a");
    ByteString blobA = ByteString.copyFromUtf8("content for a");
    Files.write(pathA, blobA.toByteArray());

    Path pathB = root.resolve("b");
    ByteString blobB = ByteString.copyFromUtf8("content for b");
    Files.write(pathB, blobB.toByteArray());

    List<NamedFileKey> files = Utils.listDirentSorted(root, fileStore);
    assertThat(files.size()).isEqualTo(2);
    Object firstKey = files.get(0).getFileKey();
    Object secondKey = files.get(1).getFileKey();
    assertThat(firstKey).isNotEqualTo(secondKey);
  }
}
