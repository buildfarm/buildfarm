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

package build.buildfarm.cas;

import static com.google.common.truth.Truth.assertThat;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import com.google.common.collect.Iterables;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FileDirectoriesIndexTest {
  private final DigestUtil DIGEST_UTIL = new DigestUtil(HashFunction.SHA256);

  private final String jdbcIndexUrl = "jdbc:sqlite:test:";
  private Path root;
  private FileDirectoriesIndex directoriesIndex;

  @Before
  public void setUp() throws IOException {
    Path fileSystemRoot =
        Iterables.getFirst(
            Jimfs.newFileSystem(
                    Configuration.unix()
                        .toBuilder()
                        .setAttributeViews("basic", "owner", "posix", "unix")
                        .build())
                .getRootDirectories(),
            null);
    root = fileSystemRoot.resolve("cache");
    directoriesIndex = new FileDirectoriesIndex(jdbcIndexUrl, root);
    Files.createDirectories(root);
  }

  @Test
  public void testFileDirectoriesIndex() throws IOException {
    // create directory and file
    ByteString coolBlob = ByteString.copyFromUtf8("cool content");
    Digest digest = DIGEST_UTIL.compute(coolBlob);
    String dirName = "cool_dir";
    Path path = root.resolve(dirName);
    Files.write(path, coolBlob.toByteArray());

    // test directoryEntries
    Digest directory = DIGEST_UTIL.compute(coolBlob);
    Iterable<String> entries = directoriesIndex.directoryEntries(directory);
    for (String e : entries) {
      assertThat(e).matches(digest.getHash());
    }
    // test put method
    directoriesIndex.put(directory, entries);

    // test entry-wise remove
    for (String entry : entries) {
      Set<Digest> digests = directoriesIndex.removeEntry(entry);
      assertThat(digests.size() == 1 && digests.contains(directory)).isTrue();
    }

    // put again to test remove by directory
    Files.write(path, coolBlob.toByteArray());
    directoriesIndex.put(directory, entries);
    directoriesIndex.remove(directory);
    assertThat(Files.notExists(directoriesIndex.path(directory))).isTrue();
  }
}
