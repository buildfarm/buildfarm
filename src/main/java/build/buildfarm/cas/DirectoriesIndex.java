// Copyright 2020 The Bazel Authors. All rights reserved.
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

import static com.google.common.io.MoreFiles.asCharSource;

import build.bazel.remote.execution.v2.Digest;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Set;

/**
 * Abstract class for entry/directory mappings.
 *
 * <p>Directories should maintain non-unique entries Entries should reference unique directories.
 */
abstract class DirectoriesIndex {
  private static final Charset UTF_8 = Charset.forName("UTF-8");

  final Path root;

  DirectoriesIndex(Path root) {
    this.root = root;
  }

  Path path(Digest digest) {
    return root.resolve(digest.getHash() + "_" + digest.getSizeBytes() + "_dir_inputs");
  }

  public Iterable<String> directoryEntries(Digest directory) throws IOException {
    try {
      return asCharSource(path(directory), UTF_8).readLines();
    } catch (NoSuchFileException e) {
      return ImmutableList.of();
    }
  }

  abstract void close();

  abstract Set<Digest> removeEntry(String entry) throws IOException;

  abstract void put(Digest directory, Iterable<String> entries) throws IOException;

  abstract void remove(Digest directory) throws IOException;

  abstract void start();
}
