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

import static com.google.common.io.MoreFiles.asCharSink;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.DigestUtil;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;

/**
 * Use ConcurrentHashMap in memory to store bidirectional entry directory mapping. Comparing to
 * using sqlite, this should reduce worker startup time a lot, but will also cause high memory
 * usage.
 */
class MemoryFileDirectoriesIndex extends DirectoriesIndex {
  private static final Charset UTF_8 = Charset.forName("UTF-8");

  private final ConcurrentHashMap<String, Set<String>> data;

  public MemoryFileDirectoriesIndex(Path root) {
    super(root);
    data = new ConcurrentHashMap<>();
  }

  @Override
  public void close() {}

  @GuardedBy("this")
  private Set<Digest> removeEntryDirectories(String entry) {
    Set<String> directories = data.remove(entry);
    directories = directories == null ? new HashSet<>() : directories;
    for (String directory : directories) {
      data.remove(directory);
    }

    return directories.stream().map(DigestUtil::parseDigest).collect(Collectors.toSet());
  }

  @Override
  public synchronized Set<Digest> removeEntry(String entry) throws IOException {
    Set<Digest> directories = removeEntryDirectories(entry);

    for (Digest directory : directories) {
      try {
        Files.delete(path(directory));
      } catch (NoSuchFileException e) {
        // ignore
      }
    }
    return directories;
  }

  @Override
  public void put(Digest directory, Iterable<String> entries) throws IOException {
    asCharSink(path(directory), UTF_8).writeLines(entries);
    String digest = DigestUtil.toString(directory);
    data.put(digest, Sets.newConcurrentHashSet(entries));
    for (String entry : entries) {
      data.putIfAbsent(entry, Sets.newConcurrentHashSet());
      data.get(entry).add(digest);
    }
  }

  @Override
  public synchronized void remove(Digest directory) throws IOException {
    try {
      Files.delete(path(directory));
    } catch (NoSuchFileException e) {
      // ignore
    }

    String digest = DigestUtil.toString(directory);
    Set<String> entries = data.remove(digest);
    if (entries == null) return;
    for (String entry : entries) {
      data.get(entry).remove(digest);
    }
  }

  @Override
  public void start() {}
}
