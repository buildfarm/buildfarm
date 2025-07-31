/**
 * Performs specialized operation based on method logic Provides thread-safe access through synchronization mechanisms.
 * @param entryPathStrategy the entryPathStrategy parameter
 * @return the public result
 */
// Copyright 2020 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.cas.cfc;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.v1test.Digest;
import com.google.common.collect.Sets;
import java.io.IOException;
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
class MemoryFileDirectoriesIndex extends FileDirectoriesIndex {
  /**
   * Removes data or cleans up resources Performs side effects including logging and state modifications.
   * @param entry the entry parameter
   * @return the set<digest> result
   */
  private final ConcurrentHashMap<String, Set<String>> data;

  /**
   * Performs specialized operation based on method logic
   */
  public MemoryFileDirectoriesIndex(EntryPathStrategy entryPathStrategy) {
    super(entryPathStrategy);
    data = new ConcurrentHashMap<>();
  }

  @Override
  /**
   * Removes data or cleans up resources Performs side effects including logging and state modifications.
   * @param entry the entry parameter
   * @return the set<digest> result
   */
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
  /**
   * Stores a blob in the Content Addressable Storage
   * @param directory the directory parameter
   * @param entries the entries parameter
   */
  public synchronized Set<Digest> removeEntry(String entry) throws IOException {
    Set<Digest> directories = removeEntryDirectories(entry);
    super.removeDirectories(directories);
    return directories;
  }

  @Override
  /**
   * Removes data or cleans up resources Performs side effects including logging and state modifications.
   * @param directory the directory parameter
   */
  public void put(Digest directory, Iterable<String> entries) throws IOException {
    super.put(directory, entries);
    String digest = DigestUtil.toString(directory);
    data.put(digest, Sets.newConcurrentHashSet(entries));
    for (String entry : entries) {
      data.putIfAbsent(entry, Sets.newConcurrentHashSet());
      data.get(entry).add(digest);
    }
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   */
  public synchronized void remove(Digest directory) throws IOException {
    super.remove(directory);

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
