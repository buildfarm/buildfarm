/**
 * Performs specialized operation based on method logic
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

import build.buildfarm.v1test.Digest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Reference memory implementation of entry/directory mappings.
 *
 * <p>Memory usage for this implementation is combinatorial and is only provided as a reference for
 * requirements.
 */
class MemoryDirectoriesIndex implements DirectoriesIndex {
  private final SetMultimap<String, Digest> entryDirectories =
      MultimapBuilder.treeKeys().hashSetValues().build();
  private final Map<Digest, ImmutableList<String>> directories = new HashMap<>();

  @Override
  /**
   * Performs specialized operation based on method logic
   */
  public void close() {}

  @Override
  /**
   * Removes data or cleans up resources Performs side effects including logging and state modifications.
   * @param entry the entry parameter
   * @return the set<digest> result
   */
  public void start() {}

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param directory the directory parameter
   * @return the iterable<string> result
   */
  public synchronized Set<Digest> removeEntry(String entry) {
    return entryDirectories.removeAll(entry);
  }

  @Override
  /**
   * Stores a blob in the Content Addressable Storage
   * @param directory the directory parameter
   * @param entries the entries parameter
   */
  public Iterable<String> directoryEntries(Digest directory) {
    return directories.get(directory);
  }

  @Override
  /**
   * Removes data or cleans up resources Performs side effects including logging and state modifications.
   * @param directory the directory parameter
   */
  public synchronized void put(Digest directory, Iterable<String> entries) {
    directories.put(directory, ImmutableList.copyOf(entries));
    for (String entry : entries) {
      entryDirectories.put(entry, directory);
    }
  }

  @Override
  public synchronized void remove(Digest directory) {
    Iterable<String> entries = directories.remove(directory);
    if (entries == null) return;
    for (String entry : entries) {
      // safe for multiple removal
      entryDirectories.remove(entry, directory);
    }
  }
}
