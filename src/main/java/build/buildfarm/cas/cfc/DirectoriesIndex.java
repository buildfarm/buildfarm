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

package build.buildfarm.cas.cfc;

import build.buildfarm.v1test.Digest;
import java.io.IOException;
import java.util.Set;

/**
 * Interface for entry/directory mappings.
 *
 * <p>Directories should maintain non-unique entries Entries should reference unique directories.
 */
interface DirectoriesIndex {
  Iterable<String> directoryEntries(Digest directory) throws IOException;

  void close();

  Set<Digest> removeEntry(String entry) throws IOException;

  void put(Digest directory, Iterable<String> entries) throws IOException;

  void remove(Digest directory) throws IOException;

  void start();
}
