// Copyright 2017 The Bazel Authors. All rights reserved.
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

package build.buildfarm.common;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class ProxyDirectoriesIndex implements Map<Digest, Directory> {
  private final Map<String, Directory> directoriesIndex;

  public ProxyDirectoriesIndex(Map<String, Directory> directoriesIndex) {
    this.directoriesIndex = directoriesIndex;
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsKey(Object key) {
    if (key instanceof Digest) {
      return directoriesIndex.containsKey(DigestUtil.toString((Digest) key));
    }
    return false;
  }

  @Override
  public boolean containsValue(Object value) {
    return directoriesIndex.containsValue(value);
  }

  @SuppressWarnings("NullableProblems")
  @Override
  public Set<Map.Entry<Digest, Directory>> entrySet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Directory get(Object key) {
    if (key instanceof Digest digest) {
      return directoriesIndex.get(digest.getHash());
    }
    return null;
  }

  @Override
  public int hashCode() {
    return directoriesIndex.hashCode();
  }

  @Override
  public boolean isEmpty() {
    return directoriesIndex.isEmpty();
  }

  @SuppressWarnings("NullableProblems")
  @Override
  public Set<Digest> keySet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Directory put(Digest key, Directory value) {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("NullableProblems")
  @Override
  public void putAll(Map<? extends Digest, ? extends Directory> m) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Directory remove(Object key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int size() {
    return directoriesIndex.size();
  }

  @SuppressWarnings("NullableProblems")
  @Override
  public Collection<Directory> values() {
    return directoriesIndex.values();
  }
}
