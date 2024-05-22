// Copyright 2014 The Bazel Authors. All rights reserved.
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

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.Objects;
import javax.annotation.Nullable;

/** Directory entry representation returned by . */
public final class Dirent implements Serializable, Comparable<Dirent> {
  /** Type of the directory entry */
  public enum Type {
    // A regular file.
    FILE,
    // A directory.
    DIRECTORY,
    // A symlink.
    SYMLINK,
    // Not one of the above. For example, a special file.
    UNKNOWN
  }

  private final String name;
  private final Type type;
  @Nullable private final FileStatus stat;

  /** Creates a new dirent with the given name and type, both of which must be non-null. */
  @SuppressWarnings("NullableProblems")
  public Dirent(String name, Type type, FileStatus stat) {
    this.name = Preconditions.checkNotNull(name);
    this.type = Preconditions.checkNotNull(type);
    this.stat = stat;
  }

  public String getName() {
    return name;
  }

  public Type getType() {
    return type;
  }

  @Nullable
  public FileStatus getStat() {
    return stat;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Dirent otherDirent)) {
      return false;
    }
    if (this == other) {
      return true;
    }
    return name.equals(otherDirent.name) && type.equals(otherDirent.type);
  }

  @Override
  public String toString() {
    return name + "[" + type.toString().toLowerCase() + "]";
  }

  @Override
  public int compareTo(Dirent other) {
    return this.getName().compareTo(other.getName());
  }
}
