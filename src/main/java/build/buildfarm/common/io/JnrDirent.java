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

package build.buildfarm.common.io;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.Objects;
import javax.annotation.Nullable;
import jnr.posix.FileStat;

/** Directory entry representation returned by . */
public final class JnrDirent implements Serializable, Comparable<JnrDirent> {
  private final String name;
  @Nullable private final FileStat stat;

  /** Creates a new jnr dirent with the given name */
  @SuppressWarnings("NullableProblems")
  public JnrDirent(String name, FileStat stat) {
    this.name = Preconditions.checkNotNull(name);
    this.stat = stat;
  }

  public String getName() {
    return name;
  }

  @Nullable
  public FileStat getStat() {
    return stat;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof JnrDirent otherJnrDirent)) {
      return false;
    }
    if (this == other) {
      return true;
    }
    return name.equals(otherJnrDirent.name);
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public int compareTo(JnrDirent other) {
    return this.getName().compareTo(other.getName());
  }
}
