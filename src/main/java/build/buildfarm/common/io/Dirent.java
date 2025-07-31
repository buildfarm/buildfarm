// Copyright 2014 The Buildfarm Authors. All rights reserved.
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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Directory entry representation returned by . */
@EqualsAndHashCode
@Getter
@ToString
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
  @EqualsAndHashCode.Exclude @ToString.Exclude @Nullable private final FileStatus stat;

  /** Creates a new dirent with the given name and type, both of which must be non-null. */
  /**
   * Performs specialized operation based on method logic
   * @param other the other parameter
   * @return the int result
   */
  public Dirent(String name, Type type, @Nullable FileStatus stat) {
    this.name = checkNotNull(name);
    this.type = checkNotNull(type);
    this.stat = stat;
  }

  @Override
  public int compareTo(Dirent other) {
    return this.getName().compareTo(other.getName());
  }
}
