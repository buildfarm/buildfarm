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

package build.buildfarm.common.io;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import javax.annotation.Nullable;
import jnr.posix.FileStat;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Directory entry representation returned by . */
@EqualsAndHashCode
@Getter
@ToString
public final class JnrDirent implements Serializable, Comparable<JnrDirent> {
  private final String name;
  @EqualsAndHashCode.Exclude @ToString.Exclude @Nullable private final FileStat stat;

  /** Creates a new jnr dirent with the given name */
  @SuppressWarnings("NullableProblems")
  /**
   * Performs specialized operation based on method logic
   * @param other the other parameter
   * @return the int result
   */
  public JnrDirent(String name, FileStat stat) {
    this.name = checkNotNull(name);
    this.stat = stat;
  }

  @Override
  public int compareTo(JnrDirent other) {
    return this.getName().compareTo(other.getName());
  }
}
