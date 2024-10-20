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

import static com.google.common.truth.Truth.assertThat;
import static java.lang.String.format;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HexBucketEntryPathStrategyTest {
  @Test
  public void branchDirectoriesNoLevelsIsEmpty() {
    Path path = Paths.get("cache");
    EntryPathStrategy entryPathStrategy = new HexBucketEntryPathStrategy(path, 0);

    Iterator<Path> paths = entryPathStrategy.branchDirectories().iterator();
    assertThat(paths.hasNext()).isFalse();
  }

  @Test
  public void branchDirectoriesSingleLevelsIsRoot() {
    Path path = Paths.get("cache");
    EntryPathStrategy entryPathStrategy = new HexBucketEntryPathStrategy(path, 1);

    Iterator<Path> paths = entryPathStrategy.branchDirectories().iterator();
    assertThat(paths.hasNext()).isTrue();
    Path dir = paths.next();
    assertThat(dir.equals(path)).isTrue();
    assertThat(paths.hasNext()).isFalse();
  }

  @Test
  public void branchDirectoriesMultipleLevels() {
    Path path = Paths.get("cache");
    EntryPathStrategy entryPathStrategy = new HexBucketEntryPathStrategy(path, 2);

    Iterator<Path> paths = entryPathStrategy.branchDirectories().iterator();
    assertThat(paths.hasNext()).isTrue();
    Path dir = paths.next();
    assertThat(dir.equals(path)).isTrue();
    for (int i = 0; i < 256; i++) {
      assertThat(paths.hasNext()).isTrue();
      dir = paths.next();
      assertThat(dir.equals(path.resolve(format("%02x", i)))).isTrue();
    }
    assertThat(paths.hasNext()).isFalse();
  }

  @Test
  public void getPathMatchesLevels() {
    Path path = Paths.get("cache");
    EntryPathStrategy entryPathStrategy = new HexBucketEntryPathStrategy(path, 2);

    String key = "aa55bb1100bb";
    Path blobPath = entryPathStrategy.getPath(key);
    assertThat(blobPath.toString())
        .isEqualTo(path.resolve("aa").resolve("55").resolve(key).toString());
  }
}
