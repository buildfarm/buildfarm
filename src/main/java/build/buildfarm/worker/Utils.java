// Copyright 2019 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker;

import static build.buildfarm.common.io.Utils.stat;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

import build.buildfarm.common.io.FileStatus;
import com.google.common.base.Stopwatch;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import org.checkerframework.checker.units.qual.Prefix;
import org.checkerframework.checker.units.qual.s;

public final class Utils {
  private Utils() {}

  public static FileStatus statIfFound(Path path, boolean followSymlinks, FileStore fileStore) {
    try {
      return stat(path, followSymlinks, fileStore);
    } catch (NoSuchFileException e) {
      return null;
    } catch (IOException e) {
      // If this codepath is ever hit, then this method should be rewritten to properly distinguish
      // between not-found exceptions and others.
      throw new IllegalStateException(e);
    }
  }

  @SuppressWarnings("units")
  public static @s(Prefix.micro) long stopwatchToMicroseconds(Stopwatch sw) {
    return sw.elapsed(MICROSECONDS);
  }

  /** A typed constant for 0 uS (Microseconds) */
  @SuppressWarnings("units")
  public static final @s(Prefix.micro) long ZERO_MICROSECONDS = 0;
}
