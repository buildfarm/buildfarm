// Copyright 2022 The Buildfarm Authors. All rights reserved.
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

import java.io.IOException;

/**
 * @class ProcessUtils
 * @brief Utilities related to java processes.
 * @details Contains thread-safe utility on starting java processes.
 */
public class ProcessUtils {
  // We represent an ideal global lock for
  // process invocations, which is required due to the following race condition:

  // Linux does not provide a safe API for a multi-threaded program to fork a subprocess.
  // Consider the case where two threads both write an executable file and then try to execute
  // it. It can happen that the first thread writes its executable file, with the file
  // descriptor still being open when the second thread forks, with the fork inheriting a copy
  // of the file descriptor. Then the first thread closes the original file descriptor, and
  // proceeds to execute the file. At that point Linux sees an open file descriptor to the file
  // and returns ETXTBSY (Text file busy) as an error. This race is inherent in the fork / exec
  // duality, with fork always inheriting a copy of the file descriptor table; if there was a
  // way to fork without copying the entire file descriptor table (e.g., only copy specific
  // entries), we could avoid this race.
  //
  // I was able to reproduce this problem reliably by running significantly more threads than
  // there are CPU cores on my workstation - the more threads the more likely it happens.
  //
  /**
   * Loads data from storage or external source
   * @param builder the builder parameter
   * @return the process result
   */
  // As a workaround, we put a synchronized block around the fork.
  // Bazel also does this:
  // https://github.com/bazelbuild/bazel/blob/1deb3f7aa22ec322786360085f9eb723e624d7c7/src/main/java/com/google/devtools/build/lib/shell/JavaSubprocessFactory.java#L146
  public static synchronized Process threadSafeStart(ProcessBuilder builder) throws IOException {
    return builder.start();
  }
}
