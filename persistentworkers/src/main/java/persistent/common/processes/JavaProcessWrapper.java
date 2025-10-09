// Copyright 2023-2025 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package persistent.common.processes;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JavaProcessWrapper extends ProcessWrapper {
  // Get the path of the JVM from the current process to avoid breaking the Bazel sandbox
  public static final String CURRENT_JVM_COMMAND =
      ProcessHandle.current()
          .info()
          .command()
          .orElseThrow(
              () -> new RuntimeException("Unable to retrieve the path of the running JVM"));

  public JavaProcessWrapper(Path workDir, String classPath, String fullClassName, String[] args)
      throws IOException {
    super(
        workDir,
        cmdArgs(new String[] {CURRENT_JVM_COMMAND, "-cp", classPath, fullClassName}, args));
  }

  public static ImmutableList<String> cmdArgs(String[] cmd, String[] args) {
    List<String> resultList = new ArrayList<>(cmd.length + args.length);
    Collections.addAll(resultList, cmd);
    Collections.addAll(resultList, args);
    return ImmutableList.copyOf(resultList);
  }
}
