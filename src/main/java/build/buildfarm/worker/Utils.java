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

import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Platform.Property;
import build.buildfarm.common.FileStatus;
import build.buildfarm.common.IOUtils;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

public class Utils {
  private Utils() {}

  public static FileStatus statIfFound(Path path, boolean followSymlinks) {
    try {
      return IOUtils.stat(path, followSymlinks);
    } catch (NoSuchFileException e) {
      return null;
    } catch (IOException e) {
      // If this codepath is ever hit, then this method should be rewritten to properly distinguish
      // between not-found exceptions and others.
      throw new IllegalStateException(e);
    }
  }

  static int commandCoreValue(boolean onlyMulticoreTests, String name, Command command) {
    int cores = -1;
    for (Property property : command.getPlatform().getPropertiesList()) {
      if (property.getName().equals(name)) {
        cores = Integer.parseInt(property.getValue());
        if (cores > 1 && onlyMulticoreTests && !commandIsTest(command)) {
          cores = 1;
        }
      }
    }
    return cores;
  }

  public static int commandMaxCores(boolean onlyMulticoreTests, Command command) {
    return commandCoreValue(onlyMulticoreTests, "max-cores", command);
  }

  public static int commandMinCores(boolean onlyMulticoreTests, Command command) {
    return commandCoreValue(onlyMulticoreTests, "min-cores", command);
  }

  static boolean commandIsTest(Command command) {
    // only tests are setting this currently - other mechanisms are unreliable
    return Iterables.any(
        command.getEnvironmentVariablesList(),
        (envVar) -> envVar.getName().equals("XML_OUTPUT_FILE"));
  }
}
