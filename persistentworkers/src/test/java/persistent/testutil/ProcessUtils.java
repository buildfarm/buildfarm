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

package persistent.testutil;

import static com.google.common.truth.Truth.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.io.IOUtils;
import persistent.bazel.client.PersistentWorker;
import persistent.common.processes.JavaProcessWrapper;

public class ProcessUtils {
  /**
   * Creates a new java process with the specified classpath and classname (with the main method),
   * passing along the --persistent_worker flag.
   */
  public static JavaProcessWrapper spawnPersistentWorkerProcess(String classpath, String className)
      throws IOException {
    JavaProcessWrapper jpw =
        new JavaProcessWrapper(
            Path.of("."),
            classpath,
            className,
            new String[] {PersistentWorker.PERSISTENT_WORKER_FLAG});
    assertThat(jpw.isAlive()).isTrue();
    return jpw;
  }

  public static JavaProcessWrapper spawnPersistentWorkerProcess(Path jar, String className)
      throws IOException {
    return spawnPersistentWorkerProcess(jar.toString(), className);
  }

  public static JavaProcessWrapper spawnPersistentWorkerProcess(String classpath, Class<?> clazz)
      throws IOException {
    String className = clazz.getPackage().getName() + "." + clazz.getSimpleName();
    return spawnPersistentWorkerProcess(classpath, className);
  }

  public static Path retrieveFileResource(ClassLoader classLoader, String filename, Path targetPath)
      throws IOException {
    InputStream is = classLoader.getResourceAsStream(filename);

    Files.write(targetPath, IOUtils.toByteArray(is));

    return targetPath;
  }
}
