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

package build.buildfarm.worker.cgroup;

import build.buildfarm.worker.WorkerContext.IOResource;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

abstract class Controller implements IOResource {
  protected final Group group;

  private static final Logger logger = Logger.getLogger(Controller.class.getName());

  private boolean opened = false;

  Controller(Group group) {
    this.group = group;
  }

  public abstract String getName();

  protected final Path getPath() {
    return group.getPath(getName());
  }

  protected final void open() throws IOException {
    if (!opened) {
      group.create(getName());
      opened = true;
    }
  }

  /**
   * This method requires that all processes under the cgroup are no longer desirable and should be
   * killed as a result. This requires a posix environment, as with cgroups, and will take
   * reasonable action to attempt to end the process.
   */
  @Override
  public void close() throws IOException {

    ExecutorService executor = Executors.newCachedThreadPool();

    // an exception safe call to cleaning up the cgroup resources
    Callable<Void> task =
        new Callable<Void>() {
          public Void call() {

            try {
              killAllCgroupProcesses();
            } catch (IOException e) {
              logger.log(Level.SEVERE, "Failure to kill all processes after action execution.", e);
            }
            return null;
          }
        };

    // perform cleanup
    Future<Void> future = executor.submit(task);

    // ensure cleanup does not get stuck indefinitely
    waitUpToNSeconds(future, 10);
  }

  private void waitUpToNSeconds(Future<Void> future, long nSeconds) throws IOException {
    try {
      future.get(nSeconds, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      logger.log(
          Level.SEVERE,
          "Killing all processes did not complete in a reasonable amount of time.",
          e);
    } catch (InterruptedException | ExecutionException e) {
      logger.log(Level.SEVERE, "Failure to kill all processes after action execution.", e);
    }
  }

  private void killAllCgroupProcesses() throws IOException {
    Path path = getPath();
    boolean exists = true;
    while (exists) {
      group.killUntilEmpty(getName());
      try {
        Files.delete(path);
        exists = false;
      } catch (IOException e) {
        exists = Files.exists(path);
        if (exists && !e.getMessage().endsWith("Device or resource busy")) {
          throw e;
        }
      }
    }
    opened = false;
  }

  @Override
  public boolean isReferenced() {
    try {
      return !group.isEmpty(getName());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected void writeInt(String propertyName, int value) throws IOException {
    Path path = getPath().resolve(propertyName);
    try (Writer out = new OutputStreamWriter(Files.newOutputStream(path))) {
      out.write(String.format("%d\n", value));
    }
  }

  protected int readInt(String propertyName) throws IOException {
    char[] data = new char[32];
    Path path = getPath().resolve(propertyName);
    int len;
    try (Reader in = new InputStreamReader(Files.newInputStream(path))) {
      len = in.read(data);
    }
    if (len < 0) {
      throw new NumberFormatException("premature end of stream");
    }
    if (len == 0 || data[0] == '\n' || data[len - 1] != '\n') {
      throw new NumberFormatException("invalid integer in '" + propertyName + "'");
    }
    return Integer.parseInt(String.copyValueOf(data, 0, len - 1));
  }
}
