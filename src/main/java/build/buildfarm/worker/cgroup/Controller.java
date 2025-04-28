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

package build.buildfarm.worker.cgroup;

import build.buildfarm.worker.WorkerContext.IOResource;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.UserPrincipal;

abstract class Controller implements IOResource {
  protected final Group group;

  private boolean opened = false;

  Controller(Group group) {
    this.group = group;
  }

  public abstract String getControllerName();

  protected final Path getPath() {
    if (Group.VERSION == CGroupVersion.CGROUPS_V2) {
      return group.getPath();
    } else {
      return group.getPath(getControllerName());
    }
  }

  protected final void open() throws IOException {
    if (!opened) {
      group.create(getControllerName());
      opened = true;
    }
  }

  /**
   * This method requires that all processes under the cgroup are no longer desirable and should be
   * killed as a result
   *
   * <p>This requires a posix environment, as with cgroups, and will take reasonable action to
   * attempt to end the process.
   */
  @Override
  public void close() throws IOException {
    Path path = getPath();
    boolean exists = true;
    while (exists) {
      group.killUntilEmpty(getControllerName());
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
      return !group.isEmpty(getControllerName());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void setOwner(String propertyName, UserPrincipal owner) throws IOException {
    Path path = getPath().resolve(propertyName);
    if (Files.exists(path)) { // cgroups v1 protector
      Files.setOwner(path, owner);
    }
  }

  public void setOwner(UserPrincipal owner) throws IOException {
    // an execution owner must be able to join a cgroup through group task/proc ownership
    open();
    setOwner("cgroup.procs", owner);
    // TODO: this is a cgroups v1 thing
    try {
      setOwner("tasks", owner);
    } catch (NoSuchFileException nsfe) {
      /* swallowed */
    }
  }

  protected void writeInt(String propertyName, int value) throws IOException {
    Path path = getPath().resolve(propertyName);
    try (Writer out = new OutputStreamWriter(Files.newOutputStream(path))) {
      out.write(String.format("%d\n", value));
    }
  }

  protected void writeIntPair(String propertyName, int value, int value2) throws IOException {
    Path path = getPath().resolve(propertyName);
    try (Writer out = new OutputStreamWriter(Files.newOutputStream(path))) {
      out.write(String.format("%d %d\n", value, value2));
    }
  }

  protected void writeLong(String propertyName, long value) throws IOException {
    Path path = getPath().resolve(propertyName);
    try (Writer out = new OutputStreamWriter(Files.newOutputStream(path))) {
      out.write(String.format("%d\n", value));
    }
  }

  protected int readLong(String propertyName) throws IOException {
    char[] data = new char[64];
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
