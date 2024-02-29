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

import static java.util.stream.Collectors.joining;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import javax.annotation.Nullable;
import lombok.extern.java.Log;

@Log
public final class Group {
  private static final Group root = new Group(/* name= */ null, /* parent= */ null);
  private static final Path rootPath = Paths.get("/sys/fs/cgroup");

  private @Nullable final String name;
  private @Nullable final Group parent;
  private final Cpu cpu;
  private final Mem mem;

  public static Group getRoot() {
    return root;
  }

  @SuppressWarnings("NullableProblems")
  private Group(String name, Group parent) {
    this.name = name;
    this.parent = parent;
    cpu = new Cpu(this);
    mem = new Mem(this);
  }

  public Group getChild(String name) {
    return new Group(name, this);
  }

  @SuppressWarnings("NullableProblems")
  public String getName() {
    return name;
  }

  public Cpu getCpu() {
    return cpu;
  }

  public Mem getMem() {
    return mem;
  }

  public String getHierarchy() {
    /* is root */
    if (parent == null) {
      return "";
    }
    /* parent is root */
    if (parent.getName() == null) {
      return getName();
    }
    /* is child of non-root parent */
    return parent.getHierarchy() + "/" + getName();
  }

  String getHierarchy(String controllerName) {
    if (parent != null) {
      return parent.getHierarchy(controllerName) + "/" + getName();
    }
    return controllerName;
  }

  Path getPath(String controllerName) {
    return rootPath.resolve(getHierarchy(controllerName));
  }

  public boolean isEmpty(String controllerName) throws IOException {
    return getProcCount(controllerName) == 0;
  }

  public int getProcCount(String controllerName) throws IOException {
    return getPids(controllerName).size();
  }

  /** Returns true if no processes exist under the controller path */
  private boolean killAllProcs(String controllerName) throws IOException {
    List<Integer> pids = getPids(controllerName);
    if (!pids.isEmpty()) {
      // TODO check arg limits, exit status, etc
      Runtime.getRuntime()
          .exec("kill -SIGKILL " + pids.stream().map(Object::toString).collect(joining(" ")));
      log.warning("Killed processes with PIDs: " + pids);
      return false;
    }
    return true;
  }

  private List<Integer> getPids(String controllerName) throws IOException {
    Path path = getPath(controllerName);
    Path procs = path.resolve("cgroup.procs");
    try {
      return Files.readAllLines(procs).stream()
          .map(Integer::parseInt)
          .collect(ImmutableList.toImmutableList());
    } catch (IOException e) {
      if (Files.exists(path)) {
        throw e;
      }
      // nonexistent controller path means no processes
      return ImmutableList.of();
    }
  }

  @SuppressWarnings("StatementWithEmptyBody")
  public void killUntilEmpty(String controllerName) throws IOException {
    while (!killAllProcs(controllerName))
      ;
  }

  void create(String controllerName) throws IOException {
    /* root already has all controllers created */
    if (parent != null) {
      parent.create(controllerName);
      Path path = getPath(controllerName);
      if (!Files.exists(path)) {
        Files.createDirectory(path);
      }
    }
  }
}
