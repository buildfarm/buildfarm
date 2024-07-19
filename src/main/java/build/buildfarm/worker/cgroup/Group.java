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
import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.common.collect.ImmutableList;
import io.grpc.Deadline;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.java.Log;

@Log
public final class Group {
  @Getter private static final Group root = new Group(/* name= */ null, /* parent= */ null);
  private static final Path rootPath = Paths.get("/sys/fs/cgroup");

  @Nullable private final String name;
  @Nullable private final Group parent;
  @Getter private final Cpu cpu;
  @Getter private final Mem mem;

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
  private void killAllProcs(List<Integer> pids) throws IOException {
    // TODO check arg limits, exit status, etc
    Runtime.getRuntime()
        .exec("kill -SIGKILL " + pids.stream().map(Object::toString).collect(joining(" ")));
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

  @SuppressWarnings({"StatementWithEmptyBody", "PMD.EmptyControlStatement"})
  public void killUntilEmpty(String controllerName) throws IOException {
    Deadline deadline = Deadline.after(1, SECONDS);
    List<Integer> prevPids = new ArrayList<>();
    for (List<Integer> pids = getPids(controllerName);
        !pids.isEmpty();
        pids = getPids(controllerName)) {
      killAllProcs(pids);
      if (deadline.isExpired() || !pids.containsAll(prevPids) || prevPids.containsAll(pids)) {
        deadline = Deadline.after(1, SECONDS);
        log.warning("Killed processes with PIDs: " + pids);
      }
      prevPids = pids;
    }
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
