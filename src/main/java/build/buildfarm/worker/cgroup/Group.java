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

import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import io.grpc.Deadline;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import jnr.constants.platform.Signal;
import jnr.posix.POSIX;
import jnr.posix.POSIXFactory;
import lombok.Getter;
import lombok.extern.java.Log;

@Log
public final class Group {
  @Getter private static final Group root = new Group(/* name= */ null, /* parent= */ null);
  private static final Path rootPath = Paths.get("/sys/fs/cgroup");
  private static final POSIX posix = POSIXFactory.getNativePOSIX();

  @Getter @Nullable private final String name;
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

  /**
   * Get the filesystem path to the given controller for <c>this</c> CGroup.
   *
   * <p>This is for CGroups v1. Starts with '/sys/fs/cgroup'.
   *
   * @param controllerName CGroup v1 Controller name, such as "cpu" or "mem".
   * @return Filesystem path to the CGroup's controller.
   */
  Path getPath(String controllerName) {
    return rootPath.resolve(getHierarchy(controllerName));
  }

  /**
   * Get the filesystem path to the given controller for <c>this</c> CGroup.
   *
   * <p>This is for CGroups v2. Starts with '/sys/fs/cgroup'.
   *
   * @return Filesystem path to the CGroup.
   */
  Path getPath() {
    return rootPath.resolve(getHierarchy());
  }

  /**
   * Determine if the controller is applied to any processes
   *
   * @param controllerName The CGroup v1 controller
   * @return <c>true</c> if there are any processes under control of the given controller name in
   *     this cgroup, <c>false</c> otherwise.
   */
  boolean isEmpty(String controllerName) throws IOException {
    return getPids(controllerName).isEmpty();
  }

  /**
   * Send SIGKILL to some process IDs
   *
   * @param processIds Process IDs to send a signal to
   */
  private void killAllProcesses(Iterable<Integer> processIds) {
    for (int pid : processIds) {
      posix.kill(pid, Signal.SIGKILL.intValue());
    }
  }

  /**
   * Get the list of Process IDs in a given CGroup by name.
   *
   * @param controllerName cgroup name, relative to cgroup root.
   * @return Set of process IDs or empty set if the CGroup is currently empty.
   * @throws IOException if the CGroup process list cannot be read or parsed.
   */
  @VisibleForTesting
  @Nonnull
  Set<Integer> getPids(String controllerName) throws IOException {
    Path path = getPath(controllerName);
    Path procs = path.resolve("cgroup.procs");
    try {
      return Files.readAllLines(procs).stream()
          .map(Integer::parseInt)
          .collect(ImmutableSet.toImmutableSet());
    } catch (IOException e) {
      if (Files.exists(path)) {
        throw e;
      }
      // nonexistent controller path means no processes
      return ImmutableSet.of();
    }
  }

  public void killUntilEmpty(String controllerName) throws IOException {
    Deadline deadline = Deadline.after(1, SECONDS);
    Set<Integer> prevPids = new HashSet<>();
    for (Set<Integer> pids = getPids(controllerName);
        !pids.isEmpty();
        pids = getPids(controllerName)) {
      killAllProcesses(pids);
      if (deadline.isExpired() || !pids.equals(prevPids)) {
        deadline = Deadline.after(1, SECONDS);
        log.warning("Sent SIGKILL to pids: " + pids);
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
