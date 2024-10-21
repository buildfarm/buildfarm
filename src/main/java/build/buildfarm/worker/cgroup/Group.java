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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.grpc.Deadline;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.stream.Collectors;
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
  private static final Path rootPath = getSelfCgroup();
  private static final POSIX posix = POSIXFactory.getNativePOSIX();

  private static final String CGROUP_CONTROLLERS = "cgroup.controllers";
  private static final String CGROUP_SUBTREE_CONTROL = "cgroup.subtree_control";

  @Getter @Nullable private final String name;
  @Nullable private final Group parent;
  @Getter private final Cpu cpu;
  @Getter private final Mem mem;

  @SuppressWarnings("PMD.MutableStaticState") // Unit tests set this.
  protected static CGroupVersion VERSION = discoverCgroupVersion();

  private static CGroupVersion discoverCgroupVersion() {
    /* Try to figure out which version of CGroups is available. */
    try {
      List<String> allMounts = Files.readAllLines(Paths.get("/proc/mounts"));
      for (String mount : allMounts) {
        String[] split = mount.split("\\s+");
        // split[0]: the block
        // split[1]: mountpoint
        // split[2]: fs type
        checkState(split.length >= 3, "could not parse /proc/mounts");
        if (split[2].contains("cgroup")) {
          if (split[2].equals("cgroup2")) {
            return CGroupVersion.CGROUPS_V2;
          }
        }
        // by this point, we haven't found any `cgroup` or `cgroup2` fs types.
      }
    } catch (IOException e) {
      log.log(
          Level.WARNING,
          "failed to read mounts to determine CGroups version. Fallback to No CGroups.",
          e);
      return CGroupVersion.NONE;
    }
    // Give up.
    return CGroupVersion.NONE;
  }

  static Path getSelfCgroup() {
    try {
      String myCgroup = Files.readString(Paths.get("/proc/self/cgroup"));
      // This always starts with "0::" for CGroups v2.
      if (myCgroup.startsWith("0::")) {
        VERSION = CGroupVersion.CGROUPS_V2;
        return Path.of("/sys/fs/cgroup", myCgroup.substring(3).trim());
      }
    } catch (IOException ioe) {
      log.log(Level.SEVERE, "Cannot read my own cgroup!", ioe);
    }
    return Paths.get("/sys/fs/cgroup");
  }

  private Group(@Nullable String name, @Nullable Group parent) {
    this.name = name;
    this.parent = parent;
    cpu = new Cpu(this);
    mem = new Mem(this);
  }

  /**
   * Construct a child group from this group.
   *
   * @param name Child CGroup name
   * @return another Group.
   */
  public Group getChild(String name) {
    return new Group(name, this);
  }

  /**
   * Get hierarchy of this cgroup and all parents.
   *
   * <p>This is for CGroups v2 only.
   *
   * @return
   */
  public String getHierarchy() {
    checkState(VERSION == CGroupVersion.CGROUPS_V2);
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

  /**
   * Get hierarchy of this cgroup and all parents. Includes the controller name.
   *
   * <p>This is for CGroups v1 only.
   *
   * @param controllerName
   * @return
   */
  String getHierarchy(String controllerName) {
    if (parent != null) {
      return parent.getHierarchy(controllerName) + "/" + getName();
    }
    return "";
  }

  /* use for cgroups v2 */
  Path getPath() {
    return rootPath.resolve(getHierarchy());
  }

  boolean isEmpty() throws IOException {
    checkState(VERSION == CGroupVersion.CGROUPS_V2);
    return getPids().isEmpty();
  }

  /**
   * Send SIGKILL to some process IDs
   *
   * @param processIds Process IDs to send a signal to
   */
  private void killAllProcesses(Iterable<Integer> processIds) {
    Streams.stream(processIds)
        .forEach(processId -> posix.kill(processId, Signal.SIGKILL.intValue()));
  }

  @VisibleForTesting
  @Nonnull
  Set<Integer> getPids() {
    checkState(VERSION == CGroupVersion.CGROUPS_V2, "Only applicable for cgroups v2");
    return getPids(getPath());
  }

  private Set<Integer> getPids(Path path) {
    Path procs = path.resolve("cgroup.procs");
    try {
      return Files.readAllLines(procs).stream()
          .map(Integer::parseInt)
          .collect(ImmutableSet.toImmutableSet());
    } catch (IOException e) {
      if (Files.exists(path)) {
        log.log(Level.WARNING, "Unable to get PIDs for Cgroup at {0}", procs);
      }
      // nonexistent controller path means no processes
      return ImmutableSet.of();
    }
  }

  /**
   * Move some processes into this CGroup.
   *
   * @param pids
   * @see <a
   *     href="https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html#organizing-processes-and-threads">Organizing
   *     Processes and Threads</a>
   */
  void adoptPids(Iterable<Integer> pids) {
    Path path = getPath().resolve("cgroup.procs");
    verify(Files.exists(path), "cgroup procs doesn't exist");
    // Only one process can be migrated on a single write(2) call.
    for (Integer pid : pids) {
      try {
        try (Writer out = new OutputStreamWriter(Files.newOutputStream(path))) {
          out.write(String.format("%d\n", pid));
        }
      } catch (IOException e) {
        log.log(
            Level.WARNING,
            "process ID " + pid + " could not be moved to CGroup " + getPath() + "; continuing",
            e);
      }
    }
  }

  /* package */ void killUntilEmpty() throws IOException {
    if (VERSION == CGroupVersion.CGROUPS_V2) {
      killUntilEmpty(this::getPids);
    } else if (VERSION == CGroupVersion.NONE) {
      throw new RuntimeException("Cannot kill empty group without CGroups support!");
    }
  }

  /**
   * Kill all processes in this CGroup until they are terminated
   *
   * @throws IOException
   */
  private void killUntilEmpty(@Nonnull Supplier<Set<Integer>> pidProvider) throws IOException {
    Deadline deadline = Deadline.after(1, SECONDS);
    Set<Integer> prevPids = new HashSet<>();
    for (Set<Integer> pids = pidProvider.get(); !pids.isEmpty(); pids = pidProvider.get()) {
      killAllProcesses(pids);
      if (deadline.isExpired() || !pids.equals(prevPids)) {
        deadline = Deadline.after(1, SECONDS);
        log.warning("Sent SIGKILL to pids: " + pids);
      }
      prevPids = pids;
    }
  }

  static void ensureControllerIsEnabled(Path cgroupPath, Set<String> controllerNames)
      throws IOException {
    ensureControllerIsEnabled(cgroupPath, controllerNames, false);
  }

  /**
   * For a given cgroup hierarchy path, ensure that the controller name is enabled. Otherwise, the
   * limits are not enforced.
   *
   * @param cgroupPath
   * @param controllerNames - example 'mem', 'cpu'
   */
  static void ensureControllerIsEnabled(
      @Nonnull Path cgroupPath, @Nonnull Set<String> controllerNames, boolean setSubtreeControl)
      throws IOException {
    checkState(VERSION == CGroupVersion.CGROUPS_V2);
    if (cgroupPath == null) {
      return;
    }
    checkNotNull(controllerNames, "Controller names is null");
    checkArgument(!controllerNames.isEmpty(), "Controller names is empty and shouldn't be");
    // set parent folder before we handle this one.
    if (cgroupPath.equals(rootPath.getParent())) {
      // Recursion sentinel.
      return;
    }
    ensureControllerIsEnabled(cgroupPath.getParent(), controllerNames, true);

    // By now, all recursive parents of `cgroupPath` already have subcontrollers set.
    // Finally, we make our new cgroup, if it doesn't exist.
    try {
      if (!Files.exists(cgroupPath)) {
        Files.createDirectory(cgroupPath);
      }
    } catch (FileAlreadyExistsException e) {
      // per the avoidance above, we don't care that this already
      // exists and we lost a race to create it
    }

    if (setSubtreeControl) {
      Path cgroupControllerPath = cgroupPath.resolve(CGROUP_CONTROLLERS);
      while (!Files.exists(cgroupControllerPath)) {
        /* busy loop */
        log.log(Level.SEVERE, "Waiting for {0}", cgroupControllerPath);
      }
      Set<String> availableControllers =
          Arrays.stream(Files.readString(cgroupControllerPath).split("\\s+"))
              .collect(Collectors.toSet());
      if (availableControllers.isEmpty()) {
        throw new IllegalStateException(
            "There are no controllers at all in CGroup at path " + cgroupPath);
      }
      if (!availableControllers.containsAll(controllerNames)) {
        Set<String> desiredAndNotAvailable = new HashSet<>(controllerNames);
        desiredAndNotAvailable.removeAll(controllerNames);
        throw new IllegalStateException(
            "Some controllers not enabled in CGroup at path "
                + cgroupPath
                + ". Only found "
                + availableControllers
                + ", missing "
                + desiredAndNotAvailable);
      }
      Path cgroupSubtreeControlPath = cgroupPath.resolve(CGROUP_SUBTREE_CONTROL);
      while (!Files.exists(cgroupSubtreeControlPath)) {
        /* busy loop */
        log.log(Level.FINE, "Waiting for {0}", cgroupSubtreeControlPath);
      }
      Set<String> alreadyEnabledControllers =
          Arrays.stream(Files.readString(cgroupSubtreeControlPath).split("\\s+"))
              .collect(Collectors.toSet());
      log.log(Level.FINE, "Existing sub_tree control: {0}", alreadyEnabledControllers);
      if (!alreadyEnabledControllers.containsAll(controllerNames)) {
        log.log(
            Level.FINE,
            "Adding "
                + controllerNames
                + " to cgroup sub controller at "
                + cgroupSubtreeControlPath);
        try (Writer out = new OutputStreamWriter(Files.newOutputStream(cgroupSubtreeControlPath))) {
          for (String controllerName : controllerNames) {
            out.write(String.format("+%s ", controllerName));
          }
        }
      }
    }
  }

  void create(@Nonnull String controllerName) throws IOException {
    /* root already has all controllers created */
    if (!root.isEmpty()) {
      Group evacuation = root.getChild("evacuation");
      if (Files.exists(evacuation.getPath())) {
        // Clean it up.
        Files.delete(evacuation.getPath());
      }
      Files.createDirectory(evacuation.getPath());
      evacuation.adoptPids(root.getPids());
      verify(root.isEmpty(), "tried to evacuate root cgroup but there were processes remaining");
      ensureControllerIsEnabled(root.getPath(), Set.of("cpu", "memory"), true);
    }
    if (parent != null) {
      parent.create(controllerName);
      // Write "cgroup.subtree_control" file with content like this:
      // +<controller1_name> +<controller2_name>
      // (if you wish to remove a controller, prefix with `-` instead of `+`)
      Path cgroupPath = getPath();
      checkState(cgroupPath.startsWith("/sys/fs/cgroup/"));
      checkState(!cgroupPath.endsWith("/"));
      ensureControllerIsEnabled(cgroupPath, Set.of(controllerName));
    }
  }
}
