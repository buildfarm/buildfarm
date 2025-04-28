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
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import jnr.constants.platform.Signal;
import jnr.posix.POSIX;
import jnr.posix.POSIXFactory;
import lombok.Getter;
import lombok.extern.java.Log;

/**
 * Represents a cgroup abstraction.
 *
 * <p>For cgroups v2, we do an evacuation dance. For cgroups v2, it's not possible to have child
 * groups (with processes) when parent groups also have processes when subtree controllers are
 * enabled. Only "leaf cgroups" can have processes in them. So we move all the processes in the
 * current cgroup into a child cgroup named "evacuation". When running with cgroups enabled, you
 * will have two child cgroups of the starting cgroup:
 *
 * <ul>
 *   <li><b>evacuation</b> - where buildfarm, tini, and whatever else was in the cgroup when
 *       buildfarm was started
 *   <li><b>executions</b> - where the exec actions will spawn
 * </ul>
 *
 * At shutdown, we make every effort to undo this re-arrangement in the {@link #onShutdown()}
 * method.
 *
 * <p>All cgroupv1 support is temporary; please migrate to v2 as quickly as possible.
 */
@Log
public final class Group {
  @Getter private static final Group root = new Group(/* name= */ null, /* parent= */ null);
  private static final Path rootPath;
  private static final POSIX posix = POSIXFactory.getNativePOSIX();

  private static final String CGROUP_CONTROLLERS = "cgroup.controllers";
  private static final String CGROUP_SUBTREE_CONTROL = "cgroup.subtree_control";
  private static final String EVACUATION_CGROUP_NAME = "evacuation";

  /**
   * These are the controllers we need enabled for our execution cgroup so that Buildfarm can set
   * cpu, memory limits for individual executions.
   *
   * <p>We also will un-do these in {@link #onShutdown()}
   */
  private static final Set<String> REQUIRED_CGROUP_CONTROLLER_NAMES = Set.of("memory", "cpu");

  @Getter @Nullable private final String name;
  @Nullable private final Group parent;
  @Getter private final Cpu cpu;
  @Getter private final Mem mem;

  @SuppressWarnings(
      "PMD.MutableStaticState") // Unit tests set this. When CGroups v1 support is gone, this will
  // go away, too.
  public static CGroupVersion VERSION;

  static {
    // Must be done in order
    VERSION = new CGroupVersionProvider().get();
    rootPath = getSelfCgroup(VERSION);
  }

  /**
   * Find our own cgroup.
   *
   * <p>For cgroups v1, preserve old behavior of always assuming `/sys/fs/cgroup/ is the root, And
   * look for controller-specific files at <code>/sys/fs/cgroups/&lt;controller&gt;/cgroup.*</code>
   *
   * @return
   */
  static Path getSelfCgroup(CGroupVersion version) {
    if (version == CGroupVersion.CGROUPS_V2) {
      try {
        // For cgroups v2, there's only one line, and it always starts with `0::`
        // For v1, there's other lines for cgroup subsystem managers.
        List<String> myCgroups = Files.readAllLines(Paths.get("/proc/self/cgroup"));

        Optional<String> selfCgroup =
            myCgroups.stream().filter(s -> s.startsWith("0::")).findFirst();
        if (selfCgroup.isPresent()) {
          Path self = Path.of("/sys/fs/cgroup", selfCgroup.get().substring(3).trim());
          if (self.endsWith(EVACUATION_CGROUP_NAME)) {
            // but wait, there's more... we seem to have landed in an already-evacuated tree.
            log.log(
                Level.WARNING,
                String.format(
                    "Found pre-existing evacuation cgroup at %s. This indicates buildfarm did not"
                        + " shutdown cleanly",
                    self));
            // Use the parent as the root.
            return gatherAllCgroupChildProcesses(self.getParent());
          }
          return self;
        }
      } catch (IOException ioe) {
        log.log(Level.SEVERE, "Cannot read my own cgroup!", ioe);
      }
    }
    // cgroup v1 or NONE always assume /sys/fs/cgroup
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
   * @return
   */
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
    if (VERSION == CGroupVersion.CGROUPS_V2) {
      return "";
    } else {
      return controllerName;
    }
  }

  /**
   * Get the path for a given Controller Name
   *
   * <p>This is for cgroups v1 only
   *
   * @param controllerName
   * @return
   */
  @Deprecated(forRemoval = true)
  Path getPath(String controllerName) {
    if (VERSION == CGroupVersion.CGROUPS_V1) {
      return rootPath.resolve(getHierarchy(controllerName));
    }
    return getPath();
  }

  /* use for cgroups v2 */
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
  @Deprecated
  boolean isEmpty(String controllerName) throws IOException {
    if (VERSION == CGroupVersion.CGROUPS_V1) {
      return getPids(controllerName).isEmpty();
    } else if (VERSION == CGroupVersion.CGROUPS_V2) {
      return isEmpty();
    }
    throw new IllegalStateException("Not cgroups 1 or 2!");
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

  /**
   * Get the list of Process IDs in a given CGroup by name.
   *
   * @param controllerName cgroup name, relative to cgroup root.
   * @return Set of process IDs or empty set if the CGroup is currently empty.
   */
  @VisibleForTesting
  @Nonnull
  @Deprecated
  Set<Integer> getPids(String controllerName) {
    checkState(VERSION == CGroupVersion.CGROUPS_V1, "Only applicable for cgroups v1");
    return getPids(getPath(controllerName));
  }

  @VisibleForTesting
  @Nonnull
  Set<Integer> getPids() {
    checkState(VERSION == CGroupVersion.CGROUPS_V2, "Only applicable for cgroups v2");
    return getPids(getPath());
  }

  private static Set<Integer> getPids(Path path) {
    Path procs = path.resolve("cgroup.procs");
    try {
      return Files.readAllLines(procs).stream()
          .map(Integer::parseInt)
          .collect(ImmutableSet.toImmutableSet());
    } catch (IOException e) {
      if (Files.exists(path)) {
        log.log(Level.WARNING, "Unable to get PIDs for Cgroup at " + procs, e);
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
    checkState(VERSION == CGroupVersion.CGROUPS_V2, "No PID adoption for v1");
    adoptPids(getPath(), pids);
  }

  static void adoptPids(Path cgroupPath, Iterable<Integer> pids) {
    Path path = cgroupPath.resolve("cgroup.procs");
    verify(Files.exists(path), "cgroup.procs doesn't exist");
    // Only one process can be migrated on a single write(2) call.
    for (Integer pid : pids) {
      try {
        try (Writer out = new OutputStreamWriter(Files.newOutputStream(path))) {
          out.write(String.format("%d\n", pid));
        }
      } catch (IOException e) {
        log.log(
            Level.WARNING,
            "process ID " + pid + " could not be moved to CGroup " + cgroupPath + "; continuing",
            e);
      }
    }
  }

  /* package */ void killUntilEmpty(@Nullable String controllerName) throws IOException {
    if (VERSION == CGroupVersion.CGROUPS_V2) {
      killUntilEmpty(this::getPids);
    } else if (VERSION == CGroupVersion.CGROUPS_V1) {
      // CGroups v1 below:
      checkNotNull(controllerName, "Controller name is null");
      killUntilEmpty(() -> getPids(controllerName));
    } else if (VERSION == CGroupVersion.NONE) {
      throw new RuntimeException("Cannot kill empty group without CGroups support!");
    }
  }

  /**
   * Kill all processes in this CGroup until they are terminated
   *
   * @throws IOException
   */
  void killUntilEmpty(@Nonnull Supplier<Set<Integer>> pidProvider) throws IOException {
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
    // checkNotNull(cgroupPath, "cgroupPath is null");
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
    if (!Files.exists(cgroupPath)) {
      Files.createDirectories(cgroupPath);
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

        throw new IllegalStateException(
            "Some controllers not enabled in CGroup at path "
                + cgroupPath
                + ". Only found "
                + availableControllers
                + ", but wanted at least "
                + controllerNames);
      }
      Path cgroupSubtreeControlPath = cgroupPath.resolve(CGROUP_SUBTREE_CONTROL);
      while (!Files.exists(cgroupSubtreeControlPath)) {
        /* busy loop */
        log.log(Level.FINE, "Waiting for {0}", cgroupSubtreeControlPath);
      }
      Set<String> alreadyEnabledControllers =
          Arrays.stream(Files.readString(cgroupSubtreeControlPath).split("\\s+"))
              .collect(Collectors.toSet());
      log.log(
          Level.FINEST,
          String.format(
              "Existing sub_tree controllers under %s: %s",
              cgroupSubtreeControlPath, alreadyEnabledControllers));
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
    if (VERSION == CGroupVersion.CGROUPS_V2 && !root.isEmpty(controllerName)) {
      Group evacuation = root.getChild(EVACUATION_CGROUP_NAME);
      log.log(
          Level.FINE,
          "beginning evacuation of root cgroup for cgroups v2 from "
              + rootPath
              + " to "
              + evacuation.getPath());
      if (Files.exists(evacuation.getPath(controllerName))) {
        // Clean it up.
        Files.delete(evacuation.getPath(controllerName));
      }
      Files.createDirectories(evacuation.getPath(controllerName));
      evacuation.adoptPids(root.getPids());
      verify(root.isEmpty(), "tried to evacuate root cgroup but there were processes remaining");
      ensureControllerIsEnabled(
          root.getPath(controllerName), REQUIRED_CGROUP_CONTROLLER_NAMES, true);
    }
    if (parent != null) {
      parent.create(controllerName);
      if (VERSION == CGroupVersion.CGROUPS_V1) {
        Path path = getPath(controllerName);
        try {
          if (!Files.exists(path)) {
            Files.createDirectories(path);
          }
        } catch (FileAlreadyExistsException e) {
          // per the avoidance above, we don't care that this already
          // exists and we lost a race to create it
        }
      } else if (VERSION == CGroupVersion.CGROUPS_V2) {
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

  /** Shutdown hook. If there were cgroups things done earlier, un-do them now. */
  public static void onShutdown() {
    if (VERSION == CGroupVersion.CGROUPS_V2) {
      try {
        Group evacuation = root.getChild(EVACUATION_CGROUP_NAME);
        if (evacuation.isEmpty()) {
          // nothing to move back.
          return;
        }
        gatherAllCgroupChildProcesses(rootPath);
      } catch (IOException ioe) {
        throw new RuntimeException("Could not clean-up cgroups v2", ioe);
      }
    }
  }

  private static void disableSubtreeControl(Path cgroupPath) throws IOException {
    Path subtreeControl = cgroupPath.resolve(CGROUP_SUBTREE_CONTROL);
    log.log(Level.FINE, String.format("Disable subtree control at %s", subtreeControl));
    try (Writer out = new OutputStreamWriter(Files.newOutputStream(subtreeControl))) {
      for (String eachControllerName : REQUIRED_CGROUP_CONTROLLER_NAMES) {
        out.write(String.format("-%s ", eachControllerName));
      }
    }
  }

  /**
   * Bring all processes in all recursive cgroups into the cgroup at the given path
   *
   * <p>This also disabled cpu,memory subtree control for the given path.
   *
   * @param cgroupPath Cgroup to contain all children
   * @return <code>cgroupPath</code>
   * @throws IOException on any cgroups i/o problems
   */
  private static Path gatherAllCgroupChildProcesses(Path cgroupPath) throws IOException {
    log.log(
        Level.FINE,
        String.format("homecoming - returning child processes to cgroup %s", cgroupPath));

    // Depth-first walk through all child cgroups first
    try (Stream<Path> childDirectories = Files.list(cgroupPath)) {
      childDirectories
          .filter(Files::isDirectory)
          .forEach(
              childPath -> {
                try {
                  // Recurse deeper. Depth-first gathering
                  gatherAllCgroupChildProcesses(childPath);

                  disableSubtreeControl(cgroupPath);
                  adoptPids(cgroupPath, getPids(childPath));
                  verify(
                      getPids(childPath).isEmpty(),
                      "evacuated but processes remain in cgroup %s",
                      childPath);

                  Files.delete(childPath);

                } catch (IOException e) {
                  log.log(
                      Level.WARNING, "Failed to move processes from child cgroup: " + childPath, e);
                }
              });
    }
    // This may have been done above, when looping through the child directories. If this is a leaf
    // directory, we need to disable subtree control here, too.
    disableSubtreeControl(cgroupPath);

    return cgroupPath;
  }
}
