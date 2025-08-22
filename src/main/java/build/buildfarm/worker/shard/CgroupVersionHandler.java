// Copyright 2025 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.worker.shard;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles cgroup operations for both legacy v1 and modern v2 hierarchies This class provides a
 * fallback for when the existing Group class fails
 */
public class CgroupVersionHandler {
  private static final Logger logger = Logger.getLogger(CgroupVersionHandler.class.getName());

  public enum CgroupVersion {
    VERSION_1,
    VERSION_2
  }

  private final CgroupVersion version;
  private final Path cgroupMountPoint;

  public CgroupVersionHandler() {
    this.cgroupMountPoint = Paths.get("/sys/fs/cgroup");
    this.version = detectCgroupVersion();
    logger.info("Detected cgroup version: " + version);
  }

  private CgroupVersion detectCgroupVersion() {
    try {
      // Look for v2 unified hierarchy markers
      if (Files.exists(cgroupMountPoint.resolve("cgroup.controllers"))
          && Files.exists(cgroupMountPoint.resolve("cgroup.procs"))) {
        return CgroupVersion.VERSION_2;
      }

      // Look for v1 separate controller hierarchies
      if (Files.exists(cgroupMountPoint.resolve("cpu"))
          || Files.exists(cgroupMountPoint.resolve("memory"))) {
        return CgroupVersion.VERSION_1;
      }

      // Default assumption
      return CgroupVersion.VERSION_2;
    } catch (Exception e) {
      logger.log(Level.WARNING, "Error detecting cgroup version, assuming v2", e);
      return CgroupVersion.VERSION_2;
    }
  }

  public boolean moveProcessToCgroup(long processId, String cgroupHierarchyPath) {
    try {
      switch (version) {
        case VERSION_1:
          return handleV1ProcessMove(processId, cgroupHierarchyPath);
        case VERSION_2:
          return handleV2ProcessMove(processId, cgroupHierarchyPath);
        default:
          logger.warning("Unknown cgroup version: " + version);
          return false;
      }
    } catch (IOException e) {
      logger.log(
          Level.WARNING,
          String.format("Failed to move process %d to cgroup %s", processId, cgroupHierarchyPath),
          e);
      return false;
    }
  }

  private boolean handleV2ProcessMove(long processId, String cgroupHierarchyPath)
      throws IOException {
    Path targetCgroupPath = cgroupMountPoint.resolve(cgroupHierarchyPath);

    // Ensure the cgroup directory exists
    if (!Files.exists(targetCgroupPath)) {
      Files.createDirectories(targetCgroupPath);
    }

    Path processControlFile = targetCgroupPath.resolve("cgroup.procs");
    if (!Files.exists(processControlFile)) {
      logger.warning("Process control file missing at: " + processControlFile);
      return false;
    }

    // Check if process still exists before attempting move
    Path procPath = Paths.get("/proc/" + processId);
    if (!Files.exists(procPath)) {
      logger.fine("Process " + processId + " no longer exists, skipping move");
      return true; // Not an error if process already exited
    }

    try (Writer out = new OutputStreamWriter(Files.newOutputStream(processControlFile))) {
      out.write(processId + "\n");
    } catch (IOException e) {
      // Handle the race condition where process exits between our check and the write
      if (e.getMessage() != null && e.getMessage().contains("No such process")) {
        logger.fine("Process " + processId + " exited during cgroup move, skipping");
        return true; // Not an error if process exited during the operation
      }
      throw e; // Re-throw other IOExceptions
    }

    logger.fine("Moved process " + processId + " to cgroup: " + cgroupHierarchyPath);
    return true;
  }

  private boolean handleV1ProcessMove(long processId, String cgroupHierarchyPath)
      throws IOException {
    // In v1, we need to handle each controller separately
    String[] availableControllers = {"cpu", "memory", "cpuset", "blkio"};
    boolean overallSuccess = true;

    for (String controller : availableControllers) {
      Path controllerPath = cgroupMountPoint.resolve(controller);
      if (!Files.exists(controllerPath)) {
        continue; // Skip unavailable controllers
      }

      Path targetCgroupPath = controllerPath.resolve(cgroupHierarchyPath);

      // Create the cgroup directory if needed - this is critical for v1
      if (!Files.exists(targetCgroupPath)) {
        try {
          Files.createDirectories(targetCgroupPath);
          logger.fine("Created cgroup directory: " + targetCgroupPath);
        } catch (IOException e) {
          logger.log(Level.WARNING, "Failed to create cgroup directory: " + targetCgroupPath, e);
          overallSuccess = false;
          continue;
        }
      }

      // Try cgroup.procs first, then fall back to tasks for older systems
      Path processControlFile = targetCgroupPath.resolve("cgroup.procs");
      if (!Files.exists(processControlFile)) {
        processControlFile = targetCgroupPath.resolve("tasks");
      }

      if (!Files.exists(processControlFile)) {
        logger.warning("No process control file found at: " + targetCgroupPath);
        overallSuccess = false;
        continue;
      }

      // Check if process still exists before attempting move
      Path procPath = Paths.get("/proc/" + processId);
      if (!Files.exists(procPath)) {
        logger.fine("Process " + processId + " no longer exists, skipping move to " + controller);
        continue; // Not an error if process already exited
      }

      try (Writer out = new OutputStreamWriter(Files.newOutputStream(processControlFile))) {
        out.write(processId + "\n");
        logger.fine(
            "Moved process " + processId + " to " + controller + " cgroup: " + cgroupHierarchyPath);
      } catch (IOException e) {
        if (e.getMessage() != null && e.getMessage().contains("No such process")) {
          logger.fine("Process " + processId + " exited during move to " + controller + " cgroup");
        } else {
          logger.log(
              Level.WARNING,
              String.format(
                  "Failed to move process %d to %s cgroup %s",
                  processId, controller, cgroupHierarchyPath),
              e);
          overallSuccess = false;
        }
      }
    }

    return overallSuccess;
  }

  public CgroupVersion getVersion() {
    return version;
  }
}
