// Copyright 2023 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.worker.persistent;

import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import lombok.extern.java.Log;

/**
 * Utility for concurrent move/copy of files Can be extended in the future to (sym)linking if we
 * need performance
 */
@Log
public final class FileAccessUtils {
  // singleton class with only static methods
  private FileAccessUtils() {}

  public static Path addPosixOwnerWrite(Path absPath) throws IOException {
    Set<PosixFilePermission> perms = Files.getPosixFilePermissions(absPath);

    ImmutableSet<PosixFilePermission> permsWithWrite =
        ImmutableSet.<PosixFilePermission>builder()
            .addAll(perms)
            .add(PosixFilePermission.OWNER_WRITE)
            .build();

    return Files.setAttribute(absPath, "posix:permissions", permsWithWrite);
  }

  private static final ConcurrentHashMap<Path, PathLock> fileLocks = new ConcurrentHashMap<>();

  // Used here as a simple lock for locking "files" (paths)
  private static final class PathLock {
    // Not used elsewhere
    private PathLock() {}
  }

  /**
   * Copies a file, creating necessary directories, replacing existing files. The resulting file is
   * set to be writeable, and we throw if we cannot set that. Thread-safe (within a process) against
   * writes to the same path.
   *
   * @param from
   * @param to
   * @throws IOException
   */
  public static void copyFile(Path from, Path to, @Nullable UserPrincipal owner)
      throws IOException {
    Path absTo = to.toAbsolutePath();
    log.finer("copyFile: " + from + " to " + absTo);
    if (!Files.exists(from)) {
      throw new IOException("copyFile: source file doesn't exist: " + from);
    }
    IOException ioException =
        writeFileSafe(
            to,
            owner,
            () -> {
              try {
                Files.copy(from, absTo, REPLACE_EXISTING, COPY_ATTRIBUTES);

                if (owner != null) {
                  Files.setOwner(absTo, owner);
                }

                addPosixOwnerWrite(absTo);
                return null;
              } catch (IOException e) {
                return new IOException("copyFile() could not set writeable: " + absTo, e);
              }
            });
    if (ioException != null) {
      throw ioException;
    }
  }

  /**
   * Moves a file, creating necessary directories, replacing existing files. The resulting file is
   * set to be writeable, and we throw if we cannot set that. Thread-safe against writes to the same
   * path.
   *
   * @param from
   * @param to
   * @throws IOException
   */
  public static void moveFile(Path from, Path to) throws IOException {
    Path absTo = to.toAbsolutePath();
    log.finer("moveFile: " + from + " to " + absTo);
    if (!Files.exists(from)) {
      throw new IOException("moveFile: source file doesn't exist: " + from);
    }
    IOException ioException =
        writeFileSafe(
            absTo,
            null,
            () -> {
              try {
                Files.move(from, absTo, REPLACE_EXISTING);
                addPosixOwnerWrite(absTo);
                return null;
              } catch (IOException e) {
                return new IOException("copyFile() could not set writeable: " + absTo, e);
              }
            });
    if (ioException != null) {
      throw ioException;
    }
  }

  /**
   * Deletes a file; Thread-safe against writes to the same path.
   *
   * @param toDelete
   * @throws IOException
   */
  public static void deleteFileIfExists(Path toDelete) throws IOException {
    Path absTo = toDelete.toAbsolutePath();
    PathLock toLock = fileLock(absTo);
    synchronized (toLock) {
      try {
        Files.deleteIfExists(absTo);
      } finally {
        fileLocks.remove(absTo);
      }
    }
  }

  /**
   * Create a directory and all necessary ancestor directories, setting the owner of each created
   * directory.
   */
  private static void createDirectoriesWithOwner(Path directory, @Nullable UserPrincipal owner)
      throws IOException {
    if (Files.exists(directory)) {
      return;
    }

    Path parent = directory.getParent();

    if (parent != null && !Files.exists(parent)) {
      createDirectoriesWithOwner(parent, owner);
    }

    try {
      Files.createDirectory(directory);

      if (owner != null) {
        Files.setOwner(directory, owner);
      }
    } catch (FileAlreadyExistsException ignored) {
    }
  }

  /**
   * Thread-safe (not multi-process-safe) wrapper for locking paths before a write operation.
   *
   * <p>This method will create necessary parent directories.
   *
   * <p>It is up to the write operation to specify whether or not to overwrite existing files.
   */
  @SuppressWarnings("PMD.UnnecessaryLocalBeforeReturn")
  private static IOException writeFileSafe(
      Path absTo, @Nullable UserPrincipal owner, Supplier<IOException> writeOp) {
    PathLock toLock = fileLock(absTo);
    synchronized (toLock) {
      try {
        // If 'absTo' is a symlink, checks if its target file exists
        createDirectoriesWithOwner(absTo.getParent(), owner);
        return writeOp.get();
      } catch (IOException e) {
        // PMD will complain about UnnecessaryLocalBeforeReturn
        // In this case, it is necessary to catch the exception
        return e;
      } finally {
        // Clean up to prevent too many locks.
        fileLocks.remove(absTo);
      }
    }
  }

  // "Logical" file lock
  private static PathLock fileLock(Path writeTo) {
    return fileLocks.computeIfAbsent(writeTo, k -> new PathLock());
  }
}
