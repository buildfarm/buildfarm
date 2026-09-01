// Copyright 2026 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.common.io;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Moves temporary files into their final location using a strategy fixed at startup. */
public final class FileSystemMover {
  private static final Logger log = Logger.getLogger(FileSystemMover.class.getName());

  @FunctionalInterface
  private interface Move {
    void apply(Path source, Path target) throws IOException;
  }

  private final Move move;

  private FileSystemMover(Move move) {
    this.move = move;
  }

  /**
   * Determines once whether the filesystem containing {@code directory} supports atomic moves.
   *
   * <p>The returned mover always uses the selected strategy. Filesystems without atomic-move
   * support use the legacy delete-and-hard-link sequence.
   */
  public static FileSystemMover probe(Path directory) throws IOException {
    Path source = Files.createTempFile(directory, ".atomic-move-probe-", ".tmp");
    Path target = Files.createTempFile(directory, ".atomic-move-probe-target-", ".tmp");
    try {
      Files.move(source, target, ATOMIC_MOVE, REPLACE_EXISTING);
      log.log(Level.INFO, "using atomic moves for " + directory);
      return atomicMover();
    } catch (AtomicMoveNotSupportedException e) {
      log.log(
          Level.WARNING,
          "atomic move not supported for "
              + directory
              + "; using non-atomic delete and hard-link replacement",
          e);
      return hardLinkMover();
    } finally {
      Files.deleteIfExists(source);
      Files.deleteIfExists(target);
    }
  }

  /** Replaces {@code target} with {@code source}. The source may remain for hard-link moves. */
  public void move(Path source, Path target) throws IOException {
    move.apply(source, target);
  }

  static FileSystemMover atomicMover() {
    return new FileSystemMover(
        (source, target) -> Files.move(source, target, ATOMIC_MOVE, REPLACE_EXISTING));
  }

  static FileSystemMover hardLinkMover() {
    return new FileSystemMover(
        (source, target) -> {
          Files.deleteIfExists(target);
          Files.createLink(target, source);
        });
  }
}
