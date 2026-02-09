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

import static com.google.common.base.Preconditions.checkState;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.UUID;

/**
 * A writer that atomically presents a target file only on successful close.
 *
 * <p>Writes are performed to a temporary file with a unique UUID suffix. On successful close(), the
 * target file is atomically replaced via hard link, and the temporary file is deleted.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * try (AtomicFileWriter atomicWriter = new AtomicFileWriter(target)) {
 *   writer.write("data");
 * } // Automatic atomic swap and cleanup on close
 * }</pre>
 *
 * Note: This resource must be told, prior to the close block, that it was successfully completed,
 * in order to accomplish the file being presented at the target. This is because there is no
 * ability in the AutoClosable to detect the exceptional state of closure. If success is not
 * indicated with an 'onSuccess' call at the time of the first close call, the temp file will be
 * deleted and no interaction with the target will occur.
 *
 * <p>No thread safety of onSuccess() and close() is guaranteed.
 */
public class AtomicFileWriter extends BufferedWriter {
  private final Path target;
  private final Path temp;
  private boolean closed = false;
  private boolean success = false;

  private static Path createSiblingRandomUUIDTemp(Path target) {
    String suffix = UUID.randomUUID().toString();
    String filename = target.getFileName().toString();
    return target.resolveSibling(filename + ".tmp." + suffix);
  }

  /**
   * Creates an AtomicFileWriter for the specified target path.
   *
   * @param target the final destination path for the file
   * @throws IOException if the temporary file cannot be created
   */
  public AtomicFileWriter(Path target) throws IOException {
    this(target, createSiblingRandomUUIDTemp(target));
  }

  private AtomicFileWriter(Path target, Path temp) throws IOException {
    super(Files.newBufferedWriter(temp));
    checkState(!target.equals(temp));
    this.target = target;
    this.temp = temp;
  }

  public void onSuccess() {
    success = true;
  }

  /**
   * Closes the writer and atomically replaces the target file.
   *
   * <p>This method:
   *
   * <ol>
   *   <li>Closes the BufferedWriter
   *   <li>Deletes the target file if it exists
   *   <li>Creates a hard link from temp file to target (atomic replacement)
   *   <li>Deletes the temporary file
   * </ol>
   *
   * The temporary file is always deleted, even if an error occurs during the atomic swap.
   *
   * @throws IOException if an error occurs during the atomic swap
   */
  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;

    try {
      // Close writer first
      super.close();
      // Only attempt atomic swap if writer closed successfully
      if (success) {
        replace();
      }
    } finally {
      Files.delete(temp);
    }
  }

  private void replace() throws IOException {
    // Delete target file (ignore if doesn't exist)
    try {
      Files.delete(target);
    } catch (NoSuchFileException e) {
      // Ignore - file may not exist
    }

    // Create hard link to atomically replace
    Files.createLink(target, temp);
  }
}
