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

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.logging.Level;
import lombok.extern.java.Log;

/**
 * An AutoCloseable writer that atomically writes to a file.
 *
 * <p>Writes are performed to a temporary file with a unique UUID suffix. On successful close(), the
 * target file is atomically replaced via hard link, and the temporary file is deleted.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * try (AtomicFileWriter atomicWriter = new AtomicFileWriter(targetPath)) {
 *   BufferedWriter writer = atomicWriter.getWriter();
 *   writer.write("data");
 * } // Automatic atomic swap and cleanup on close
 * }</pre>
 */
@Log
public class AtomicFileWriter implements AutoCloseable {
  private final Path targetPath;
  private final Path tempPath;
  private final BufferedWriter writer;
  private boolean closed = false;

  /**
   * Creates an AtomicFileWriter for the specified target path.
   *
   * @param targetPath the final destination path for the file
   * @throws IOException if the temporary file cannot be created
   */
  public AtomicFileWriter(Path targetPath) throws IOException {
    this.targetPath = targetPath;
    String suffix = UUID.randomUUID().toString();
    String filename = targetPath.getFileName().toString();
    this.tempPath = targetPath.resolveSibling(filename + ".tmp." + suffix);
    this.writer = Files.newBufferedWriter(tempPath);
  }

  /**
   * Returns the BufferedWriter for writing to the temporary file.
   *
   * @return the BufferedWriter
   */
  public BufferedWriter getWriter() {
    return writer;
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

    // Track exceptions to ensure cleanup happens
    IOException primaryException = null;

    // Close writer first
    try {
      writer.close();
    } catch (IOException e) {
      primaryException = e;
    }

    // Only attempt atomic swap if writer closed successfully
    if (primaryException == null) {
      try {
        // Delete target file (ignore if doesn't exist)
        try {
          Files.delete(targetPath);
        } catch (IOException e) {
          // Ignore - file may not exist
        }

        // Create hard link to atomically replace
        Files.createLink(targetPath, tempPath);
      } catch (IOException e) {
        primaryException = e;
      }
    }

    // Always try to delete temp file (aggressive cleanup)
    try {
      Files.delete(tempPath);
    } catch (IOException e) {
      // Log but don't suppress primary exception
      if (primaryException != null) {
        log.log(Level.FINE, "Failed to delete temporary file: " + tempPath, e);
      } else {
        log.log(Level.WARNING, "Failed to delete temporary file: " + tempPath, e);
      }
    }

    // Rethrow primary exception if any
    if (primaryException != null) {
      throw primaryException;
    }
  }
}
