// Copyright 2019 The Bazel Authors. All rights reserved.
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

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;

import com.google.common.util.concurrent.ListenableFuture;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Directories {
  private static final Logger logger = Logger.getLogger(Directories.class.getName());

  private Directories() {}

  public static ListenableFuture<Void> remove(Path path, ExecutorService service) {
    String suffix = UUID.randomUUID().toString();
    Path filename = path.getFileName();
    String tmpFilename = filename + ".tmp." + suffix;
    Path tmpPath = path.resolveSibling(tmpFilename);
    try {
      // rename must be synchronous to call
      Files.move(path, tmpPath);
    } catch (IOException e) {
      return immediateFailedFuture(e);
    }
    return listeningDecorator(service)
        .submit(
            () -> {
              try {
                remove(tmpPath);
              } catch (IOException e) {
                logger.log(Level.SEVERE, "error removing directory " + tmpPath, e);
              }
            },
            null);
  }

  public static void remove(Path directory) throws IOException {
    enableAllWriteAccess(directory);
    Files.walkFileTree(
        directory,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            Files.delete(file);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
            if (e == null) {
              Files.delete(dir);
              return FileVisitResult.CONTINUE;
            }
            // directory iteration failed
            throw e;
          }
        });
  }

  public static void disableAllWriteAccess(Path directory) throws IOException {
    Files.walkFileTree(
        directory,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            new File(file.toString()).setWritable(false);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
            if (e == null) {
              new File(dir.toString()).setWritable(false);
              return FileVisitResult.CONTINUE;
            }
            // directory iteration failed
            throw e;
          }
        });
  }

  public static void enableAllWriteAccess(Path directory) throws IOException {
    Files.walkFileTree(
        directory,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            new File(file.toString()).setWritable(true);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
            if (e == null) {
              new File(dir.toString()).setWritable(true);
              return FileVisitResult.CONTINUE;
            }
            // directory iteration failed
            throw e;
          }
        });
  }
}
