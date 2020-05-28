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

package build.buildfarm.worker;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;

import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Platform.Property;
import build.buildfarm.common.FileStatus;
import build.buildfarm.common.IOUtils;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.File;
import java.io.FileNotFoundException;
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

public class Utils {
  private static final Logger logger = Logger.getLogger(Utils.class.getName());

  private Utils() {}

  public static ListenableFuture<Void> removeDirectory(Path path, ExecutorService service) {
    String suffix = UUID.randomUUID().toString();
    Path filename = path.getFileName();
    String tmpFilename = filename + ".tmp." + suffix;
    Path tmpPath = path.resolveSibling(tmpFilename);
    try {
      // rename must be synchronous to call
      enableAllWriteAccess(path);
      Files.move(path, tmpPath);
    } catch (IOException e) {
      return immediateFailedFuture(e);
    }
    return listeningDecorator(service)
        .submit(
            () -> {
              try {
                removeDirectory(tmpPath);
              } catch (IOException e) {
                logger.log(Level.SEVERE, "error removing directory " + tmpPath, e);
              }
            },
            null);
  }

  public static void removeDirectory(Path directory) throws IOException {
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

  public static FileStatus statIfFound(Path path, boolean followSymlinks) {
    try {
      return IOUtils.stat(path, followSymlinks);
    } catch (FileNotFoundException e) {
      return null;
    } catch (IOException e) {
      // If this codepath is ever hit, then this method should be rewritten to properly distinguish
      // between not-found exceptions and others.
      throw new IllegalStateException(e);
    }
  }

  static int commandCoreValue(boolean onlyMulticoreTests, String name, Command command) {
    int cores = -1;
    for (Property property : command.getPlatform().getPropertiesList()) {
      if (property.getName().equals(name)) {
        cores = Integer.parseInt(property.getValue());
        if (cores > 1 && onlyMulticoreTests && !commandIsTest(command)) {
          cores = 1;
        }
      }
    }
    return cores;
  }

  public static int commandMaxCores(boolean onlyMulticoreTests, Command command) {
    return commandCoreValue(onlyMulticoreTests, "max-cores", command);
  }

  public static int commandMinCores(boolean onlyMulticoreTests, Command command) {
    return commandCoreValue(onlyMulticoreTests, "min-cores", command);
  }

  static boolean commandIsTest(Command command) {
    // only tests are setting this currently - other mechanisms are unreliable
    return Iterables.any(
        command.getEnvironmentVariablesList(),
        (envVar) -> envVar.getName().equals("XML_OUTPUT_FILE"));
  }
}
