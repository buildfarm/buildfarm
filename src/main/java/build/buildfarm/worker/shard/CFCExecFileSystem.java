// Copyright 2017 The Bazel Authors. All rights reserved.
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

import static build.buildfarm.cas.CASFileCache.getInterruptiblyOrIOException;
import static build.buildfarm.common.IOUtils.readdir;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.util.concurrent.Executors.newWorkStealingPool;
import static java.util.concurrent.TimeUnit.MINUTES;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.buildfarm.cas.CASFileCache;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.Dirent;
import build.buildfarm.common.io.Directories;
import build.buildfarm.worker.OutputDirectory;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.UserPrincipal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

class CFCExecFileSystem implements ExecFileSystem {
  private static final Logger logger = Logger.getLogger(Worker.class.getName());

  private final Path root;
  private final CASFileCache fileCache;
  private final @Nullable UserPrincipal owner;
  private final boolean
      linkInputDirectories; // perform first-available non-output symlinking and retain directories
  // in cache
  private final Map<Path, Iterable<String>> rootInputFiles = new ConcurrentHashMap<>();
  private final Map<Path, Iterable<Digest>> rootInputDirectories = new ConcurrentHashMap<>();
  private final ExecutorService fetchService = newWorkStealingPool(128);
  private final ExecutorService removeDirectoryService;
  private final ExecutorService accessRecorder;
  private final long deadlineAfter;
  private final TimeUnit deadlineAfterUnits;

  CFCExecFileSystem(
      Path root,
      CASFileCache fileCache,
      @Nullable UserPrincipal owner,
      boolean linkInputDirectories,
      ExecutorService removeDirectoryService,
      ExecutorService accessRecorder,
      long deadlineAfter,
      TimeUnit deadlineAfterUnits) {
    this.root = root;
    this.fileCache = fileCache;
    this.owner = owner;
    this.linkInputDirectories = linkInputDirectories;
    this.removeDirectoryService = removeDirectoryService;
    this.accessRecorder = accessRecorder;
    this.deadlineAfter = deadlineAfter;
    this.deadlineAfterUnits = deadlineAfterUnits;
  }

  @Override
  public void start(Consumer<List<Digest>> onDigests, boolean skipLoad)
      throws IOException, InterruptedException {
    List<Dirent> dirents = null;
    try {
      dirents = readdir(root, /* followSymlinks= */ false);
    } catch (IOException e) {
      logger.log(Level.SEVERE, "error reading directory " + root.toString(), e);
    }

    ImmutableList.Builder<ListenableFuture<Void>> removeDirectoryFutures = ImmutableList.builder();

    // only valid path under root is cache
    for (Dirent dirent : dirents) {
      String name = dirent.getName();
      Path child = root.resolve(name);
      if (!child.equals(fileCache.getRoot())) {
        removeDirectoryFutures.add(Directories.remove(root.resolve(name), removeDirectoryService));
      }
    }

    ImmutableList.Builder<Digest> blobDigests = ImmutableList.builder();
    fileCache.start(
        digest -> {
          synchronized (blobDigests) {
            blobDigests.add(digest);
          }
        },
        removeDirectoryService,
        skipLoad);
    onDigests.accept(blobDigests.build());

    getInterruptiblyOrIOException(allAsList(removeDirectoryFutures.build()));
  }

  @Override
  public void stop() {
    if (!shutdownAndAwaitTermination(fetchService, 1, MINUTES)) {
      logger.log(Level.SEVERE, "could not terminate fetchService");
    }
    if (!shutdownAndAwaitTermination(removeDirectoryService, 1, MINUTES)) {
      logger.log(Level.SEVERE, "could not terminate removeDirectoryService");
    }
    if (!shutdownAndAwaitTermination(accessRecorder, 1, MINUTES)) {
      logger.log(Level.SEVERE, "could not terminate accessRecorder");
    }
  }

  @Override
  public ContentAddressableStorage getStorage() {
    return fileCache;
  }

  @Override
  public InputStream newInput(Digest digest, long offset) throws IOException, InterruptedException {
    return fileCache.newInput(digest, offset);
  }

  private ListenableFuture<Void> put(
      Path path, FileNode fileNode, ImmutableList.Builder<String> inputFiles) {
    Path filePath = path.resolve(fileNode.getName());
    Digest digest = fileNode.getDigest();
    if (digest.getSizeBytes() == 0) {
      return listeningDecorator(fetchService)
          .submit(
              () -> {
                Files.createFile(filePath);
                // ignore executable
                return null;
              });
    }
    String key = fileCache.getKey(digest, fileNode.getIsExecutable());
    return transformAsync(
        fileCache.put(digest, fileNode.getIsExecutable(), fetchService),
        (fileCachePath) -> {
          checkNotNull(key);
          // we saw null entries in the built immutable list without synchronization
          synchronized (inputFiles) {
            inputFiles.add(key);
          }
          if (fileNode.getDigest().getSizeBytes() != 0) {
            try {
              Files.createLink(filePath, fileCachePath);
            } catch (IOException e) {
              return immediateFailedFuture(e);
            }
          }
          return immediateFuture(null);
        },
        fetchService);
  }

  private Iterable<ListenableFuture<Void>> fetchInputs(
      Path path,
      Digest directoryDigest,
      Map<Digest, Directory> directoriesIndex,
      OutputDirectory outputDirectory,
      ImmutableList.Builder<String> inputFiles,
      ImmutableList.Builder<Digest> inputDirectories)
      throws IOException {
    Directory directory = directoriesIndex.get(directoryDigest);
    if (directory == null) {
      // not quite IO...
      throw new IOException(
          "Directory " + DigestUtil.toString(directoryDigest) + " is not in directories index");
    }

    Iterable<ListenableFuture<Void>> downloads =
        directory.getFilesList().stream()
            .map((fileNode) -> put(path, fileNode, inputFiles))
            .collect(ImmutableList.toImmutableList());

    for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
      Digest digest = directoryNode.getDigest();
      String name = directoryNode.getName();
      OutputDirectory childOutputDirectory =
          outputDirectory != null ? outputDirectory.getChild(name) : null;
      Path dirPath = path.resolve(name);
      if (childOutputDirectory != null || !linkInputDirectories || name.equals("external")) {
        Files.createDirectories(dirPath);
        downloads =
            concat(
                downloads,
                fetchInputs(
                    dirPath,
                    digest,
                    directoriesIndex,
                    childOutputDirectory,
                    inputFiles,
                    inputDirectories));
      } else {
        downloads =
            concat(
                downloads,
                ImmutableList.of(
                    transform(
                        linkDirectory(dirPath, digest, directoriesIndex),
                        (result) -> {
                          // we saw null entries in the built immutable list without synchronization
                          synchronized (inputDirectories) {
                            inputDirectories.add(digest);
                          }
                          return null;
                        },
                        fetchService)));
      }
      if (Thread.currentThread().isInterrupted()) {
        break;
      }
    }
    return downloads;
  }

  private ListenableFuture<Void> linkDirectory(
      Path execPath, Digest digest, Map<Digest, Directory> directoriesIndex) {
    return transformAsync(
        fileCache.putDirectory(digest, directoriesIndex, fetchService),
        (cachePath) -> {
          Files.createSymbolicLink(execPath, cachePath);
          return immediateFuture(null);
        },
        fetchService);
  }

  private static class ExecDirException extends IOException {
    private final Path path;
    private final List<Throwable> exceptions;

    ExecDirException(Path path, List<Throwable> exceptions) {
      super(String.format("%s: %d exceptions", path, exceptions.size()));
      this.path = path;
      this.exceptions = exceptions;
      for (Throwable exception : exceptions) {
        addSuppressed(exception);
      }
    }

    Path getPath() {
      return path;
    }

    List<Throwable> getExceptions() {
      return exceptions;
    }
  }

  private static void checkExecErrors(Path path, List<Throwable> errors) throws ExecDirException {
    if (!errors.isEmpty()) {
      throw new ExecDirException(path, errors);
    }
  }

  @Override
  public Path createExecDir(
      String operationName, Map<Digest, Directory> directoriesIndex, Action action, Command command)
      throws IOException, InterruptedException {
    OutputDirectory outputDirectory =
        OutputDirectory.parse(
            command.getOutputFilesList(),
            command.getOutputDirectoriesList(),
            command.getEnvironmentVariablesList());

    Path execDir = root.resolve(operationName);
    if (Files.exists(execDir)) {
      Directories.remove(execDir);
    }
    Files.createDirectories(execDir);

    ImmutableList.Builder<String> inputFiles = new ImmutableList.Builder<>();
    ImmutableList.Builder<Digest> inputDirectories = new ImmutableList.Builder<>();

    logger.log(
        Level.INFO, "ExecFileSystem::createExecDir(" + operationName + ") calling fetchInputs");
    Iterable<ListenableFuture<Void>> fetchedFutures =
        fetchInputs(
            execDir,
            action.getInputRootDigest(),
            directoriesIndex,
            outputDirectory,
            inputFiles,
            inputDirectories);
    boolean success = false;
    try {
      InterruptedException exception = null;
      boolean wasInterrupted = false;
      ImmutableList.Builder<Throwable> exceptions = ImmutableList.builder();
      for (ListenableFuture<Void> fetchedFuture : fetchedFutures) {
        if (exception != null || wasInterrupted) {
          fetchedFuture.cancel(true);
        } else {
          try {
            fetchedFuture.get();
          } catch (ExecutionException e) {
            // just to ensure that no other code can react to interrupt status
            exceptions.add(e.getCause());
          } catch (InterruptedException e) {
            fetchedFuture.cancel(true);
            exception = e;
          }
        }
        wasInterrupted = Thread.interrupted() || wasInterrupted;
      }
      if (wasInterrupted) {
        Thread.currentThread().interrupt();
        // unlikely, but worth guarding
        if (exception == null) {
          exception = new InterruptedException();
        }
      }
      if (exception != null) {
        throw exception;
      }
      checkExecErrors(execDir, exceptions.build());
      success = true;
    } finally {
      if (!success) {
        fileCache.decrementReferences(inputFiles.build(), inputDirectories.build());
        Directories.remove(execDir);
      }
    }

    rootInputFiles.put(execDir, inputFiles.build());
    rootInputDirectories.put(execDir, inputDirectories.build());

    logger.log(
        Level.INFO,
        "ExecFileSystem::createExecDir(" + operationName + ") stamping output directories");
    boolean stamped = false;
    try {
      outputDirectory.stamp(execDir);
      stamped = true;
    } finally {
      if (!stamped) {
        destroyExecDir(execDir);
      }
    }
    if (owner != null) {
      Directories.setAllOwner(execDir, owner);
    }
    return execDir;
  }

  @Override
  public void destroyExecDir(Path execDir) throws IOException, InterruptedException {
    Iterable<String> inputFiles = rootInputFiles.remove(execDir);
    Iterable<Digest> inputDirectories = rootInputDirectories.remove(execDir);
    if (inputFiles != null || inputDirectories != null) {
      fileCache.decrementReferences(
          inputFiles == null ? ImmutableList.of() : inputFiles,
          inputDirectories == null ? ImmutableList.of() : inputDirectories);
    }
    if (Files.exists(execDir)) {
      Directories.remove(execDir);
    }
  }
}
