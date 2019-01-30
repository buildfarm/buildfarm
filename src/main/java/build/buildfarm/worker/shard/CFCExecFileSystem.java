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

import static build.buildfarm.worker.CASFileCache.getInterruptiblyOrIOException;
import static build.buildfarm.worker.Utils.readdir;
import static build.buildfarm.worker.Utils.removeDirectory;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.catchingAsync;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.logging.Level.SEVERE;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.worker.CASFileCache;
import build.buildfarm.worker.Dirent;
import build.buildfarm.worker.OutputDirectory;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.logging.Logger;

class CFCExecFileSystem implements ExecFileSystem {
  private static final Logger logger = Logger.getLogger(Worker.class.getName());

  private final Path root;
  private final CASFileCache fileCache;
  private final boolean linkInputDirectories; // perform first-available non-output symlinking and retain directories in cache
  private final Map<Path, Iterable<Path>> rootInputFiles = new ConcurrentHashMap<>();
  private final Map<Path, Iterable<Digest>> rootInputDirectories = new ConcurrentHashMap<>();
  private final ExecutorService fetchService = newCachedThreadPool();
  private final ExecutorService removeDirectoryService;

  CFCExecFileSystem(
      Path root,
      CASFileCache fileCache,
      boolean linkInputDirectories,
      ExecutorService removeDirectoryService) {
    this.root = root;
    this.fileCache = fileCache;
    this.linkInputDirectories = linkInputDirectories;
    this.removeDirectoryService = removeDirectoryService;
  }

  @Override
  public void start(Consumer<List<Digest>> onDigests) throws IOException, InterruptedException {
    List<Dirent> dirents = null;
    try {
      dirents = readdir(root, /* followSymlinks= */ false);
    } catch (IOException e) {
      logger.log(SEVERE, "error reading directory " + root.toString(), e);
    }

    ImmutableList.Builder<ListenableFuture<Void>> removeDirectoryFutures = ImmutableList.builder();

    // only valid path under root is cache
    for (Dirent dirent : dirents) {
      String name = dirent.getName();
      Path child = root.resolve(name);
      if (!child.equals(fileCache.getRoot())) {
        removeDirectoryFutures.add(removeDirectory(root.resolve(name), removeDirectoryService));
      }
    }

    ImmutableList.Builder<Digest> blobDigests = ImmutableList.builder();
    fileCache.start(blobDigests::add, removeDirectoryService);
    onDigests.accept(blobDigests.build());

    getInterruptiblyOrIOException(allAsList(removeDirectoryFutures.build()));
  }

  @Override
  public void stop() {
    fetchService.shutdown();
    while (!fetchService.isTerminated()) {
      try {
        if (!fetchService.awaitTermination(1, MINUTES)) {
          fetchService.shutdownNow();
        }
      } catch (InterruptedException e) {
        fetchService.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
    removeDirectoryService.shutdown();
    while (!removeDirectoryService.isTerminated()) {
      try {
        if (!removeDirectoryService.awaitTermination(1, MINUTES)) {
          removeDirectoryService.shutdownNow();
        }
      } catch (InterruptedException e) {
        removeDirectoryService.shutdownNow();
        Thread.currentThread().interrupt();
      }
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

  @Override
  public OutputStream newOutput(Digest digest) throws IOException {
    return fileCache.newOutput(digest);
  }

  private ListenableFuture<Void> put(
      Path path,
      FileNode fileNode,
      ImmutableList.Builder<Path> inputFiles) {
    Path filePath = path.resolve(fileNode.getName());
    Digest digest = fileNode.getDigest();
    if (digest.getSizeBytes() == 0) {
      return listeningDecorator(fetchService).submit(() -> {
        Files.createFile(filePath);
        // ignore executable
        return null;
      });
    }
    return transformAsync(
        fileCache.put(
            digest,
            fileNode.getIsExecutable(),
            /* containingDirectory=*/ null,
            fetchService),
        (fileCacheKey) -> {
          checkNotNull(fileCacheKey);
          // we saw null entries in the built immutable list without synchronization
          synchronized (inputFiles) {
            inputFiles.add(fileCacheKey);
          }
          if (fileNode.getDigest().getSizeBytes() != 0) {
            try {
              Files.createLink(filePath, fileCacheKey);
            } catch (IOException e) {
              return immediateFailedFuture(e);
            }
          }
          return immediateFuture((Void) null);
        },
        fetchService);
  }

  private Iterable<ListenableFuture<Void>> fetchInputs(
      Path path,
      Digest directoryDigest,
      Map<Digest, Directory> directoriesIndex,
      OutputDirectory outputDirectory,
      ImmutableList.Builder<Path> inputFiles,
      ImmutableList.Builder<Digest> inputDirectories)
      throws IOException, InterruptedException {
    Directory directory = directoriesIndex.get(directoryDigest);
    if (directory == null) {
      throw new IOException("Directory " + DigestUtil.toString(directoryDigest) + " is not in directories index");
    }

    Iterable<ListenableFuture<Void>> downloads = directory.getFilesList()
        .stream()
        .map((fileNode) -> put(path, fileNode, inputFiles))
        .collect(ImmutableList.<ListenableFuture<Void>>toImmutableList());

    for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
      Digest digest = directoryNode.getDigest();
      String name = directoryNode.getName();
      OutputDirectory childOutputDirectory = outputDirectory != null
          ? outputDirectory.getChild(name) : null;
      Path dirPath = path.resolve(name);
      if (childOutputDirectory != null || !linkInputDirectories) {
        Files.createDirectories(dirPath);
        downloads = concat(downloads, fetchInputs(dirPath, digest, directoriesIndex, childOutputDirectory, inputFiles, inputDirectories));
      } else {
        downloads = concat(downloads, ImmutableList.of(transform(
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
    }
    return downloads;
  }

  private ListenableFuture<Void> linkDirectory(
      Path execPath,
      Digest digest,
      Map<Digest, Directory> directoriesIndex) {
    return transformAsync(
        fileCache.putDirectory(digest, directoriesIndex, fetchService),
        (cachePath) -> {
          Files.createSymbolicLink(execPath, cachePath);
          return immediateFuture(null);
        },
        fetchService);
  }

  @Override
  public Path createExecDir(String operationName, Map<Digest, Directory> directoriesIndex, Action action, Command command) throws IOException, InterruptedException {
    OutputDirectory outputDirectory = OutputDirectory.parse(
        command.getOutputFilesList(),
        command.getOutputDirectoriesList());

    Path execDir = root.resolve(operationName);
    if (Files.exists(execDir)) {
      removeDirectory(execDir);
    }
    Files.createDirectories(execDir);

    ImmutableList.Builder<Path> inputFiles = new ImmutableList.Builder<>();
    ImmutableList.Builder<Digest> inputDirectories = new ImmutableList.Builder<>();

    logger.info("ExecFileSystem::createExecDir(" + DigestUtil.toString(action.getInputRootDigest()) + ") calling fetchInputs");
    ListenableFuture<List<Void>> fetchedFuture = allAsList(
        fetchInputs(
            execDir,
            action.getInputRootDigest(),
            directoriesIndex,
            outputDirectory,
            inputFiles,
            inputDirectories));
    boolean completed = false;
    try {
      getInterruptiblyOrIOException(fetchedFuture);
      completed = true;
    } finally {
      if (!completed) {
        fetchedFuture.cancel(true);
        fileCache.decrementReferences(inputFiles.build(), inputDirectories.build());
        removeDirectory(execDir);
      }
    }

    rootInputFiles.put(execDir, inputFiles.build());
    rootInputDirectories.put(execDir, inputDirectories.build());

    logger.info("ExecFileSystem::createExecDir(" + DigestUtil.toString(action.getInputRootDigest()) + ") stamping output directories");
    try {
      outputDirectory.stamp(execDir);
    } catch (IOException e) {
      destroyExecDir(execDir);
      throw e;
    }
    return execDir;
  }

  @Override
  public void destroyExecDir(Path execDir) throws IOException, InterruptedException {
    Iterable<Path> inputFiles = rootInputFiles.remove(execDir);
    Iterable<Digest> inputDirectories = rootInputDirectories.remove(execDir);
    if (inputFiles != null || inputDirectories != null) {
      fileCache.decrementReferences(
          inputFiles == null ? ImmutableList.of() : inputFiles,
          inputDirectories == null ? ImmutableList.of() : inputDirectories);
    }
    if (Files.exists(execDir)) {
      removeDirectory(execDir);
    }
  }
}
