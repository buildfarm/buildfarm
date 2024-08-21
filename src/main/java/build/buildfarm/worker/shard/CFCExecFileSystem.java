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

import static build.buildfarm.common.io.Utils.getInterruptiblyOrIOException;
import static build.buildfarm.common.io.Utils.readdir;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.catchingAsync;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.util.concurrent.TimeUnit.MINUTES;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.SymlinkNode;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.cfc.CASFileCache;
import build.buildfarm.common.BuildfarmExecutors;
import build.buildfarm.common.io.Directories;
import build.buildfarm.common.io.Dirent;
import build.buildfarm.worker.ExecDirException;
import build.buildfarm.worker.ExecDirException.ViolationException;
import build.buildfarm.worker.OutputDirectory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.UserPrincipal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.logging.Level;
import javax.annotation.Nullable;
import lombok.extern.java.Log;

@Log
class CFCExecFileSystem implements ExecFileSystem {
  private final Path root;
  private final CASFileCache fileCache;
  private final @Nullable UserPrincipal owner;

  // perform first-available non-output symlinking and retain directories in cache
  private final boolean linkInputDirectories;

  // override the symlinking above for a set of matching paths
  private final Iterable<String> realInputDirectories;

  private final Map<Path, Iterable<String>> rootKeys = new ConcurrentHashMap<>();
  private final Map<Path, Iterable<String>> rootInputFiles = new ConcurrentHashMap<>();
  private final Map<Path, Iterable<Digest>> rootInputDirectories = new ConcurrentHashMap<>();
  private final ExecutorService fetchService = BuildfarmExecutors.getFetchServicePool();
  private final ExecutorService removeDirectoryService;
  private final ExecutorService accessRecorder;
  private FileStore fileStore; // initialized with start

  CFCExecFileSystem(
      Path root,
      CASFileCache fileCache,
      @Nullable UserPrincipal owner,
      boolean linkInputDirectories,
      Iterable<String> realInputDirectories,
      ExecutorService removeDirectoryService,
      ExecutorService accessRecorder) {
    this.root = root;
    this.fileCache = fileCache;
    this.owner = owner;
    this.linkInputDirectories = linkInputDirectories;
    this.realInputDirectories = realInputDirectories;
    this.removeDirectoryService = removeDirectoryService;
    this.accessRecorder = accessRecorder;
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public void start(Consumer<List<Digest>> onDigests, boolean skipLoad)
      throws IOException, InterruptedException {
    fileStore = Files.getFileStore(root);
    List<Dirent> dirents = null;
    try {
      dirents = readdir(root, /* followSymlinks= */ false, fileStore);
    } catch (IOException e) {
      log.log(Level.SEVERE, "error reading directory " + root.toString(), e);
    }

    ImmutableList.Builder<ListenableFuture<Void>> removeDirectoryFutures = ImmutableList.builder();

    // only valid path under root is cache
    for (Dirent dirent : dirents) {
      String name = dirent.getName();
      Path child = root.resolve(name);
      if (!child.equals(fileCache.getRoot())) {
        removeDirectoryFutures.add(
            Directories.remove(root.resolve(name), fileStore, removeDirectoryService));
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
      log.log(Level.SEVERE, "could not terminate fetchService");
    }
    if (!shutdownAndAwaitTermination(removeDirectoryService, 1, MINUTES)) {
      log.log(Level.SEVERE, "could not terminate removeDirectoryService");
    }
    if (!shutdownAndAwaitTermination(accessRecorder, 1, MINUTES)) {
      log.log(Level.SEVERE, "could not terminate accessRecorder");
    }
  }

  @Override
  public Path root() {
    return root;
  }

  @Override
  public ContentAddressableStorage getStorage() {
    return fileCache;
  }

  @Override
  public InputStream newInput(Compressor.Value compressor, Digest digest, long offset)
      throws IOException {
    return fileCache.newInput(compressor, digest, offset);
  }

  private ListenableFuture<Void> putSymlink(Path path, SymlinkNode symlinkNode) {
    Path symlinkPath = path.resolve(symlinkNode.getName());
    Path relativeTargetPath = path.getFileSystem().getPath(symlinkNode.getTarget());
    checkState(!relativeTargetPath.isAbsolute());
    return listeningDecorator(fetchService)
        .submit(
            () -> {
              Files.createSymbolicLink(symlinkPath, relativeTargetPath);
              return null;
            });
  }

  @SuppressWarnings("ConstantConditions")
  private ListenableFuture<Void> put(
      Digest digest, Path path, boolean isExecutable, Consumer<String> onKey) {
    if (digest.getSizeBytes() == 0) {
      return listeningDecorator(fetchService)
          .submit(
              () -> {
                Files.createFile(path);
                // ignore executable
                return null;
              });
    }
    String key = fileCache.getKey(digest, isExecutable);
    return transformAsync(
        fileCache.put(digest, isExecutable, fetchService),
        (fileCachePath) -> {
          checkNotNull(key);
          // we saw null entries in the built immutable list without synchronization
          onKey.accept(key);
          if (digest.getSizeBytes() != 0) {
            try {
              // Coordinated with the CAS - consider adding an API for safe path
              // access
              synchronized (fileCache) {
                Files.createLink(path, fileCachePath);
              }
            } catch (IOException e) {
              return immediateFailedFuture(e);
            }
          }
          return immediateFuture(null);
        },
        fetchService);
  }

  private ListenableFuture<Void> catchingPut(
      Digest digest, Path root, Path path, boolean isExecutable, Consumer<String> onKey) {
    return catchingAsync(
        put(digest, path, isExecutable, onKey),
        Throwable.class, // required per docs
        t -> {
          if (t instanceof IOException) {
            return immediateFailedFuture(
                new ViolationException(
                    digest, root.relativize(path), isExecutable, (IOException) t));
          }
          return immediateFailedFuture(t);
        },
        directExecutor());
  }

  private Iterable<ListenableFuture<Void>> fetchInputs(
      Path root,
      Path path,
      Digest directoryDigest,
      Map<Digest, Directory> directoriesIndex,
      OutputDirectory outputDirectory,
      Consumer<String> onKey,
      ImmutableList.Builder<Digest> inputDirectories)
      throws IOException {
    Directory directory = directoriesIndex.get(directoryDigest);
    checkNotNull(directory);
    Iterable<ListenableFuture<Void>> downloads =
        directory.getFilesList().stream()
            .map(
                fileNode ->
                    catchingPut(
                        fileNode.getDigest(),
                        root,
                        path.resolve(fileNode.getName()),
                        fileNode.getIsExecutable(),
                        onKey))
            .collect(ImmutableList.toImmutableList());
    downloads =
        concat(
            downloads,
            directory.getSymlinksList().stream()
                .map(symlinkNode -> putSymlink(path, symlinkNode))
                .collect(ImmutableList.toImmutableList()));

    for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
      Digest digest = directoryNode.getDigest();
      String name = directoryNode.getName();
      OutputDirectory childOutputDirectory =
          outputDirectory != null ? outputDirectory.getChild(name) : null;
      Path dirPath = path.resolve(name);
      if (childOutputDirectory != null || !linkInputDirectories) {
        Files.createDirectories(dirPath);
        downloads =
            concat(
                downloads,
                fetchInputs(
                    root,
                    dirPath,
                    digest,
                    directoriesIndex,
                    childOutputDirectory,
                    onKey,
                    inputDirectories));
      } else {
        downloads =
            concat(
                downloads,
                ImmutableList.of(
                    transform(
                        linkDirectory(dirPath, digest, directoriesIndex),
                        (result) -> {
                          // note: this could non-trivial make sync due to
                          // the way decrementReferences is implemented.
                          // we saw null entries in the built immutable list
                          // without synchronization
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

  @SuppressWarnings("ConstantConditions")
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

  private static void checkExecErrors(Path path, List<Throwable> errors) throws ExecDirException {
    if (!errors.isEmpty()) {
      throw new ExecDirException(path, errors);
    }
  }

  private static boolean treeContainsPath(
      String directoryPath, Map<Digest, Directory> directoriesIndex, Digest rootDigest) {
    Directory directory = directoriesIndex.get(rootDigest);
    for (String name : directoryPath.split("/")) {
      List<DirectoryNode> subdirs = directory.getDirectoriesList();
      int index = Collections.binarySearch(Lists.transform(subdirs, DirectoryNode::getName), name);
      if (index < 0) {
        return false;
      }
      directory = directoriesIndex.get(subdirs.get(index).getDigest());
    }
    return true;
  }

  private Iterable<String> realDirectories(
      Map<Digest, Directory> directoriesIndex, Digest rootDigest) {
    // skip this search if all the directories are real
    if (linkInputDirectories) {
      // somewhat inefficient, but would need many overrides to be painful
      return filter(
          realInputDirectories,
          realInputDirectory -> treeContainsPath(realInputDirectory, directoriesIndex, rootDigest));
    }
    return ImmutableList.of();
  }

  @Override
  public Path createExecDir(
      String operationName, Map<Digest, Directory> directoriesIndex, Action action, Command command)
      throws IOException, InterruptedException {
    log.log(Level.FINEST, "ExecFileSystem::createExecDir(" + operationName + ")");
    Digest inputRootDigest = action.getInputRootDigest();
    OutputDirectory outputDirectory =
        OutputDirectory.parse(
            command.getOutputFilesList(),
            concat(
                command.getOutputDirectoriesList(),
                realDirectories(directoriesIndex, inputRootDigest)),
            command.getEnvironmentVariablesList());

    Path execDir = root.resolve(operationName);
    if (Files.exists(execDir)) {
      Directories.remove(execDir, fileStore);
    }
    Files.createDirectories(execDir);

    ImmutableList.Builder<String> inputFiles = new ImmutableList.Builder<>();
    ImmutableList.Builder<Digest> inputDirectories = new ImmutableList.Builder<>();

    // Get lock keys so we can increment them prior to downloading
    // and no other threads can to create/delete during
    // eviction or the invocation of fetchInputs
    Iterable<String> lockedKeys =
        fileCache.lockDirectoryKeys(execDir, inputRootDigest, directoriesIndex);

    Iterable<ListenableFuture<Void>> fetchedFutures =
        fetchInputs(
            execDir,
            execDir,
            inputRootDigest,
            directoriesIndex,
            outputDirectory,
            key -> {
              synchronized (inputFiles) {
                inputFiles.add(key);
              }
            },
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
        log.log(Level.INFO, "Failed to create exec dir (" + operationName + "), cleaning up");
        fileCache.decrementReferences(inputFiles.build(), inputDirectories.build());
        fileCache.unlockKeys(lockedKeys);
        Directories.remove(execDir, fileStore);
      }
    }

    rootKeys.put(execDir, lockedKeys);
    rootInputFiles.put(execDir, inputFiles.build());
    rootInputDirectories.put(execDir, inputDirectories.build());

    log.log(
        Level.FINE,
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
    log.log(Level.FINEST, "ExecFileSystem::destroyExecDir(" + execDir + "");
    Iterable<String> lockedKeys = rootKeys.remove(execDir);
    Iterable<String> inputFiles = rootInputFiles.remove(execDir);
    Iterable<Digest> inputDirectories = rootInputDirectories.remove(execDir);
    if (inputFiles != null || inputDirectories != null) {
      fileCache.decrementReferences(
          inputFiles == null ? ImmutableList.of() : inputFiles,
          inputDirectories == null ? ImmutableList.of() : inputDirectories);
    }
    fileCache.unlockKeys(lockedKeys);
    if (Files.exists(execDir)) {
      Directories.remove(execDir, fileStore);
    }
  }
}
