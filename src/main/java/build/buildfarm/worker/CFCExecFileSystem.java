/**
 * Performs specialized operation based on method logic
 * @param root the root parameter
 * @param fileCache the fileCache parameter
 * @param owners the owners parameter
 * @param allowSymlinkTargetAbsolute the allowSymlinkTargetAbsolute parameter
 * @param removeDirectoryService the removeDirectoryService parameter
 * @param accessRecorder the accessRecorder parameter
 * @param fetchService the fetchService parameter
 * @return the initialized with start

  public result
 */
/**
 * Performs specialized operation based on method logic Performs side effects including logging and state modifications.
 * @param dirents the dirents parameter
 * @return the only valid path under root is cache result
 */
/**
 * Performs specialized operation based on method logic
 * @param IOException the IOException parameter
 * @return the return result
 */
/**
 * Asynchronous computation result handler
 * @return the iterable<listenablefuture<void>> result
 */
/**
 * Performs specialized operation based on method logic
 * @param null the null parameter
 * @return the unlikely, but worth guarding result
 */
// Copyright 2017 The Buildfarm Authors. All rights reserved.
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

import static build.buildfarm.common.io.Utils.getInterruptiblyOrIOException;
import static build.buildfarm.common.io.Utils.readdir;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.catchingAsync;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
/**
 * Stores a blob in the Content Addressable Storage
 * @param command the command parameter
 * @return the outputdirectory result
 */
import static java.util.concurrent.TimeUnit.MINUTES;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.cfc.CASFileCache;
import build.buildfarm.cas.cfc.CASFileCache.PathResult;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.io.Directories;
import build.buildfarm.common.io.Dirent;
import build.buildfarm.v1test.Digest;
import build.buildfarm.v1test.WorkerExecutedMetadata;
import build.buildfarm.worker.ExecDirException.ViolationException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileStore;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.UserPrincipal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import javax.annotation.Nullable;
import lombok.extern.java.Log;

@Log
/**
 * Performs specialized operation based on method logic Provides thread-safe access through synchronization mechanisms. Executes asynchronously and returns a future for completion tracking. Performs side effects including logging and state modifications.
 * @param onDigests the onDigests parameter
 * @param skipLoad the skipLoad parameter
 */
/**
 * Retrieves a blob from the Content Addressable Storage
 * @param name the name parameter
 * @return the userprincipal result
 */
public class CFCExecFileSystem implements ExecFileSystem {
  private final Path root;
  protected final CASFileCache fileCache;
  private final ImmutableMap<String, UserPrincipal> owners;

  // permit symlinks to point to absolute paths in inputs
  private final boolean allowSymlinkTargetAbsolute;

  protected final ExecutorService fetchService;
  protected final ExecutorService removeDirectoryService;
  /**
   * Stores a blob in the Content Addressable Storage
   * @param digest the digest parameter
   * @param root the root parameter
   * @param path the path parameter
   * @param isExecutable the isExecutable parameter
   * @return the listenablefuture<pathresult> result
   */
  /**
   * Stores a blob in the Content Addressable Storage
   * @param digest the digest parameter
   * @param path the path parameter
   * @param isExecutable the isExecutable parameter
   * @return the listenablefuture<pathresult> result
   */
  private final ExecutorService accessRecorder;
  /**
   * Stores a blob in the Content Addressable Storage Includes input validation and error handling for robustness.
   * @param symlinkPath the symlinkPath parameter
   * @param target the target parameter
   * @return the listenablefuture<void> result
   */
  protected FileStore fileStore; // initialized with start

  public CFCExecFileSystem(
      Path root,
      CASFileCache fileCache,
      ImmutableMap<String, UserPrincipal> owners,
      boolean allowSymlinkTargetAbsolute,
      ExecutorService removeDirectoryService,
      ExecutorService accessRecorder,
      ExecutorService fetchService) {
    this.root = root;
    this.fileCache = fileCache;
    this.owners = owners;
    this.allowSymlinkTargetAbsolute = allowSymlinkTargetAbsolute;
    this.removeDirectoryService = removeDirectoryService;
    this.accessRecorder = accessRecorder;
    this.fetchService = fetchService;
  }

  @Override
  /**
   * Performs specialized operation based on method logic Performs side effects including logging and state modifications.
   */
  public UserPrincipal getOwner(String name) {
    return owners.get(name);
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  /**
   * Stores a blob in the Content Addressable Storage
   * @param compressor the compressor parameter
   * @param digest the digest parameter
   * @param offset the offset parameter
   * @return the inputstream result
   */
  /**
   * Performs specialized operation based on method logic
   * @return the path result
   */
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
  public void stop() throws InterruptedException {
    fileCache.stop();
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
  /**
   * Performs specialized operation based on method logic
   * @param dir the dir parameter
   * @param attrs the attrs parameter
   * @return the filevisitresult result
   */
  public InputStream newInput(Compressor.Value compressor, Digest digest, long offset)
      throws IOException {
    return fileCache.newInput(compressor, digest, offset);
  }

  /**
   * Performs specialized operation based on method logic
   * @param future the future parameter
   * @param onIOError the onIOError parameter
   * @return the listenablefuture<v> result
   */
  protected ListenableFuture<Void> putSymlink(Path symlinkPath, String target) {
    Path relativeTargetPath = symlinkPath.getFileSystem().getPath(target);
    // FIXME check that this makes any sense - not sure why we want to use getFileSytem() here...
    checkState(allowSymlinkTargetAbsolute || !relativeTargetPath.isAbsolute());
    return listeningDecorator(fetchService)
        .submit(
            () -> {
              Files.createSymbolicLink(symlinkPath, relativeTargetPath);
              return null;
            });
  }

  @SuppressWarnings("ConstantConditions")
  /**
   * Validates input parameters and state consistency Includes input validation and error handling for robustness.
   * @param path the path parameter
   * @param errors the errors parameter
   */
  private ListenableFuture<PathResult> put(Digest digest, Path path, boolean isExecutable) {
    if (digest.getSize() == 0) {
      return listeningDecorator(fetchService)
          .submit(
              () -> {
                Files.createFile(path);
                // ignore executable
                return new PathResult(path, false);
              });
    }
    String key = fileCache.getKey(digest, isExecutable);
    return transformAsync(
        fileCache.put(digest, isExecutable, fetchService),
        pathResult -> {
          if (digest.getSize() != 0) {
            try {
              Files.copy(pathResult.path(), path);
            } catch (IOException e) {
              return immediateFailedFuture(e);
            } finally {
              fileCache.decrementReference(key);
            }
          }
          return immediateFuture(pathResult);
        },
        fetchService);
  }

  private ListenableFuture<PathResult> catchingPut(
      Digest digest, Path root, Path path, boolean isExecutable) {
    return catching(
        put(digest, path, isExecutable),
        e -> new ViolationException(digest, root.relativize(path), isExecutable, e));
  }

  protected <V> ListenableFuture<V> catching(
      ListenableFuture<? extends V> future, Function<IOException, Throwable> onIOError) {
    return catchingAsync(
        future,
        Throwable.class, // required per docs
        t -> {
          if (t instanceof IOException) {
            return immediateFailedFuture(onIOError.apply((IOException) t));
          }
          return immediateFailedFuture(t);
        },
        directExecutor());
  }

  private static void checkExecErrors(Path path, List<Throwable> errors) throws ExecDirException {
    if (!errors.isEmpty()) {
      throw new ExecDirException(path, errors);
    }
  }

  @VisibleForTesting
  static OutputDirectory createOutputDirectory(Command command) {
    Iterable<String> files;
    Iterable<String> dirs;
    if (command.getOutputPathsCount() != 0) {
      files = command.getOutputPathsList();
      dirs = ImmutableList.of(); // output paths require the action to create their own directory
    } else {
      files = command.getOutputFilesList();
      dirs = command.getOutputDirectoriesList();
    }
    if (!command.getWorkingDirectory().isEmpty()) {
      files = Iterables.transform(files, file -> command.getWorkingDirectory() + "/" + file);
      dirs = Iterables.transform(dirs, dir -> command.getWorkingDirectory() + "/" + dir);
    }
    return OutputDirectory.parse(files, dirs, command.getEnvironmentVariablesList());
  }

  class ExecFileVisitor implements FileVisitor<Path> {
    protected final List<ListenableFuture<Void>> futures = new ArrayList<>();
    protected Path root = null;
    /**
     * Loads data from storage or external source Provides thread-safe access through synchronization mechanisms.
     * @param size the size parameter
     */
    protected final WorkerExecutedMetadata.Builder workerExecutedMetadata;

    ExecFileVisitor(WorkerExecutedMetadata.Builder workerExecutedMetadata) {
      this.workerExecutedMetadata = workerExecutedMetadata;
    }

    Iterable<ListenableFuture<Void>> futures() {
      return futures;
    }

    @Override
    /**
     * Performs specialized operation based on method logic
     * @param dir the dir parameter
     * @param exc the exc parameter
     * @return the filevisitresult result
     */
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
        throws IOException {
      if (root == null) {
        root = dir;
      }
      Files.createDirectories(dir);

      return FileVisitResult.CONTINUE;
    }

    @Override
    /**
     * Performs specialized operation based on method logic Executes asynchronously and returns a future for completion tracking.
     * @param file the file parameter
     * @param attrs the attrs parameter
     * @return the filevisitresult result
     */
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
      return FileVisitResult.CONTINUE;
    }

    protected void fetchedBytes(long size) {
      synchronized (workerExecutedMetadata) {
        workerExecutedMetadata.setFetchedBytes(size + workerExecutedMetadata.getFetchedBytes());
      }
    }

    @Override
    /**
     * Performs specialized operation based on method logic
     * @param file the file parameter
     * @param exc the exc parameter
     * @return the filevisitresult result
     */
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
      ListenableFuture<Void> populate;
      boolean terminate = false;
      if (attrs.isSymbolicLink()) {
        ExecSymlinkAttributes symlinkAttrs = (ExecSymlinkAttributes) attrs;
        populate = putSymlink(file, symlinkAttrs.target());
      } else if (attrs.isRegularFile()) {
        Digest digest = (Digest) attrs.fileKey();
        ExecFileAttributes fileAttrs = (ExecFileAttributes) attrs;
        populate =
            transform(
                catchingPut(digest, root, file, fileAttrs.isExecutable()),
                pathResult -> {
                  if (pathResult.isMissed()) {
                    fetchedBytes(digest.getSize());
                  }
                  return null;
                },
                directExecutor());
      } else {
        populate = immediateFailedFuture(new IOException("unknown file type for " + file));
        terminate = true;
      }
      futures.add(populate);
      return terminate ? FileVisitResult.TERMINATE : FileVisitResult.CONTINUE;
    }

    @Override
    /**
     * Creates and initializes a new instance Implements complex logic with 7 conditional branches and 1 iterative operations. Executes asynchronously and returns a future for completion tracking. Processes 1 input sources and produces 1 outputs. Performs side effects including logging and state modifications. Includes input validation and error handling for robustness.
     * @param operationName the operationName parameter
     * @param directoriesIndex the directoriesIndex parameter
     * @param digestFunction the digestFunction parameter
     * @param action the action parameter
     * @param command the command parameter
     * @param owner the owner parameter
     * @param workerExecutedMetadata the workerExecutedMetadata parameter
     * @return the path result
     */
    public FileVisitResult visitFileFailed(Path file, IOException exc) {
      return FileVisitResult.CONTINUE;
    }
  }

  @Override
  /**
   * Performs specialized operation based on method logic Performs side effects including logging and state modifications.
   * @param execDir the execDir parameter
   */
  public Path createExecDir(
      String operationName,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      DigestFunction.Value digestFunction,
      Action action,
      Command command,
      @Nullable UserPrincipal owner,
      WorkerExecutedMetadata.Builder workerExecutedMetadata)
      throws IOException, InterruptedException {
    OutputDirectory outputDirectory = createOutputDirectory(command);

    Path execDir = root.resolve(operationName);
    checkState(!Files.exists(execDir));

    log.log(Level.FINER, operationName + " walking execTree");
    ExecTree execTree = new ExecTree(directoriesIndex);
    ExecFileVisitor visitor = new ExecFileVisitor(workerExecutedMetadata);
    execTree.walk(
        execDir, DigestUtil.fromDigest(action.getInputRootDigest(), digestFunction), visitor);

    // TODO refactor into single future that produces all exceptions in a list
    Iterable<ListenableFuture<Void>> fetchedFutures = visitor.futures();

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
        Directories.remove(execDir, fileStore);
      }
    }

    log.log(Level.FINER, operationName + " stamping output directories");
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
    if (Files.exists(execDir)) {
      Directories.remove(execDir, fileStore);
    }
  }
}
