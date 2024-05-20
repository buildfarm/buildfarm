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

package build.buildfarm.worker;

import static build.buildfarm.common.io.Utils.getInterruptiblyOrIOException;
import static build.buildfarm.common.io.Utils.readdir;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.catchingAsync;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
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
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.cfc.CASFileCache;
import build.buildfarm.common.io.Directories;
import build.buildfarm.common.io.Dirent;
import build.buildfarm.worker.ExecDirException.ViolationException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
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
public class CFCExecFileSystem implements ExecFileSystem {
  private final Path root;
  protected final CASFileCache fileCache;
  protected final @Nullable UserPrincipal owner;

  // permit symlinks to point to absolute paths in inputs
  private final boolean allowSymlinkTargetAbsolute;

  protected final ExecutorService fetchService;
  protected final ExecutorService removeDirectoryService;
  private final ExecutorService accessRecorder;
  protected FileStore fileStore; // initialized with start

  public CFCExecFileSystem(
      Path root,
      CASFileCache fileCache,
      @Nullable UserPrincipal owner,
      boolean allowSymlinkTargetAbsolute,
      ExecutorService removeDirectoryService,
      ExecutorService accessRecorder,
      ExecutorService fetchService) {
    this.root = root;
    this.fileCache = fileCache;
    this.owner = owner;
    this.allowSymlinkTargetAbsolute = allowSymlinkTargetAbsolute;
    this.removeDirectoryService = removeDirectoryService;
    this.accessRecorder = accessRecorder;
    this.fetchService = fetchService;
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
  public InputStream newInput(Compressor.Value compressor, Digest digest, long offset)
      throws IOException {
    return fileCache.newInput(compressor, digest, offset);
  }

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
  private ListenableFuture<Void> put(Digest digest, Path path, boolean isExecutable) {
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
          if (digest.getSizeBytes() != 0) {
            try {
              Files.copy(fileCachePath, path);
            } catch (IOException e) {
              return immediateFailedFuture(e);
            } finally {
              fileCache.decrementReference(key);
            }
          }
          return immediateFuture(null);
        },
        fetchService);
  }

  private ListenableFuture<Void> catchingPut(
      Digest digest, Path root, Path path, boolean isExecutable) {
    return catching(
        put(digest, path, isExecutable),
        e -> new ViolationException(digest, root.relativize(path), isExecutable, e));
  }

  protected ListenableFuture<Void> catching(
      ListenableFuture<Void> future, Function<IOException, Throwable> onIOError) {
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

    Iterable<ListenableFuture<Void>> futures() {
      return futures;
    }

    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
        throws IOException {
      if (root == null) {
        root = dir;
      }
      Files.createDirectories(dir);

      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
      ListenableFuture<Void> populate;
      boolean terminate = false;
      if (attrs.isSymbolicLink()) {
        ExecSymlinkAttributes symlinkAttrs = (ExecSymlinkAttributes) attrs;
        populate = putSymlink(file, symlinkAttrs.target());
      } else if (attrs.isRegularFile()) {
        Digest digest = (Digest) attrs.fileKey();
        ExecFileAttributes fileAttrs = (ExecFileAttributes) attrs;
        populate = catchingPut(digest, root, file, fileAttrs.isExecutable());
      } else {
        populate = immediateFailedFuture(new IOException("unknown file type for " + file));
        terminate = true;
      }
      futures.add(populate);
      return terminate ? FileVisitResult.TERMINATE : FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc) {
      return FileVisitResult.CONTINUE;
    }
  }

  @Override
  public Path createExecDir(
      String operationName, Map<Digest, Directory> directoriesIndex, Action action, Command command)
      throws IOException, InterruptedException {
    OutputDirectory outputDirectory = createOutputDirectory(command);

    Path execDir = root.resolve(operationName);
    checkState(!Files.exists(execDir));

    log.log(Level.FINER, operationName + " walking execTree");
    ExecTree execTree = new ExecTree(directoriesIndex);
    ExecFileVisitor visitor = new ExecFileVisitor();
    execTree.walk(execDir, action.getInputRootDigest(), visitor);

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
