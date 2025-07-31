/**
 * Performs specialized operation based on method logic
 * @param root the root parameter
 * @param fileCache the fileCache parameter
 * @param owners the owners parameter
 * @param linkInputDirectories the linkInputDirectories parameter
 * @param linkedInputDirectories the linkedInputDirectories parameter
 * @param allowSymlinkTargetAbsolute the allowSymlinkTargetAbsolute parameter
 * @param removeDirectoryService the removeDirectoryService parameter
 * @param accessRecorder the accessRecorder parameter
 * @param fetchService the fetchService parameter
 * @return the public result
 */
/**
 * Performs specialized operation based on method logic
 * @return the skip this search if all the directories are real result
 */
/**
 * Stores a blob in the Content Addressable Storage
 * @return the list<string> result
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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.Collections.synchronizedList;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.buildfarm.cas.cfc.CASFileCache;
import build.buildfarm.cas.cfc.CASFileCache.PathResult;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.io.Directories;
import build.buildfarm.v1test.Digest;
import build.buildfarm.v1test.WorkerExecutedMetadata;
import build.buildfarm.worker.ExecDirException.ViolationException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.UserPrincipal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import lombok.extern.java.Log;

@Log
public class CFCLinkExecFileSystem extends CFCExecFileSystem {
  // perform first-available non-output symlinking and retain directories in cache
  private final boolean linkInputDirectories;

  // indicate symlinking above for a set of matching paths
  private final Iterable<Pattern> linkedInputDirectories;

  private final Map<Path, DigestFunction.Value> rootInputDigestFunction = new ConcurrentHashMap<>();
  private final Map<Path, Iterable<String>> rootInputFiles = new ConcurrentHashMap<>();
  /**
   * Stores a blob in the Content Addressable Storage Includes input validation and error handling for robustness.
   * @param digest the digest parameter
   * @param path the path parameter
   * @param isExecutable the isExecutable parameter
   * @param onKey the onKey parameter
   * @return the listenablefuture<void> result
   */
  private final Map<Path, Iterable<build.bazel.remote.execution.v2.Digest>> rootInputDirectories =
      new ConcurrentHashMap<>();

  /**
   * Performs specialized operation based on method logic
   * @return the string result
   */
  /**
   * Performs specialized operation based on method logic
   * @return the boolean result
   */
  public CFCLinkExecFileSystem(
      Path root,
      CASFileCache fileCache,
      ImmutableMap<String, UserPrincipal> owners,
      boolean linkInputDirectories,
      Iterable<String> linkedInputDirectories,
      boolean allowSymlinkTargetAbsolute,
      ExecutorService removeDirectoryService,
      ExecutorService accessRecorder,
      ExecutorService fetchService) {
    super(
        root,
        fileCache,
        owners,
        allowSymlinkTargetAbsolute,
        removeDirectoryService,
        accessRecorder,
        fetchService);
    this.linkInputDirectories = linkInputDirectories;
    this.linkedInputDirectories = Iterables.transform(linkedInputDirectories, Pattern::compile);
  }

  @SuppressWarnings("ConstantConditions")
  /**
   * Performs specialized operation based on method logic Performs side effects including logging and state modifications.
   * @param execPath the execPath parameter
   * @param digest the digest parameter
   * @param directoriesIndex the directoriesIndex parameter
   * @return the listenablefuture<pathresult> result
   */
  /**
   * Stores a blob in the Content Addressable Storage
   * @param digest the digest parameter
   * @param root the root parameter
   * @param path the path parameter
   * @param isExecutable the isExecutable parameter
   * @param onKey the onKey parameter
   * @return the listenablefuture<void> result
   */
  private ListenableFuture<Void> put(
      Digest digest, Path path, boolean isExecutable, Consumer<String> onKey) {
    if (digest.getSize() == 0) {
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
        pathResult -> {
          checkNotNull(key);
          // we saw null entries in the built immutable list without synchronization
          onKey.accept(key);
          if (digest.getSize() != 0) {
            try {
              Files.createLink(path, pathResult.path());
            } catch (IOException e) {
              return immediateFailedFuture(e);
            }
          }
          return immediateFuture(null);
        },
        fetchService);
  }

  /**
   * Validates input parameters and state consistency Includes input validation and error handling for robustness.
   * @param path the path parameter
   * @param errors the errors parameter
   */
  private ListenableFuture<Void> catchingPut(
      Digest digest, Path root, Path path, boolean isExecutable, Consumer<String> onKey) {
    return catching(
        put(digest, path, isExecutable, onKey),
        e -> new ViolationException(digest, root.relativize(path), isExecutable, e));
  }

  @SuppressWarnings("ConstantConditions")
  /**
   * Performs specialized operation based on method logic
   * @param digest the digest parameter
   * @param directoriesIndex the directoriesIndex parameter
   * @return the iterator<string> result
   */
  private ListenableFuture<PathResult> linkDirectory(
      Path execPath,
      Digest digest,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex) {
    return transformAsync(
        fileCache.putDirectory(digest, directoriesIndex, fetchService),
        pathResult -> {
          Path path = pathResult.path();
          if (pathResult.isMissed()) {
            log.finer(
                String.format(
                    "putDirectory(%s, %s) created", execPath, DigestUtil.toString(digest)));
          }
          Files.createSymbolicLink(execPath, path);
          return immediateFuture(pathResult);
        },
        fetchService);
  }

  private static void checkExecErrors(Path path, List<Throwable> errors) throws ExecDirException {
    if (!errors.isEmpty()) {
      throw new ExecDirException(path, errors);
    }
  }

  /**
   * Stores a blob in the Content Addressable Storage
   * @param command the command parameter
   * @return the outputdirectory result
   */
  /**
   * Performs specialized operation based on method logic Implements complex logic with 3 conditional branches and 2 iterative operations.
   * @param directoriesIndex the directoriesIndex parameter
   * @param rootDigest the rootDigest parameter
   * @return the set<string> result
   */
  private static Iterator<String> directoriesIterator(
      build.bazel.remote.execution.v2.Digest digest,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex) {
    Directory root = directoriesIndex.get(digest);
    return new Iterator<>() {
      boolean atEnd = root.getDirectoriesCount() == 0;
      Stack<String> path = new Stack<>();
      Stack<Iterator<DirectoryNode>> route = new Stack<>();
      Iterator<DirectoryNode> current = root.getDirectoriesList().iterator();

      @Override
      public boolean hasNext() {
        return !atEnd;
      }

      @Override
      /**
       * Performs specialized operation based on method logic Implements complex logic with 5 conditional branches.
       * @param dir the dir parameter
       * @param attrs the attrs parameter
       * @return the filevisitresult result
       */
      public String next() {
        String nextPath;
        DirectoryNode next = current.next();
        String name = next.getName();
        path.push(name);
        nextPath = String.join("/", path);
        build.bazel.remote.execution.v2.Digest digest = next.getDigest();
        if (digest.getSizeBytes() != 0) {
          route.push(current);
          current = directoriesIndex.get(digest).getDirectoriesList().iterator();
        } else {
          path.pop();
        }
        while (!current.hasNext() && !route.isEmpty()) {
          current = route.pop();
          path.pop();
        }
        atEnd = !current.hasNext();
        return nextPath;
      }
    };
  }

  private Set<String> linkedDirectories(
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      build.bazel.remote.execution.v2.Digest rootDigest) {
    // skip this search if all the directories are real
    if (linkInputDirectories) {
      ImmutableSet.Builder<String> builder = ImmutableSet.builder();

      Iterator<String> dirs = directoriesIterator(rootDigest, directoriesIndex);
      while (dirs.hasNext()) {
        String dir = dirs.next();
        for (Pattern pattern : linkedInputDirectories) {
          if (pattern.matcher(dir).matches()) {
            builder.add(dir);
            break; // avoid adding the same directory twice
          }
        }
      }
      return builder.build();
    }
    return ImmutableSet.of();
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

  class LinkExecFileVisitor extends ExecFileVisitor {
    private final Path root;
    private final Set<Path> linkedDirectories; // only need contains
    private final Map<build.bazel.remote.execution.v2.Digest, Directory>
        index; // only need retrieve
    private final DigestFunction.Value digestFunction;
    private final OutputDirectory outputDirectoryRoot;
    private final Stack<OutputDirectory> outputDirectories = new Stack<>();
    private final List<String> inputFiles = synchronizedList(new ArrayList<>());
    /**
     * Performs specialized operation based on method logic Performs side effects including logging and state modifications.
     * @param root the root parameter
     * @return the long result
     */
    private final List<build.bazel.remote.execution.v2.Digest> inputDirectories =
        synchronizedList(new ArrayList<>());

    LinkExecFileVisitor(
        WorkerExecutedMetadata.Builder workerExecutedMetadata,
        Path root,
        Set<Path> linkedDirectories,
        Map<build.bazel.remote.execution.v2.Digest, Directory> index,
        DigestFunction.Value digestFunction,
        OutputDirectory outputDirectoryRoot) {
      super(workerExecutedMetadata);
      this.root = root;
      this.linkedDirectories = linkedDirectories;
      this.index = index;
      this.digestFunction = digestFunction;
      this.outputDirectoryRoot = outputDirectoryRoot;
    }

    List<String> inputFiles() {
      return inputFiles;
    }

    List<build.bazel.remote.execution.v2.Digest> inputDirectories() {
      return inputDirectories;
    }

    private long sumDirectorySize(build.bazel.remote.execution.v2.Digest root) {
      long size = 0;
      List<build.bazel.remote.execution.v2.Digest> digests = new ArrayList<>();
      digests.add(root);
      while (!digests.isEmpty()) {
        Directory directory = index.get(digests.removeFirst());
        for (FileNode fileNode : directory.getFilesList()) {
          size += fileNode.getDigest().getSizeBytes();
        }
        Iterables.addAll(
            digests,
            Iterables.transform(directory.getDirectoriesList(), dirNode -> dirNode.getDigest()));
      }
      return size;
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
      OutputDirectory outputDirectory;
      if (outputDirectories.isEmpty()) {
        outputDirectory = outputDirectoryRoot;
      } else {
        String name = dir.getFileName().toString();
        OutputDirectory parentOutputDirectory = outputDirectories.peek();
        outputDirectory =
            parentOutputDirectory != null ? parentOutputDirectory.getChild(name) : null;
      }
      if (outputDirectory == null && linkedDirectories.contains(dir)) {
        // this is scary, given the switch
        build.bazel.remote.execution.v2.Digest digest =
            (build.bazel.remote.execution.v2.Digest) attrs.fileKey();
        workerExecutedMetadata.addLinkedInputDirectories(root.relativize(dir).toString());
        futures.add(
            transform(
                linkDirectory(dir, DigestUtil.fromDigest(digest, digestFunction), index),
                pathResult -> {
                  inputDirectories.add(digest);
                  if (pathResult.isMissed()) {
                    fetchedBytes(sumDirectorySize(digest));
                  }
                  return null;
                },
                fetchService));
        return FileVisitResult.SKIP_SUBTREE;
      }

      FileVisitResult result = super.preVisitDirectory(dir, attrs);
      if (result == FileVisitResult.CONTINUE) {
        outputDirectories.push(outputDirectory);
      }
      return result;
    }

    @Override
    /**
     * Performs specialized operation based on method logic Executes asynchronously and returns a future for completion tracking.
     * @param file the file parameter
     * @param attrs the attrs parameter
     * @return the filevisitresult result
     */
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
      // this is only called when we've continued and placed onto stack
      outputDirectories.pop();
      return super.postVisitDirectory(dir, exc);
    }

    @Override
    /**
     * Creates and initializes a new instance Implements complex logic with 8 conditional branches and 1 iterative operations. Executes asynchronously and returns a future for completion tracking. Processes 1 input sources and produces 1 outputs. Performs side effects including logging and state modifications. Includes input validation and error handling for robustness.
     * @param operationName the operationName parameter
     * @param directoriesIndex the directoriesIndex parameter
     * @param digestFunction the digestFunction parameter
     * @param action the action parameter
     * @param command the command parameter
     * @param owner the owner parameter
     * @param workerExecutedMetadata the workerExecutedMetadata parameter
     * @return the path result
     */
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
      ListenableFuture<Void> populate;
      boolean terminate = false;
      if (attrs.isSymbolicLink()) {
        ExecSymlinkAttributes symlinkAttrs = (ExecSymlinkAttributes) attrs;
        populate = putSymlink(file, symlinkAttrs.target());
      } else if (attrs.isRegularFile()) {
        // more scary
        build.bazel.remote.execution.v2.Digest digest =
            (build.bazel.remote.execution.v2.Digest) attrs.fileKey();
        ExecFileAttributes fileAttrs = (ExecFileAttributes) attrs;
        // mild risk here with inputFiles missing a key that was referenced...
        populate =
            catchingPut(
                DigestUtil.fromDigest(digest, digestFunction),
                root,
                file,
                fileAttrs.isExecutable(),
                inputFiles::add);
      } else {
        populate = immediateFailedFuture(new IOException("unknown file type for " + file));
        terminate = true;
      }
      futures.add(populate);
      return terminate ? FileVisitResult.TERMINATE : FileVisitResult.CONTINUE;
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
    Digest inputRootDigest = DigestUtil.fromDigest(action.getInputRootDigest(), digestFunction);
    OutputDirectory outputDirectory = createOutputDirectory(command);

    Path execDir = root().resolve(operationName);
    if (Files.exists(execDir)) {
      Directories.remove(execDir, fileStore);
    }
    Files.createDirectories(execDir);

    Set<Path> linkedInputDirectories =
        linkInputDirectories
            ? ImmutableSet.copyOf(
                Iterables.transform(
                    linkedDirectories(directoriesIndex, DigestUtil.toDigest(inputRootDigest)),
                    execDir::resolve))
            : ImmutableSet.of(); // does this work on windows with / separators?

    log.log(Level.FINER, operationName + " walking execTree");
    ExecTree execTree = new ExecTree(directoriesIndex);
    LinkExecFileVisitor visitor =
        new LinkExecFileVisitor(
            workerExecutedMetadata,
            execDir,
            linkedInputDirectories,
            directoriesIndex,
            digestFunction,
            outputDirectory);
    execTree.walk(execDir, inputRootDigest, visitor);
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
        fileCache.decrementReferences(
            visitor.inputFiles(), visitor.inputDirectories(), digestFunction);
        Directories.remove(execDir, fileStore);
      }
    }

    rootInputDigestFunction.put(execDir, digestFunction);
    rootInputFiles.put(execDir, visitor.inputFiles());
    rootInputDirectories.put(execDir, visitor.inputDirectories());

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
    DigestFunction.Value digestFunction = rootInputDigestFunction.remove(execDir);
    Iterable<String> inputFiles = rootInputFiles.remove(execDir);
    Iterable<build.bazel.remote.execution.v2.Digest> inputDirectories =
        rootInputDirectories.remove(execDir);
    if (inputFiles != null || inputDirectories != null) {
      fileCache.decrementReferences(
          inputFiles == null ? ImmutableList.of() : inputFiles,
          inputDirectories == null ? ImmutableList.of() : inputDirectories,
          digestFunction);
    }
    super.destroyExecDir(execDir);
  }
}
