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
import build.buildfarm.worker.util.LinkedInputExclusions;
import build.buildfarm.worker.util.LinkedInputExclusions.ExclusionSet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.UserPrincipal;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import lombok.extern.java.Log;
import org.jspecify.annotations.Nullable;

@Log
public class CFCLinkExecFileSystem extends CFCExecFileSystem {
  // --- Phase 5 (LinkedInputExclusions) directory-link ratio ---
  // The risk here is asymmetric: exclusions too broad push linkable directories back to per-file
  // hardlinking (silent loss of the speedup). These two counters are that tripwire -- if a denylist
  // change quietly drops the symlinked share, the ratio symlinked/(symlinked+fallback) falls before
  // users notice. Counted at the per-directory link decision in preVisitDirectory.
  private static final Counter execDirDirectoriesSymlinkedTotal =
      Counter.build()
          .name("exec_dir_directories_symlinked_total")
          .help("Input directories materialized as a single directory symlink (the cheap path).")
          .register();
  private static final Counter execDirDirectoriesHardlinkedFallbackTotal =
      Counter.build()
          .name("exec_dir_directories_hardlinked_fallback_total")
          .help(
              "Link-candidate input directories excluded from directory symlinking, descended as "
                  + "real directories and materialized via per-file hardlinking.")
          .register();

  // perform first-available non-output symlinking and retain directories in cache
  private final boolean linkInputDirectories;

  // operator-supplied regex patterns; any input directory whose path-relative-to-the-input-root
  // matches is kept as a real directory rather than symlinked to its CAS tree. These merge with the
  // auto-computed LinkedInputExclusions set — a directory is excluded from linking if it appears in
  // the computed set OR matches one of these patterns.
  private final ImmutableList<Pattern> linkedInputExclusionPatterns;

  private final Map<Path, DigestFunction.Value> rootInputDigestFunction = new ConcurrentHashMap<>();
  private final Map<Path, Iterable<String>> rootInputFiles = new ConcurrentHashMap<>();
  private final Map<Path, Iterable<build.bazel.remote.execution.v2.Digest>> rootInputDirectories =
      new ConcurrentHashMap<>();

  public CFCLinkExecFileSystem(
      Path root,
      CASFileCache fileCache,
      ImmutableMap<String, UserPrincipal> owners,
      boolean linkInputDirectories,
      Iterable<String> linkedInputDirectories,
      Iterable<String> linkedInputExclusionPatterns,
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
    this.linkedInputExclusionPatterns = compileExclusionPatterns(linkedInputExclusionPatterns);
    if (linkedInputDirectories.iterator().hasNext()) {
      log.warning(
          "linkedInputDirectories is deprecated and ignored; the LinkedInputExclusions computation"
              + " automatically determines which directories can be symlinked based on output"
              + " paths. To force specific directories to remain real, use"
              + " linkedInputExclusionPatterns.");
    }
  }

  private static ImmutableList<Pattern> compileExclusionPatterns(Iterable<String> patterns) {
    ImmutableList.Builder<Pattern> compiled = ImmutableList.builder();
    for (String pattern : patterns) {
      try {
        compiled.add(Pattern.compile(pattern));
      } catch (PatternSyntaxException e) {
        throw new IllegalArgumentException(
            "linkedInputExclusionPatterns contains an invalid regex: " + pattern, e);
      }
    }
    return compiled.build();
  }

  private static final class DirectoryFrame {
    private final String path;
    private final Directory directory;

    private DirectoryFrame(String path, Directory directory) {
      this.path = path;
      this.directory = directory;
    }
  }

  @SuppressWarnings("ConstantConditions")
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
              materializePathTotal.labels("hardlink_file").inc();
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
    return catching(
        put(digest, path, isExecutable, onKey),
        e -> new ViolationException(digest, root.relativize(path), isExecutable, e));
  }

  @SuppressWarnings("ConstantConditions")
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
          materializePathTotal.labels("symlink_dir").inc();
          return immediateFuture(pathResult);
        },
        fetchService);
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

  private boolean matchesLinkedInputExclusionPattern(String relativePath) {
    for (Pattern pattern : linkedInputExclusionPatterns) {
      if (pattern.matcher(relativePath).matches()) {
        return true;
      }
    }
    return false;
  }

  private ImmutableSet<String> matchedLinkedInputExclusionDirectories(
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      build.bazel.remote.execution.v2.Digest rootDigest) {
    if (linkedInputExclusionPatterns.isEmpty()) {
      return ImmutableSet.of();
    }

    HashSet<String> matches = new HashSet<>();
    ArrayDeque<DirectoryFrame> remaining = new ArrayDeque<>();
    remaining.add(new DirectoryFrame("", directoriesIndex.get(rootDigest)));
    while (!remaining.isEmpty()) {
      DirectoryFrame frame = remaining.removeLast();
      for (DirectoryNode directoryNode : frame.directory.getDirectoriesList()) {
        String path =
            frame.path.isEmpty()
                ? directoryNode.getName()
                : frame.path + "/" + directoryNode.getName();
        if (matchesLinkedInputExclusionPattern(path)) {
          matches.add(path);
        }
        if (directoryNode.getDigest().getSizeBytes() != 0) {
          remaining.add(new DirectoryFrame(path, directoriesIndex.get(directoryNode.getDigest())));
        }
      }
    }
    return ImmutableSet.copyOf(matches);
  }

  class LinkExecFileVisitor extends ExecFileVisitor {
    private final Path root;
    private final ExclusionSet linkedInputExclusions;
    private final Map<build.bazel.remote.execution.v2.Digest, Directory>
        index; // only need retrieve
    private final OutputDirectory outputDirectoryRoot;
    private final Stack<OutputDirectory> outputDirectories = new Stack<>();
    private final List<String> inputFiles = synchronizedList(new ArrayList<>());
    private final List<build.bazel.remote.execution.v2.Digest> inputDirectories =
        synchronizedList(new ArrayList<>());

    LinkExecFileVisitor(
        WorkerExecutedMetadata.Builder workerExecutedMetadata,
        Path root,
        ExclusionSet linkedInputExclusions,
        Map<build.bazel.remote.execution.v2.Digest, Directory> index,
        OutputDirectory outputDirectoryRoot) {
      super(workerExecutedMetadata);
      this.root = root;
      this.linkedInputExclusions = linkedInputExclusions;
      this.index = index;
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
      String relativePath = LinkedInputExclusions.pathToRelativeString(root, dir);
      // A directory is a symlink candidate when input-dir linking is on and it is not an output
      // directory; of those, an exclusion is what forces the per-file-hardlink fallback.
      boolean linkCandidate = linkInputDirectories && outputDirectory == null;
      boolean excluded = linkCandidate && linkedInputExclusions.excludes(relativePath);
      if (linkCandidate && !excluded) {
        execDirDirectoriesSymlinkedTotal.inc();
        Digest digest = (Digest) attrs.fileKey();
        build.bazel.remote.execution.v2.Digest reapiDigest = DigestUtil.toDigest(digest);
        workerExecutedMetadata.addLinkedInputDirectories(relativePath);
        futures.add(
            transform(
                linkDirectory(dir, digest, index),
                pathResult -> {
                  inputDirectories.add(reapiDigest);
                  if (pathResult.isMissed()) {
                    fetchedBytes(sumDirectorySize(reapiDigest));
                  }
                  return null;
                },
                fetchService));
        return FileVisitResult.SKIP_SUBTREE;
      }
      if (excluded) {
        execDirDirectoriesHardlinkedFallbackTotal.inc();
      }

      FileVisitResult result = super.preVisitDirectory(dir, attrs);
      if (result == FileVisitResult.CONTINUE) {
        outputDirectories.push(outputDirectory);
      }
      return result;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
      // this is only called when we've continued and placed onto stack
      outputDirectories.pop();
      return super.postVisitDirectory(dir, exc);
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
        // mild risk here with inputFiles missing a key that was referenced...
        populate = catchingPut(digest, root, file, fileAttrs.isExecutable(), inputFiles::add);
      } else {
        populate = immediateFailedFuture(new IOException("unknown file type for " + file));
        terminate = true;
      }
      futures.add(populate);
      return terminate ? FileVisitResult.TERMINATE : FileVisitResult.CONTINUE;
    }
  }

  @Override
  public Path createExecDir(
      String operationName,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      DigestFunction.Value digestFunction,
      Action action,
      Command command,
      @Nullable UserPrincipal owner,
      WorkerExecutedMetadata.Builder workerExecutedMetadata)
      throws IOException, InterruptedException {
    Histogram.Timer materializeTimer = materializeExecRootSeconds.labels("regular").startTimer();
    try {
    Digest inputRootDigest = DigestUtil.fromDigest(action.getInputRootDigest(), digestFunction);
    OutputDirectory outputDirectory = createOutputDirectory(command);

    ExclusionSet linkedInputExclusions =
        linkInputDirectories
            ? LinkedInputExclusions.computeExclusionSet(
                command,
                ImmutableSet.of(),
                matchedLinkedInputExclusionDirectories(
                    directoriesIndex, DigestUtil.toDigest(inputRootDigest)))
            : ExclusionSet.empty();

    Path execDir = root().resolve(operationName);
    if (Files.exists(execDir)) {
      Directories.remove(execDir, fileStore);
    }
    Files.createDirectories(execDir);

    log.log(Level.FINER, operationName + " walking execTree");
    ExecTree execTree = new ExecTree(directoriesIndex);
    LinkExecFileVisitor visitor =
        new LinkExecFileVisitor(
            workerExecutedMetadata,
            execDir,
            linkedInputExclusions,
            directoriesIndex,
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
          } catch (CancellationException e) {
            exceptions.add(e);
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
    } finally {
      materializeTimer.observeDuration();
    }
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
