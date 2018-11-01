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

import static build.buildfarm.worker.CASFileCache.getOrIOException;
import static build.buildfarm.worker.UploadManifest.readdir;
import static com.google.common.util.concurrent.Futures.allAsList;
import static java.util.logging.Level.SEVERE;

import build.buildfarm.common.ContentAddressableStorage;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.worker.CASFileCache;
import build.buildfarm.worker.Dirent;
import build.buildfarm.worker.OutputDirectory;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.DirectoryNode;
import com.google.devtools.remoteexecution.v1test.FileNode;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.logging.Logger;

class CFCExecFileSystem implements ExecFileSystem {
  private static final Logger logger = Logger.getLogger(CFCExecFileSystem.class.getName());

  private final Path root;
  private final CASFileCache fileCache;
  private final boolean linkInputDirectories; // perform first-available non-output symlinking and retain directories in cache
  private final Map<Path, Iterable<Path>> rootInputFiles = new ConcurrentHashMap<>();
  private final Map<Path, Iterable<Digest>> rootInputDirectories = new ConcurrentHashMap<>();

  CFCExecFileSystem(Path root, CASFileCache fileCache, boolean linkInputDirectories) {
    this.root = root;
    this.fileCache = fileCache;
    this.linkInputDirectories = linkInputDirectories;
  }

  @Override
  public void start(Consumer<List<Digest>> onDigests) throws IOException, InterruptedException {
    List<Dirent> dirents = null;
    try {
      dirents = readdir(root, /* followSymlinks= */ false);
    } catch (IOException e) {
      logger.log(SEVERE, "error reading " + root.toString(), e);
    }

    ImmutableList.Builder<ListenableFuture<Void>> removeDirectoryFutures = ImmutableList.builder();

    // only valid path under root is cache
    for (Dirent dirent : dirents) {
      String name = dirent.getName();
      Path child = root.resolve(name);
      if (!child.equals(fileCache.getRoot())) {
        removeDirectoryFutures.add(fileCache.removeDirectoryAsync(root.resolve(name)));
      }
    }

    ImmutableList.Builder<Digest> blobDigests = ImmutableList.builder();
    fileCache.start(blobDigests::add);
    onDigests.accept(blobDigests.build());

    getOrIOException(allAsList(removeDirectoryFutures.build()));
  }

  @Override
  public void stop() {
    fileCache.stop();
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

  private void fetchInputs(
      Path execDir,
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

    for (FileNode fileNode : directory.getFilesList()) {
      Path execPath = execDir.resolve(fileNode.getName());
      Path fileCacheKey = fileCache.put(fileNode.getDigest(), fileNode.getIsExecutable(), /* containingDirectory=*/ null);
      inputFiles.add(fileCacheKey);
      Files.createLink(execPath, fileCacheKey);
    }

    for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
      Digest digest = directoryNode.getDigest();
      String name = directoryNode.getName();
      OutputDirectory childOutputDirectory = outputDirectory != null
          ? outputDirectory.getChild(name) : null;
      Path dirPath = execDir.resolve(name);
      if (childOutputDirectory != null || !linkInputDirectories) {
        Files.createDirectories(dirPath);
        fetchInputs(dirPath, digest, directoriesIndex, childOutputDirectory, inputFiles, inputDirectories);
      } else {
        linkDirectory(dirPath, digest, directoriesIndex);
        inputDirectories.add(digest);
      }
    }
  }

  private void linkDirectory(
      Path execPath,
      Digest digest,
      Map<Digest, Directory> directoriesIndex) throws IOException, InterruptedException {
    Path cachePath = fileCache.putDirectory(digest, directoriesIndex);
    Files.createSymbolicLink(execPath, cachePath);
  }

  @Override
  public Path createExecDir(String operationName, Map<Digest, Directory> directoriesIndex, Action action) throws IOException, InterruptedException {
    OutputDirectory outputDirectory = OutputDirectory.parse(
        action.getOutputFilesList(),
        action.getOutputDirectoriesList());

    Path actionRoot = root.resolve(operationName);
    if (Files.exists(actionRoot)) {
      getOrIOException(fileCache.removeDirectoryAsync(actionRoot));
    }
    Files.createDirectories(actionRoot);

    ImmutableList.Builder<Path> inputFiles = new ImmutableList.Builder<>();
    ImmutableList.Builder<Digest> inputDirectories = new ImmutableList.Builder<>();

    logger.info("ExecFileSystem::createExecDir(" + DigestUtil.toString(action.getInputRootDigest()) + ") calling fetchInputs");
    boolean fetched = false;
    try {
      fetchInputs(
          actionRoot,
          action.getInputRootDigest(),
          directoriesIndex,
          outputDirectory,
          inputFiles,
          inputDirectories);
      fetched = true;
    } finally {
      if (!fetched) {
        fileCache.decrementReferences(inputFiles.build(), inputDirectories.build());
      }
    }

    rootInputFiles.put(actionRoot, inputFiles.build());
    rootInputDirectories.put(actionRoot, inputDirectories.build());

    logger.info("ExecFileSystem::createExecDir(" + DigestUtil.toString(action.getInputRootDigest()) + ") stamping output directories");
    try {
      outputDirectory.stamp(actionRoot);
    } catch (IOException e) {
      destroyExecDir(actionRoot);
      throw e;
    }
    return actionRoot;
  }

  @Override
  public void destroyExecDir(Path actionRoot) throws IOException, InterruptedException {
    Iterable<Path> inputFiles = rootInputFiles.remove(actionRoot);
    Iterable<Digest> inputDirectories = rootInputDirectories.remove(actionRoot);
    if (inputFiles != null || inputDirectories != null) {
      fileCache.decrementReferences(
          inputFiles == null ? ImmutableList.<Path>of() : inputFiles,
          inputDirectories == null ? ImmutableList.<Digest>of() : inputDirectories);
    }
    if (Files.exists(actionRoot)) {
      getOrIOException(fileCache.removeDirectoryAsync(actionRoot));
    }
  }
}
