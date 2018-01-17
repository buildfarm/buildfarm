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

import build.buildfarm.common.Digests;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.DirectoryNode;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata;
import com.google.devtools.remoteexecution.v1test.FileNode;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;

class InputFetchStage extends PipelineStage {
  private final BlockingQueue<OperationContext> queue;

  InputFetchStage(Worker worker, PipelineStage output, PipelineStage error) {
    super(worker, output, error);
    queue = new ArrayBlockingQueue<>(1);
  }

  @Override
  public OperationContext take() throws InterruptedException {
    return queue.take();
  }

  @Override
  public void offer(OperationContext operationContext) {
    queue.offer(operationContext);
  }

  @Override
  protected OperationContext tick(OperationContext operationContext) {
    final String operationName = operationContext.operation.getName();
    Poller poller = new Poller(worker.config.getOperationPollPeriod(), () -> {
          boolean success = worker.instance.pollOperation(
              operationName,
              ExecuteOperationMetadata.Stage.QUEUED);
          return success;
        });
    new Thread(poller).start();

    Path execDir = operationContext.execDir;

    boolean success = true;
    try {
      if (Files.exists(execDir)) {
        Worker.removeDirectory(execDir);
      }
      Files.createDirectories(execDir);

      fetchInputs(
          execDir,
          operationContext.action.getInputRootDigest(),
          operationContext.inputFiles);

      verifyOutputLocations(
          execDir,
          operationContext.action.getOutputFilesList(),
          operationContext.action.getOutputDirectoriesList());
    } catch (IOException|InterruptedException ex) {
      ex.printStackTrace();
      success = false;
    }

    poller.stop();

    if (!success) {
      worker.fileCache.update(operationContext.inputFiles);
    }

    return success ? operationContext : null;
  }

  private void fetchInputs(
      Path execDir,
      Digest inputRoot,
      List<String> inputFiles) throws IOException, InterruptedException {
    ImmutableList.Builder<Directory> directories = new ImmutableList.Builder<>();
    String pageToken = "";

    do {
      pageToken = worker.instance.getTree(inputRoot, worker.config.getTreePageSize(), pageToken, directories);
    } while (!pageToken.isEmpty());

    Set<Digest> directoryDigests = new HashSet<>();
    ImmutableMap.Builder<Digest, Directory> directoriesIndex = new ImmutableMap.Builder<>();
    long entries = 0;
    for (Directory directory : directories.build()) {
      entries++;
      Digest directoryDigest = Digests.computeDigest(directory);
      if (!directoryDigests.add(directoryDigest)) {
        continue;
      }
      directoriesIndex.put(directoryDigest, directory);
    }

    linkInputs(execDir, inputRoot, directoriesIndex.build(), inputFiles);
  }

  private void linkInputs(
      Path execDir,
      Digest inputRoot,
      Map<Digest, Directory> directoriesIndex,
      List<String> inputFiles)
      throws IOException, InterruptedException {
    Directory directory = directoriesIndex.get(inputRoot);

    for (FileNode fileNode : directory.getFilesList()) {
      Path execPath = execDir.resolve(fileNode.getName());
      String fileCacheKey = worker.fileCache.put(fileNode.getDigest(), fileNode.getIsExecutable());
      if (fileCacheKey == null) {
        throw new IOException("InputFetchStage: Failed to create cache entry for " + execPath);
      }
      inputFiles.add(fileCacheKey);
      Files.createLink(execPath, worker.fileCache.getPath(fileCacheKey));
    }

    for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
      Digest digest = directoryNode.getDigest();
      String name = directoryNode.getName();
      Path dirPath = execDir.resolve(name);
      Files.createDirectories(dirPath);
      linkInputs(dirPath, digest, directoriesIndex, inputFiles);
    }
  }

  private void verifyOutputLocations(
      Path execDir,
      Iterable<String> outputFiles,
      Iterable<String> outputDirs) throws IOException {
    for (String outputFile : outputFiles) {
      Path outputDir = execDir.resolve(outputFile).getParent();
      if (!Files.exists(outputDir)) {
        Files.createDirectories(outputDir);
      }
    }

    for (String outputDir : outputDirs) {
      throw new IllegalArgumentException("InputFetchStage: outputDir specified: " + outputDir);
    }
  }
}
