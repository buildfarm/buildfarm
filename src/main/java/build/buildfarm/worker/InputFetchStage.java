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

import build.buildfarm.common.DigestUtil;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;

public class InputFetchStage extends PipelineStage {
  private final BlockingQueue<OperationContext> queue;

  public InputFetchStage(WorkerContext workerContext, PipelineStage output, PipelineStage error) {
    super(workerContext, output, error);
    queue = new ArrayBlockingQueue<>(1);
  }

  @Override
  public OperationContext take() throws InterruptedException {
    return queue.take();
  }

  @Override
  public void put(OperationContext operationContext) throws InterruptedException {
    queue.put(operationContext);
  }

  @Override
  protected OperationContext tick(OperationContext operationContext) throws InterruptedException {
    Poller poller = workerContext.createPoller(
        "InputFetchStage",
        operationContext.operation.getName(),
        ExecuteOperationMetadata.Stage.QUEUED,
        () -> {});

    Path execDir = operationContext.execDir;

    OutputDirectory outputDirectory = parseOutputDirectories(
        operationContext.action.getOutputFilesList(),
        operationContext.action.getOutputDirectoriesList());

    boolean success = true;
    try {
      if (Files.exists(execDir)) {
        CASFileCache.removeDirectory(execDir);
      }
      Files.createDirectories(execDir);

      fetchInputs(
          execDir,
          operationContext.action.getInputRootDigest(),
          outputDirectory,
          operationContext.inputFiles,
          operationContext.inputDirectories);

      verifyOutputLocations(
          execDir,
          operationContext.action.getOutputFilesList(),
          operationContext.action.getOutputDirectoriesList());
    } catch (IOException e) {
      e.printStackTrace();
      success = false;
    }

    poller.stop();

    if (!success) {
      workerContext.getBlobPathFactory().decrementReferences(operationContext.inputFiles, operationContext.inputDirectories);
    }

    return success ? operationContext : null;
  }

  class OutputDirectory {
    public final Map<String, OutputDirectory> directories;

    OutputDirectory() {
      directories = new HashMap<>();
    }
  }

  private OutputDirectory parseOutputDirectories(Iterable<String> outputFiles, Iterable<String> outputDirs) {
    OutputDirectory outputDirectory = new OutputDirectory();
    Stack<OutputDirectory> stack = new Stack<>();

    OutputDirectory currentOutputDirectory = outputDirectory;
    String prefix = "";
    for (String outputFile : outputFiles) {
      while (!outputFile.startsWith(prefix)) {
        currentOutputDirectory = stack.pop();
        int upPathSeparatorIndex = prefix.lastIndexOf(File.separator, prefix.length() - 2);
        prefix = prefix.substring(0, upPathSeparatorIndex + 1);
      }
      String prefixedFile = outputFile.substring(prefix.length());
      int separatorIndex = prefixedFile.indexOf(File.separator);
      while (separatorIndex >= 0) {
        if (separatorIndex == 0) {
          throw new IllegalArgumentException("InputFetchStage: double separator in output file");
        }

        String directoryName = prefixedFile.substring(0, separatorIndex);
        prefix += directoryName + File.separator;
        prefixedFile = prefixedFile.substring(separatorIndex + 1);
        stack.push(currentOutputDirectory);
        OutputDirectory nextOutputDirectory = new OutputDirectory();
        currentOutputDirectory.directories.put(directoryName, nextOutputDirectory);
        currentOutputDirectory = nextOutputDirectory;
        separatorIndex = prefixedFile.indexOf(File.separator);
      }
    }

    if (!Iterables.isEmpty(outputDirs)) {
      throw new IllegalArgumentException("InputFetchStage: outputDir specified: " + outputDirs);
    }

    return outputDirectory;
  }

  private void fetchInputs(
      Path execDir,
      Digest inputRoot,
      OutputDirectory outputDirectory,
      List<Path> inputFiles,
      List<Digest> inputDirectories) throws IOException, InterruptedException {
    ImmutableList.Builder<Directory> directories = new ImmutableList.Builder<>();
    String pageToken = "";

    do {
      pageToken = workerContext.getInstance().getTree(inputRoot, workerContext.getTreePageSize(), pageToken, directories);
    } while (!pageToken.isEmpty());

    Set<Digest> directoryDigests = new HashSet<>();
    ImmutableMap.Builder<Digest, Directory> directoriesIndex = new ImmutableMap.Builder<>();
    for (Directory directory : directories.build()) {
      Digest directoryDigest = workerContext.getDigestUtil().compute(directory);
      if (!directoryDigests.add(directoryDigest)) {
        continue;
      }
      directoriesIndex.put(directoryDigest, directory);
    }

    linkInputs(execDir, inputRoot, directoriesIndex.build(), outputDirectory, inputFiles, inputDirectories);
  }

  private void linkInputs(
      Path execDir,
      Digest inputRoot,
      Map<Digest, Directory> directoriesIndex,
      OutputDirectory outputDirectory,
      List<Path> inputFiles,
      List<Digest> inputDirectories)
      throws IOException, InterruptedException {
    Directory directory = directoriesIndex.get(inputRoot);
    if (directory == null) {
      throw new IOException("Directory " + DigestUtil.toString(inputRoot) + " is not in input index");
    }

    for (FileNode fileNode : directory.getFilesList()) {
      Path execPath = execDir.resolve(fileNode.getName());
      Path fileCacheKey = workerContext.getBlobPathFactory().getBlobPath(fileNode.getDigest(), fileNode.getIsExecutable(), /* containingDirectory=*/ null);
      if (fileCacheKey == null) {
        throw new IOException("InputFetchStage: Failed to create cache entry for " + execPath);
      }
      inputFiles.add(fileCacheKey);
      Files.createLink(execPath, fileCacheKey);
    }

    for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
      Digest digest = directoryNode.getDigest();
      String name = directoryNode.getName();
      OutputDirectory childOutputDirectory = outputDirectory != null
          ? outputDirectory.directories.get(name) : null;
      Path dirPath = execDir.resolve(name);
      if (childOutputDirectory != null || !workerContext.getLinkInputDirectories()) {
        Files.createDirectories(dirPath);
        linkInputs(dirPath, digest, directoriesIndex, childOutputDirectory, inputFiles, inputDirectories);
      } else {
        inputDirectories.add(digest);
        linkDirectory(dirPath, digest, directoriesIndex);
      }
    }
  }

  private void linkDirectory(
      Path execPath,
      Digest digest,
      Map<Digest, Directory> directoriesIndex) throws IOException, InterruptedException {
    Path cachePath = workerContext.getBlobPathFactory().getDirectoryPath(digest, directoriesIndex);
    Files.createSymbolicLink(execPath, cachePath);
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
