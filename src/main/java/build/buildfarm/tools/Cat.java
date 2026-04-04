// Copyright 2019 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.tools;

import static build.buildfarm.common.grpc.Channels.createChannel;
import static build.buildfarm.common.resources.ResourceParser.parseUploadBlobRequest;
import static build.buildfarm.instance.Utils.getBlob;
import static build.buildfarm.server.services.OperationsService.LIST_OPERATIONS_MAXIMUM_PAGE_SIZE;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.ExecutedActionMetadata;
import build.bazel.remote.execution.v2.ExecutionStage;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.LogFile;
import build.bazel.remote.execution.v2.OutputDirectory;
import build.bazel.remote.execution.v2.OutputFile;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.ServerCapabilities;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.ProxyDirectoriesIndex;
import build.buildfarm.common.Write;
import build.buildfarm.common.resources.UploadBlobRequest;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.v1test.BatchWorkerProfilesResponse;
import build.buildfarm.v1test.Digest;
import build.buildfarm.v1test.DispatchedOperation;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.OperationTimesBetweenStages;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.QueuedOperation;
import build.buildfarm.v1test.QueuedOperationMetadata;
import build.buildfarm.v1test.StageInformation;
import build.buildfarm.v1test.Tree;
import build.buildfarm.v1test.WorkerExecutedMetadata;
import build.buildfarm.v1test.WorkerProfileMessage;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.Code;
import com.google.rpc.PreconditionFailure;
import com.google.rpc.RetryInfo;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import picocli.CommandLine;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

/** Display operations on the buildfarm server. */
@picocli.CommandLine.Command(
    name = "bf-cat",
    mixinStandardHelpOptions = true,
    description = "Request, retrieve, and display information related to REAPI services",
    subcommands = {
      Cat.CatWorkerProfile.class,
      Cat.CatCapabilities.class,
      Cat.CatOperations.class,
      Cat.CatBackplaneStatus.class,
      Cat.CatMissing.class,
      Cat.CatOperation.class,
      Cat.CatWatch.class,
      Cat.CatActionResult.class,
      Cat.CatDirectoryTree.class,
      Cat.CatTreeLayout.class,
      Cat.CatFile.class,
      Cat.CatAction.class,
      Cat.CatQueuedOperation.class,
      Cat.CatDumpQueuedOperation.class,
      Cat.CatREDirectoryTree.class,
      Cat.CatRETreeLayout.class,
      Cat.CatRECommand.class,
      Cat.CatDirectory.class,
      Cat.CatFetch.class,
      Cat.CatWriteStatus.class,
    })
class Cat implements Callable<Integer> {

  @Parameters(index = "0", description = CliConstants.BUILDFARM_HOST)
  String host;

  @Parameters(index = "1", description = CliConstants.INSTANCE_NAME)
  String instanceName;

  private static void printCapabilities(ServerCapabilities capabilities) {
    System.out.println(capabilities);
  }

  private static void printAction(ByteString actionBlob, DigestFunction.Value digestFunction) {
    Action action;
    try {
      action = Action.parseFrom(actionBlob);
    } catch (InvalidProtocolBufferException e) {
      System.out.println("Not an action");
      return;
    }
    printAction(0, action, digestFunction);
  }

  private static void printAction(int level, Action action, DigestFunction.Value digestFunction) {
    indentOut(
        level,
        "Command Digest: Command "
            + DigestUtil.toString(
                DigestUtil.fromDigest(action.getCommandDigest(), digestFunction)));
    indentOut(
        level,
        "Input Root Digest: Directory "
            + DigestUtil.toString(
                DigestUtil.fromDigest(action.getInputRootDigest(), digestFunction)));
    indentOut(level, "DoNotCache: " + (action.getDoNotCache() ? "true" : "false"));
    if (action.hasTimeout()) {
      indentOut(
          level,
          "Timeout: "
              + (action.getTimeout().getSeconds() + action.getTimeout().getNanos() / 1e9)
              + "s");
    }
    indentOut(level, "Salt: " + action.getSalt());
    indentOut(level, "Platform: " + action.getPlatform());
  }

  private static void printCommand(ByteString commandBlob) {
    Command command;
    try {
      command = Command.parseFrom(commandBlob);
    } catch (InvalidProtocolBufferException e) {
      System.out.println("Not a command");
      return;
    }
    printCommand(0, command);
  }

  private static void printCommand(int level, Command command) {
    for (String outputFile : command.getOutputFilesList()) {
      indentOut(level, "OutputFile: " + outputFile);
    }
    for (String outputDirectory : command.getOutputDirectoriesList()) {
      indentOut(level, "OutputDirectory: " + outputDirectory);
    }
    indentOut(level, "Arguments: ('" + String.join("', '", command.getArgumentsList()) + "')");
    if (!command.getEnvironmentVariablesList().isEmpty()) {
      indentOut(level, "Environment Variables:");
      for (Command.EnvironmentVariable env : command.getEnvironmentVariablesList()) {
        indentOut(level, "  " + env.getName() + "='" + env.getValue() + "'");
      }
    }
    if (!command.getPlatform().getPropertiesList().isEmpty()) {
      indentOut(level, "Platform:");
      for (build.bazel.remote.execution.v2.Platform.Property property :
          command.getPlatform().getPropertiesList()) {
        indentOut(level, "  " + property.getName() + "='" + property.getValue() + "'");
      }
    } else {
      indentOut(level, "Platform: (none)");
    }
    indentOut(level, "WorkingDirectory: " + command.getWorkingDirectory());
  }

  private static void indentOut(int level, String msg) {
    System.out.println(Strings.repeat("  ", level) + msg);
  }

  @SuppressWarnings("ConstantConditions")
  private static void printActionResult(
      ActionResult result, DigestFunction.Value digestFunction, int indentLevel) {
    for (OutputFile outputFile : result.getOutputFilesList()) {
      String attrs = "";
      if (outputFile.getIsExecutable()) {
        attrs += (attrs.length() == 0 ? "" : ",") + "executable";
      }
      if (attrs.length() != 0) {
        attrs = " (" + attrs + ")";
      }
      indentOut(
          indentLevel,
          "Output File: "
              + outputFile.getPath()
              + attrs
              + " File "
              + DigestUtil.toString(DigestUtil.fromDigest(outputFile.getDigest(), digestFunction)));
    }
    for (OutputDirectory outputDirectory : result.getOutputDirectoriesList()) {
      indentOut(
          indentLevel,
          "Output Directory: "
              + outputDirectory.getPath()
              + " Directory "
              + DigestUtil.toString(
                  DigestUtil.fromDigest(outputDirectory.getTreeDigest(), digestFunction)));
    }
    indentOut(indentLevel, "Exit Code: " + result.getExitCode());
    if (!result.getStdoutRaw().isEmpty()) {
      indentOut(indentLevel, "Stdout: " + result.getStdoutRaw().toStringUtf8());
    }
    if (result.hasStdoutDigest()) {
      indentOut(
          indentLevel,
          "Stdout Digest: "
              + DigestUtil.toString(
                  DigestUtil.fromDigest(result.getStdoutDigest(), digestFunction)));
    }
    if (!result.getStderrRaw().isEmpty()) {
      indentOut(indentLevel, "Stderr: " + result.getStderrRaw().toStringUtf8());
    }
    if (result.hasStderrDigest()) {
      indentOut(
          indentLevel,
          "Stderr Digest: "
              + DigestUtil.toString(
                  DigestUtil.fromDigest(result.getStderrDigest(), digestFunction)));
    }
    if (result.hasExecutionMetadata()) {
      indentOut(indentLevel, "ExecutionMetadata:");
      printExecutedActionMetadata(result.getExecutionMetadata(), indentLevel + 1);
    }
  }

  private static void printWorkerMetadata(WorkerExecutedMetadata metadata, int indentLevel) {
    indentOut(indentLevel, format("Fetched Bytes: %d", metadata.getFetchedBytes()));
    if (metadata.getLinkedInputDirectoriesCount() > 0) {
      indentOut(indentLevel, "Linked Input Directories:");
    }
    for (String linkedInputDirectory : metadata.getLinkedInputDirectoriesList()) {
      indentOut(indentLevel + 1, linkedInputDirectory);
    }
  }

  private static void printExecutedActionMetadata(
      ExecutedActionMetadata metadata, int indentLevel) {
    if (!metadata.getWorker().isEmpty()) {
      indentOut(indentLevel, "Worker: " + metadata.getWorker());
    }
    // TODO switch to spans/stalls
    if (metadata.hasQueuedTimestamp()) {
      indentOut(indentLevel, "Queued At: " + Timestamps.toString(metadata.getQueuedTimestamp()));
    }
    if (metadata.hasWorkerStartTimestamp()) {
      indentOut(
          indentLevel, "Worker Start: " + Timestamps.toString(metadata.getWorkerStartTimestamp()));
    }
    if (metadata.hasInputFetchStartTimestamp()) {
      indentOut(
          indentLevel,
          "Input Fetch Start: " + Timestamps.toString(metadata.getInputFetchStartTimestamp()));
    }
    if (metadata.hasInputFetchCompletedTimestamp()) {
      indentOut(
          indentLevel,
          "Input Fetch Completed: "
              + Timestamps.toString(metadata.getInputFetchCompletedTimestamp()));
    }
    if (metadata.hasExecutionStartTimestamp()) {
      indentOut(
          indentLevel,
          "Execution Start: " + Timestamps.toString(metadata.getExecutionStartTimestamp()));
    }
    if (metadata.hasExecutionCompletedTimestamp()) {
      indentOut(
          indentLevel,
          "Execution Completed: " + Timestamps.toString(metadata.getExecutionCompletedTimestamp()));
    }
    if (metadata.hasOutputUploadStartTimestamp()) {
      indentOut(
          indentLevel,
          "Output Upload Start: " + Timestamps.toString(metadata.getOutputUploadStartTimestamp()));
    }
    if (metadata.hasOutputUploadCompletedTimestamp()) {
      indentOut(
          indentLevel,
          "Output Upload Completed: "
              + Timestamps.toString(metadata.getOutputUploadCompletedTimestamp()));
    }
    if (metadata.hasWorkerCompletedTimestamp()) {
      indentOut(
          indentLevel,
          "Worker Completed: " + Timestamps.toString(metadata.getWorkerCompletedTimestamp()));
    }
    if (metadata.getAuxiliaryMetadataCount() > 0) {
      indentOut(indentLevel, "Auxiliary Metadata:");
    }
    for (Any auxiliary : metadata.getAuxiliaryMetadataList()) {
      if (auxiliary.is(WorkerExecutedMetadata.class)) {
        try {
          printWorkerMetadata(auxiliary.unpack(WorkerExecutedMetadata.class), indentLevel + 1);
        } catch (InvalidProtocolBufferException e) {
          // unlikely
          e.printStackTrace();
        }
      } else {
        indentOut(indentLevel + 1, "Unrecognized Metadata: " + auxiliary);
      }
    }
  }

  private static void printFindMissing(
      Instance instance,
      Iterable<build.bazel.remote.execution.v2.Digest> digests,
      DigestFunction.Value digestFunction)
      throws ExecutionException, InterruptedException {
    Stopwatch stopwatch = Stopwatch.createStarted();
    Iterable<build.bazel.remote.execution.v2.Digest> missingDigests =
        instance
            .findMissingBlobs(digests, digestFunction, RequestMetadata.getDefaultInstance())
            .get();
    long elapsedMicros = stopwatch.elapsed(TimeUnit.MICROSECONDS);

    boolean missing = false;
    for (build.bazel.remote.execution.v2.Digest missingDigest : missingDigests) {
      System.out.printf(
          "Missing: %s Took %gms%n",
          DigestUtil.toString(DigestUtil.fromDigest(missingDigest, digestFunction)),
          elapsedMicros / 1000.0f);
      missing = true;
    }
    if (!missing) {
      System.out.printf("Took %gms%n", elapsedMicros / 1000.0f);
    }
  }

  private static Tree fetchTree(Instance instance, Digest rootDigest) {
    Tree.Builder tree = Tree.newBuilder();
    String pageToken = Instance.SENTINEL_PAGE_TOKEN;

    do {
      pageToken = instance.getTree(rootDigest, 1024, pageToken, tree);
    } while (!pageToken.equals(Instance.SENTINEL_PAGE_TOKEN));

    return tree.build();
  }

  private static long computeDirectoryWeights(
      build.bazel.remote.execution.v2.Digest directoryDigest,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      Map<build.bazel.remote.execution.v2.Digest, Long> directoryWeights) {
    long weight = directoryDigest.getSizeBytes();
    Directory directory = directoriesIndex.get(directoryDigest);
    if (directory != null) {
      for (DirectoryNode dirNode : directory.getDirectoriesList()) {
        if (directoryWeights.containsKey(dirNode.getDigest())) {
          weight += directoryWeights.get(dirNode.getDigest());
        } else {
          weight +=
              computeDirectoryWeights(dirNode.getDigest(), directoriesIndex, directoryWeights);
        }
      }
      // filenodes are already computed
    }
    directoryWeights.put(directoryDigest, weight);
    return weight;
  }

  private static void printTreeAt(
      int indentLevel,
      Directory directory,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      DigestFunction.Value digestFunction,
      long totalWeight,
      Map<build.bazel.remote.execution.v2.Digest, Long> directoryWeights) {
    for (DirectoryNode dirNode : directory.getDirectoriesList()) {
      Directory subDirectory = directoriesIndex.get(dirNode.getDigest());
      long weight = directoryWeights.get(dirNode.getDigest());
      String displayName =
          format("%s/ %d (%d%%)", dirNode.getName(), weight, (int) (weight * 100.0 / totalWeight));
      indentOut(indentLevel, displayName);
      if (subDirectory == null) {
        indentOut(indentLevel + 1, "DIRECTORY MISSING FROM CAS");
      } else {
        printTreeAt(
            indentLevel + 1,
            subDirectory,
            directoriesIndex,
            digestFunction,
            totalWeight,
            directoryWeights);
      }
    }
    for (FileNode fileNode : directory.getFilesList()) {
      String name = fileNode.getName();
      String displayName =
          format(
              "%s%s %s",
              name,
              fileNode.getIsExecutable() ? "*" : "",
              DigestUtil.toString(DigestUtil.fromDigest(fileNode.getDigest(), digestFunction)));
      indentOut(indentLevel, displayName);
    }
  }

  private static void printRETreeLayout(
      DigestUtil digestUtil, build.bazel.remote.execution.v2.Tree reTree)
      throws IOException, InterruptedException {
    Tree tree = reTreeToTree(digestUtil, reTree);
    printTreeLayout(new ProxyDirectoriesIndex(tree.getDirectoriesMap()), tree.getRootDigest());
  }

  private static void printTreeLayout(
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex, Digest rootDigest) {
    Map<build.bazel.remote.execution.v2.Digest, Long> directoryWeights = Maps.newHashMap();
    long totalWeight =
        computeDirectoryWeights(
            DigestUtil.toDigest(rootDigest), directoriesIndex, directoryWeights);

    printTreeAt(
        0,
        directoriesIndex.get(DigestUtil.toDigest(rootDigest)),
        directoriesIndex,
        rootDigest.getDigestFunction(),
        totalWeight,
        directoryWeights);
  }

  private static Tree reTreeToTree(
      DigestUtil digestUtil, build.bazel.remote.execution.v2.Tree reTree) {
    Digest rootDigest = digestUtil.compute(reTree.getRoot());
    Tree.Builder tree = Tree.newBuilder().setRootDigest(rootDigest);
    tree.putDirectories(rootDigest.getHash(), reTree.getRoot());
    for (Directory directory : reTree.getChildrenList()) {
      tree.putDirectories(digestUtil.compute(directory).getHash(), directory);
    }
    return tree.build();
  }

  private static void printREDirectoryTree(
      DigestUtil digestUtil, build.bazel.remote.execution.v2.Tree reTree) {
    Tree tree = reTreeToTree(digestUtil, reTree);
    printTree(0, tree, tree.getRootDigest());
  }

  private static void printDirectoryTree(Instance instance, Digest rootDigest)
      throws IOException, InterruptedException {
    printTree(0, fetchTree(instance, rootDigest), rootDigest);
  }

  private static void printTree(int level, Tree tree, Digest rootDigest) {
    indentOut(level, "Directory (Root): " + rootDigest);
    for (Map.Entry<String, Directory> entry : tree.getDirectoriesMap().entrySet()) {
      System.out.println("Directory: " + entry.getKey());
      printDirectory(1, entry.getValue(), rootDigest.getDigestFunction());
    }
  }

  private static void printQueuedOperation(ByteString blob, DigestUtil digestUtil) {
    QueuedOperation queuedOperation;
    try {
      queuedOperation = QueuedOperation.parseFrom(blob);
    } catch (InvalidProtocolBufferException e) {
      System.out.println("Not a QueuedOperation");
      return;
    }
    System.out.println("QueuedOperation:");
    System.out.println(
        "  Action: " + DigestUtil.toString(digestUtil.compute(queuedOperation.getAction())));
    printAction(2, queuedOperation.getAction(), digestUtil.getDigestFunction());
    System.out.println("  Command:");
    printCommand(2, queuedOperation.getCommand());
    System.out.println("  Tree:");
    printTree(2, queuedOperation.getTree(), queuedOperation.getTree().getRootDigest());
  }

  private static void dumpQueuedOperation(ByteString blob, DigestUtil digestUtil)
      throws IOException {
    QueuedOperation queuedOperation;
    try {
      queuedOperation = QueuedOperation.parseFrom(blob);
    } catch (InvalidProtocolBufferException e) {
      System.out.println("Not a QueuedOperation");
      return;
    }
    ImmutableList.Builder<Message> messages = ImmutableList.builder();
    messages.add(queuedOperation.getAction());
    messages.add(queuedOperation.getCommand());
    messages.addAll(queuedOperation.getTree().getDirectoriesMap().values());
    Path blobs = Path.of("blobs");
    for (Message message : messages.build()) {
      Digest digest = digestUtil.compute(message);
      try (OutputStream out =
          Files.newOutputStream(blobs.resolve(digest.getHash() + "_" + digest.getSize()))) {
        message.toByteString().writeTo(out);
      }
    }
  }

  private static void printDirectory(
      ByteString directoryBlob, DigestFunction.Value digestFunction) {
    Directory directory;
    try {
      directory = Directory.parseFrom(directoryBlob);
    } catch (InvalidProtocolBufferException e) {
      System.out.println("Not a directory");
      return;
    }

    printDirectory(0, directory, digestFunction);
  }

  private static void printDirectory(
      int indentLevel, Directory directory, DigestFunction.Value digestFunction) {
    boolean filesUnsorted = false;
    String last = "";
    for (FileNode fileNode : directory.getFilesList()) {
      String name = fileNode.getName();
      String displayName = name;
      if (fileNode.getIsExecutable()) {
        displayName = "*" + name + "*";
      }
      indentOut(
          indentLevel,
          "File: "
              + displayName
              + " File "
              + DigestUtil.toString(DigestUtil.fromDigest(fileNode.getDigest(), digestFunction)));
      if (!filesUnsorted && last.compareTo(name) > 0) {
        filesUnsorted = true;
      } else {
        last = name;
      }
    }
    if (filesUnsorted) {
      System.err.println("ERROR: file list is not ordered");
    }

    boolean directoriesUnsorted = false;
    last = "";
    for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
      indentOut(
          indentLevel,
          "Dir: "
              + directoryNode.getName()
              + " Directory "
              + DigestUtil.toString(
                  DigestUtil.fromDigest(directoryNode.getDigest(), digestFunction)));
      if (!directoriesUnsorted && last.compareTo(directoryNode.getName()) > 0) {
        directoriesUnsorted = true;
      } else {
        last = directoryNode.getName();
      }
    }

    if (directoriesUnsorted) {
      System.err.println("ERROR: directory list is not ordered");
    }
  }

  private static void listOperations(Instance instance, Iterable<String> args) throws IOException {
    String pageToken = "";
    java.util.Iterator<String> arg = args.iterator();
    String filter = "";
    String name = "executions";
    if (arg.hasNext()) {
      filter = arg.next();
    }
    if (arg.hasNext()) {
      name = arg.next();
    }
    do {
      ImmutableList.Builder<Operation> operations = new ImmutableList.Builder<>();
      pageToken =
          instance.listOperations(
              name, LIST_OPERATIONS_MAXIMUM_PAGE_SIZE, pageToken, filter, operations::add);
      System.out.println(pageToken);
      System.out.println("Page size: " + operations.build().size());
      for (Operation operation : operations.build()) {
        printOperation(operation);
      }
    } while (!pageToken.equals(Instance.SENTINEL_PAGE_TOKEN));
  }

  private static void printRequestMetadata(RequestMetadata metadata) {
    System.out.println("ToolDetails:");
    System.out.println("  ToolName: " + metadata.getToolDetails().getToolName());
    System.out.println("  ToolVersion: " + metadata.getToolDetails().getToolVersion());
    System.out.println("ActionId: " + metadata.getActionId());
    System.out.println("ToolInvocationId: " + metadata.getToolInvocationId());
    System.out.println("CorrelatedInvocationsId: " + metadata.getCorrelatedInvocationsId());
    System.out.println("ActionMnemonic: " + metadata.getActionMnemonic());
    System.out.println("TargetId: " + metadata.getTargetId());
    System.out.println("ConfigurationId: " + metadata.getConfigurationId());
  }

  private static void printStatus(com.google.rpc.Status status)
      throws InvalidProtocolBufferException {
    System.out.println("  Code: " + Code.forNumber(status.getCode()));
    if (!status.getMessage().isEmpty()) {
      System.out.println("  Message: " + status.getMessage());
    }
    if (status.getDetailsCount() > 0) {
      System.out.println("  Details:");
      for (Any detail : status.getDetailsList()) {
        if (detail.is(RetryInfo.class)) {
          RetryInfo retryInfo = detail.unpack(RetryInfo.class);
          System.out.println(
              "    RetryDelay: "
                  + (retryInfo.getRetryDelay().getSeconds()
                      + retryInfo.getRetryDelay().getNanos() / 1000000000.0f));
        } else if (detail.is(PreconditionFailure.class)) {
          PreconditionFailure preconditionFailure = detail.unpack(PreconditionFailure.class);
          System.out.println("    PreconditionFailure:");
          for (PreconditionFailure.Violation violation : preconditionFailure.getViolationsList()) {
            System.out.println("      Violation: " + violation.getType());
            System.out.println("        Subject: " + violation.getSubject());
            System.out.println("        Description: " + violation.getDescription());
          }
        } else {
          System.out.println("    Unknown Detail: " + detail.getTypeUrl());
        }
      }
    }
  }

  private static void printExecuteResponse(
      ExecuteResponse response, DigestFunction.Value digestFunction)
      throws InvalidProtocolBufferException {
    printStatus(response.getStatus());
    if (response.hasResult()) {
      printActionResult(response.getResult(), digestFunction, 2);
      System.out.println("  CachedResult: " + (response.getCachedResult() ? "true" : "false"));
    }
    if (response.getServerLogsCount() > 0) {
      System.out.println("  Server Logs:");
      for (Map.Entry<String, LogFile> entry : response.getServerLogsMap().entrySet()) {
        LogFile logFile = entry.getValue();
        System.out.printf(
            "    %s: Log %s%s%n",
            entry.getKey(),
            DigestUtil.toString(DigestUtil.fromDigest(logFile.getDigest(), digestFunction)),
            logFile.getHumanReadable() ? " (human-readable)" : "");
      }
    }
    if (!response.getMessage().isEmpty()) {
      System.out.println("  Message: " + response.getMessage());
    }
  }

  private static ExecuteOperationMetadata executeEntryMetadata(
      ExecuteEntry executeEntry, ExecutionStage.Value stage) {
    return ExecuteOperationMetadata.newBuilder()
        .setStage(stage)
        .setActionDigest(DigestUtil.toDigest(executeEntry.getActionDigest()))
        .setDigestFunction(executeEntry.getActionDigest().getDigestFunction())
        .build();
  }

  private static void printOperation(Operation operation) {
    System.out.println("Operation: " + operation.getName());
    if (operation.getDone()) {
      System.out.println("Done");
    }
    DigestFunction.Value digestFunction = DigestFunction.Value.UNKNOWN;
    try {
      ExecuteOperationMetadata metadata;
      RequestMetadata requestMetadata;
      if (operation.getMetadata().is(QueuedOperationMetadata.class)) {
        QueuedOperationMetadata queuedOperationMetadata =
            operation.getMetadata().unpack(QueuedOperationMetadata.class);
        metadata = queuedOperationMetadata.getExecuteOperationMetadata();
        requestMetadata = queuedOperationMetadata.getRequestMetadata();
      } else if (operation.getMetadata().is(ExecuteEntry.class)) {
        ExecuteEntry executeEntry = operation.getMetadata().unpack(ExecuteEntry.class);
        metadata = executeEntryMetadata(executeEntry, ExecutionStage.Value.UNKNOWN);
        requestMetadata = executeEntry.getRequestMetadata();
      } else if (operation.getMetadata().is(QueueEntry.class)) {
        QueueEntry queueEntry = operation.getMetadata().unpack(QueueEntry.class);
        ExecuteEntry executeEntry = queueEntry.getExecuteEntry();
        metadata = executeEntryMetadata(executeEntry, ExecutionStage.Value.QUEUED);
        requestMetadata = executeEntry.getRequestMetadata();
      } else if (operation.getMetadata().is(DispatchedOperation.class)) {
        DispatchedOperation dispatchedOperation =
            operation.getMetadata().unpack(DispatchedOperation.class);
        ExecuteEntry executeEntry = dispatchedOperation.getQueueEntry().getExecuteEntry();
        metadata =
            executeEntryMetadata(
                executeEntry, ExecutionStage.Value.QUEUED); // latest we can know about here
        requestMetadata = executeEntry.getRequestMetadata();
      } else {
        metadata = operation.getMetadata().unpack(ExecuteOperationMetadata.class);
        requestMetadata = null;
      }
      if (metadata != null) {
        printExecutedActionMetadata(metadata.getPartialExecutionMetadata(), 1);
        System.out.println("Metadata:");
        System.out.println("  Stage: " + metadata.getStage());
        digestFunction = metadata.getDigestFunction();
        System.out.println(
            "  Action: "
                + DigestUtil.toString(
                    DigestUtil.fromDigest(metadata.getActionDigest(), digestFunction)));
        System.out.println("  Stdout Stream: " + metadata.getStdoutStreamName());
        System.out.println("  Stderr Stream: " + metadata.getStderrStreamName());
      }
      if (requestMetadata != null) {
        printRequestMetadata(requestMetadata);
      }
    } catch (InvalidProtocolBufferException e) {
      // System.out.println("  UNKNOWN TYPE: " + e.getMessage());
    }
    if (operation.getDone()) {
      switch (operation.getResultCase()) {
        case RESPONSE:
          System.out.println("Response:");
          try {
            printExecuteResponse(
                operation.getResponse().unpack(ExecuteResponse.class), digestFunction);
          } catch (InvalidProtocolBufferException e) {
            System.out.println("  UNKNOWN RESPONSE TYPE: " + operation.getResponse());
          }
          break;
        case ERROR:
          System.out.println("Error: " + Code.forNumber(operation.getError().getCode()));
          break;
        default:
          System.out.println("  UNKNOWN RESULT!");
          break;
      }
    }
  }

  private static void watchOperation(Instance instance, String operationName)
      throws InterruptedException {
    // need to build name
    try {
      UUID executionId = instance.unbindExecutions(operationName);
      instance
          .watchExecution(
              executionId,
              (operation) -> {
                if (operation == null) {
                  throw Status.NOT_FOUND.asRuntimeException();
                }
                printOperation(operation);
              })
          .get();
    } catch (ExecutionException e) {
      e.getCause().printStackTrace();
    }
  }

  static int deadlineSecondsForType(String type) {
    if (type.equals("Watch") || type.equals("Execute")) {
      return 60 * 60 * 24;
    }
    return 10;
  }

  private static void fetch(Instance instance, Iterable<String> uris) throws Exception {
    ListenableFuture<Digest> digest =
        instance.fetchBlob(
            uris,
            ImmutableMap.of(),
            /* expectedDigest= */ Digest.getDefaultInstance(),
            RequestMetadata.getDefaultInstance());
    System.out.println(DigestUtil.toString(digest.get()));
  }

  private static void batchWorkerProfiles(Instance instance, Iterable<String> names)
      throws Exception {
    BatchWorkerProfilesResponse responses = instance.batchWorkerProfiles(names).get();
    for (BatchWorkerProfilesResponse.Response response : responses.getResponsesList()) {
      System.out.println("Worker: " + response.getWorkerName());
      int code = response.getStatus().getCode();
      if (code == Code.OK.getNumber()) {
        printWorkerProfile(response.getProfile());
      } else {
        System.out.println("Error: " + Code.forNumber(code));
      }
    }
  }

  private static void printWorkerProfile(WorkerProfileMessage workerProfile) {
    System.out.println("\nWorkerProfile:");
    String strIntFormat = "%-50s : %d";
    long entryCount = workerProfile.getCasEntryCount();
    long unreferencedEntryCount = workerProfile.getCasUnreferencedEntryCount();
    System.out.printf((strIntFormat) + "%n", "Current Total Entry Count", entryCount);
    System.out.printf((strIntFormat) + "%n", "Current Total Size", workerProfile.getCasSize());
    System.out.printf((strIntFormat) + "%n", "Max Size", workerProfile.getCasMaxSize());
    System.out.printf((strIntFormat) + "%n", "Max Entry Size", workerProfile.getCasMaxEntrySize());
    System.out.printf(
        (strIntFormat) + "%n", "Current Unreferenced Entry Count", unreferencedEntryCount);
    if (entryCount != 0) {
      System.out.println(
          format(
              "%-50s : %2.1f%%",
              "Percentage of Unreferenced Entry", (100.0f * unreferencedEntryCount) / entryCount));
    }
    System.out.printf(
        (strIntFormat) + "%n",
        "Current DirectoryEntry Count",
        workerProfile.getCasDirectoryEntryCount());
    System.out.printf(
        (strIntFormat) + "%n",
        "Number of Evicted Entries",
        workerProfile.getCasEvictedEntryCount());
    System.out.printf(
        (strIntFormat) + "%n",
        "Total Evicted Entries size in Bytes",
        workerProfile.getCasEvictedEntrySize());

    List<StageInformation> stages = workerProfile.getStagesList();
    for (StageInformation stage : stages) {
      printStageInformation(stage);
    }

    List<OperationTimesBetweenStages> times = workerProfile.getTimesList();
    for (OperationTimesBetweenStages time : times) {
      printOperationTime(time);
    }
  }

  private static void printStageInformation(StageInformation stage) {
    System.out.printf("%s slots configured: %d%n", stage.getName(), stage.getSlotsConfigured());
    System.out.printf("%s slots used %d%n", stage.getName(), stage.getSlotsUsed());
    for (String operationName : stage.getOperationNamesList()) {
      System.out.printf("%s operation %s\n", stage.getName(), operationName);
    }
  }

  private static void printOperationTime(OperationTimesBetweenStages time) {
    String periodInfo = "\nIn last ";
    switch ((int) time.getPeriod().getSeconds()) {
      case 60:
        periodInfo += "1 minute";
        break;
      case 600:
        periodInfo += "10 minutes";
        break;
      case 3600:
        periodInfo += "1 hour";
        break;
      case 10800:
        periodInfo += "3 hours";
        break;
      case 86400:
        periodInfo += "24 hours";
        break;
      default:
        System.out.println("The period is UNKNOWN: " + time.getPeriod().getSeconds());
        periodInfo = periodInfo + time.getPeriod().getSeconds() + " seconds";
        break;
    }

    periodInfo += ":";
    System.out.println(periodInfo);
    System.out.println("Number of operations completed: " + time.getOperationCount());
    String strStrNumFormat = "%-28s -> %-28s : %12.2f ms";
    System.out.printf(
        (strStrNumFormat) + "%n",
        "Queued",
        "MatchStage",
        durationToMillis(time.getQueuedToMatch()));
    System.out.printf(
        (strStrNumFormat) + "%n",
        "MatchStage",
        "InputFetchStage start",
        durationToMillis(time.getMatchToInputFetchStart()));
    System.out.printf(
        (strStrNumFormat) + "%n",
        "InputFetchStage Start",
        "InputFetchStage Complete",
        durationToMillis(time.getInputFetchStartToComplete()));
    System.out.printf(
        (strStrNumFormat) + "%n",
        "InputFetchStage Complete",
        "ExecutionStage Start",
        durationToMillis(time.getInputFetchCompleteToExecutionStart()));
    System.out.printf(
        (strStrNumFormat) + "%n",
        "ExecutionStage Start",
        "ExecutionStage Complete",
        durationToMillis(time.getExecutionStartToComplete()));
    System.out.printf(
        (strStrNumFormat) + "%n",
        "ExecutionStage Complete",
        "ReportResultStage Start",
        durationToMillis(time.getExecutionCompleteToOutputUploadStart()));
    System.out.printf(
        (strStrNumFormat) + "%n",
        "OutputUploadStage Start",
        "OutputUploadStage Complete",
        durationToMillis(time.getOutputUploadStartToComplete()));
    System.out.println();
  }

  private static float durationToMillis(Duration d) {
    return Durations.toNanos(d) / (1000.0f * 1000.0f);
  }

  @Override
  public Integer call() {
    // Parent command: print usage if no subcommand given
    CommandLine.usage(this, System.err);
    return 0;
  }

  Instance createInstance() {
    ManagedChannel channel = createChannel(host);
    return new StubInstance(instanceName, "bf-cat", channel, Durations.fromSeconds(10));
  }

  @SuppressWarnings("ThrowFromFinallyBlock")
  static int runWithDeadline(int deadlineSeconds, Callable<Integer> action) throws Exception {
    ScheduledExecutorService service = newSingleThreadScheduledExecutor();
    Context.CancellableContext ctx =
        Context.current().withDeadlineAfter(deadlineSeconds, TimeUnit.SECONDS, service);
    Context prevContext = ctx.attach();
    try {
      return action.call();
    } finally {
      ctx.cancel(null);
      ctx.detach(prevContext);
      if (!shutdownAndAwaitTermination(service, 1, TimeUnit.SECONDS)) {
        throw new RuntimeException("could not shut down service");
      }
    }
  }

  @picocli.CommandLine.Command(
      name = "WorkerProfile",
      mixinStandardHelpOptions = true,
      description = "Status including Execution and CAS statistics (Worker only)")
  static class CatWorkerProfile implements Callable<Integer> {
    @ParentCommand private Cat parent;

    @Parameters(arity = "0..*", description = "Worker names")
    private List<String> names;

    @Override
    public Integer call() throws Exception {
      return runWithDeadline(10, this::run);
    }

    private int run() throws Exception {
      Instance instance = parent.createInstance();
      try {
        batchWorkerProfiles(instance, names != null ? names : ImmutableList.of());
      } finally {
        instance.stop();
      }
      return 0;
    }
  }

  @picocli.CommandLine.Command(
      name = "Capabilities",
      mixinStandardHelpOptions = true,
      description = "List remote capabilities")
  static class CatCapabilities implements Callable<Integer> {
    @ParentCommand private Cat parent;

    @Override
    public Integer call() throws Exception {
      return runWithDeadline(10, this::run);
    }

    private int run() throws Exception {
      Instance instance = parent.createInstance();
      try {
        ServerCapabilities capabilities = instance.getCapabilities();
        printCapabilities(capabilities);
      } finally {
        instance.stop();
      }
      return 0;
    }
  }

  @picocli.CommandLine.Command(
      name = "Operations",
      mixinStandardHelpOptions = true,
      description = "List longrunning operations")
  static class CatOperations implements Callable<Integer> {
    @ParentCommand private Cat parent;

    @Parameters(arity = "0..*", description = "Filter and name arguments")
    private List<String> args;

    @Override
    public Integer call() throws Exception {
      return runWithDeadline(10, this::run);
    }

    private int run() throws Exception {
      Instance instance = parent.createInstance();
      try {
        System.out.println("Listing Operations");
        listOperations(instance, args != null ? args : ImmutableList.of());
      } finally {
        instance.stop();
      }
      return 0;
    }
  }

  @picocli.CommandLine.Command(
      name = "BackplaneStatus",
      mixinStandardHelpOptions = true,
      description = "Acquire backplane status")
  static class CatBackplaneStatus implements Callable<Integer> {
    @ParentCommand private Cat parent;

    @Override
    public Integer call() throws Exception {
      return runWithDeadline(10, this::run);
    }

    private int run() throws Exception {
      Instance instance = parent.createInstance();
      try {
        System.out.println(instance.backplaneStatus());
      } finally {
        instance.stop();
      }
      return 0;
    }
  }

  @picocli.CommandLine.Command(
      name = "Missing",
      mixinStandardHelpOptions = true,
      description = "Query missing blob [digests...]")
  static class CatMissing implements Callable<Integer> {
    @ParentCommand private Cat parent;

    @Parameters(arity = "0..*", description = "Digests to check")
    private List<String> digestStrings;

    @Override
    public Integer call() throws Exception {
      return runWithDeadline(10, this::run);
    }

    private int run() throws Exception {
      Instance instance = parent.createInstance();
      try {
        ImmutableList<Digest> digests =
            (digestStrings != null ? digestStrings : ImmutableList.<String>of())
                .stream().map(DigestUtil::parseDigest).collect(ImmutableList.toImmutableList());

        if (!digests.isEmpty()) {
          printFindMissing(
              instance,
              Iterables.transform(digests, DigestUtil::toDigest),
              digests.getFirst().getDigestFunction());
        }
      } finally {
        instance.stop();
      }
      return 0;
    }
  }

  @picocli.CommandLine.Command(
      name = "Operation",
      mixinStandardHelpOptions = true,
      description = "Current status of Operation [names...]")
  static class CatOperation implements Callable<Integer> {
    @ParentCommand private Cat parent;

    @Parameters(arity = "1..*", description = "Operation names")
    private List<String> operationNames;

    @Override
    public Integer call() throws Exception {
      return runWithDeadline(10, this::run);
    }

    private int run() throws Exception {
      Instance instance = parent.createInstance();
      try {
        for (String operationName : operationNames) {
          printOperation(instance.getOperation(operationName));
        }
      } finally {
        instance.stop();
      }
      return 0;
    }
  }

  @picocli.CommandLine.Command(
      name = "Watch",
      mixinStandardHelpOptions = true,
      description = "Wait for updates on Operation [names...] until it is completed")
  static class CatWatch implements Callable<Integer> {
    @ParentCommand private Cat parent;

    @Parameters(arity = "1..*", description = "Operation names")
    private List<String> operationNames;

    @Override
    public Integer call() throws Exception {
      return runWithDeadline(deadlineSecondsForType("Watch"), this::run);
    }

    private int run() throws Exception {
      Instance instance = parent.createInstance();
      try {
        for (String operationName : operationNames) {
          watchOperation(instance, operationName);
        }
      } finally {
        instance.stop();
      }
      return 0;
    }
  }

  @picocli.CommandLine.Command(
      name = "ActionResult",
      mixinStandardHelpOptions = true,
      description = "Get results of Action [digests...] from the ActionCache")
  static class CatActionResult implements Callable<Integer> {
    @ParentCommand private Cat parent;

    @Parameters(arity = "1..*", description = "Digests")
    private List<String> digestStrings;

    @Override
    public Integer call() throws Exception {
      return runWithDeadline(10, this::run);
    }

    private int run() throws Exception {
      Instance instance = parent.createInstance();
      try {
        for (Digest digest :
            digestStrings.stream().map(DigestUtil::parseDigest).collect(Collectors.toList())) {
          ActionResult actionResult =
              instance
                  .getActionResult(
                      DigestUtil.asActionKey(digest), RequestMetadata.getDefaultInstance())
                  .get();
          if (actionResult != null) {
            printActionResult(actionResult, digest.getDigestFunction(), 0);
          } else {
            System.out.println("ActionResult not found for " + DigestUtil.toString(digest));
          }
        }
      } finally {
        instance.stop();
      }
      return 0;
    }
  }

  @picocli.CommandLine.Command(
      name = "DirectoryTree",
      mixinStandardHelpOptions = true,
      description = "Simple recursive root Directory [digests...], missing directories will error")
  static class CatDirectoryTree implements Callable<Integer> {
    @ParentCommand private Cat parent;

    @Parameters(arity = "1..*", description = "Digests")
    private List<String> digestStrings;

    @Override
    public Integer call() throws Exception {
      return runWithDeadline(10, this::run);
    }

    private int run() throws Exception {
      Instance instance = parent.createInstance();
      try {
        for (Digest digest :
            digestStrings.stream().map(DigestUtil::parseDigest).collect(Collectors.toList())) {
          printDirectoryTree(instance, digest);
        }
      } finally {
        instance.stop();
      }
      return 0;
    }
  }

  @picocli.CommandLine.Command(
      name = "TreeLayout",
      mixinStandardHelpOptions = true,
      description =
          "Rich tree layout of root directory [digests...], with weighting and missing tolerance")
  static class CatTreeLayout implements Callable<Integer> {
    @ParentCommand private Cat parent;

    @Parameters(arity = "1..*", description = "Digests")
    private List<String> digestStrings;

    @Override
    public Integer call() throws Exception {
      return runWithDeadline(10, this::run);
    }

    private int run() throws Exception {
      Instance instance = parent.createInstance();
      try {
        for (Digest digest :
            digestStrings.stream().map(DigestUtil::parseDigest).collect(Collectors.toList())) {
          Tree tree = fetchTree(instance, digest);
          printTreeLayout(new ProxyDirectoriesIndex(tree.getDirectoriesMap()), digest);
        }
      } finally {
        instance.stop();
      }
      return 0;
    }
  }

  @picocli.CommandLine.Command(
      name = "File",
      mixinStandardHelpOptions = true,
      description = "Raw content of blob [digests...] file (60s time limit)")
  static class CatFile implements Callable<Integer> {
    @ParentCommand private Cat parent;

    @Parameters(arity = "1..*", description = "Digests")
    private List<String> digestStrings;

    @Override
    public Integer call() throws Exception {
      return runWithDeadline(10, this::run);
    }

    private int run() throws Exception {
      Instance instance = parent.createInstance();
      try {
        for (Digest digest :
            digestStrings.stream().map(DigestUtil::parseDigest).collect(Collectors.toList())) {
          try (InputStream in =
              instance.newBlobInput(
                  Compressor.Value.IDENTITY,
                  digest,
                  0,
                  60,
                  TimeUnit.SECONDS,
                  RequestMetadata.getDefaultInstance())) {
            ByteStreams.copy(in, System.out);
          }
        }
      } finally {
        instance.stop();
      }
      return 0;
    }
  }

  @picocli.CommandLine.Command(
      name = "Action",
      mixinStandardHelpOptions = true,
      description = "Definition of Action [digests...]")
  static class CatAction implements Callable<Integer> {
    @ParentCommand private Cat parent;

    @Parameters(arity = "1..*", description = "Digests")
    private List<String> digestStrings;

    @Override
    public Integer call() throws Exception {
      return runWithDeadline(10, this::run);
    }

    private int run() throws Exception {
      Instance instance = parent.createInstance();
      try {
        for (Digest digest :
            digestStrings.stream().map(DigestUtil::parseDigest).collect(Collectors.toList())) {
          ByteString blob =
              getBlob(
                  instance,
                  Compressor.Value.IDENTITY,
                  digest,
                  RequestMetadata.getDefaultInstance());
          printAction(blob, digest.getDigestFunction());
        }
      } finally {
        instance.stop();
      }
      return 0;
    }
  }

  @picocli.CommandLine.Command(
      name = "QueuedOperation",
      mixinStandardHelpOptions = true,
      description = "Definition of prepared operation [digests...] for execution")
  static class CatQueuedOperation implements Callable<Integer> {
    @ParentCommand private Cat parent;

    @Parameters(arity = "1..*", description = "Digests")
    private List<String> digestStrings;

    @Override
    public Integer call() throws Exception {
      return runWithDeadline(10, this::run);
    }

    private int run() throws Exception {
      Instance instance = parent.createInstance();
      try {
        for (Digest digest :
            digestStrings.stream().map(DigestUtil::parseDigest).collect(Collectors.toList())) {
          ByteString blob =
              getBlob(
                  instance,
                  Compressor.Value.IDENTITY,
                  digest,
                  RequestMetadata.getDefaultInstance());
          printQueuedOperation(blob, new DigestUtil(HashFunction.get(digest.getDigestFunction())));
        }
      } finally {
        instance.stop();
      }
      return 0;
    }
  }

  @picocli.CommandLine.Command(
      name = "DumpQueuedOperation",
      mixinStandardHelpOptions = true,
      description =
          "Binary QueuedOperation [digests...] content, suitable for retention in local 'blobs'"
              + " directory and use with bf-executor")
  static class CatDumpQueuedOperation implements Callable<Integer> {
    @ParentCommand private Cat parent;

    @Parameters(arity = "1..*", description = "Digests")
    private List<String> digestStrings;

    @Override
    public Integer call() throws Exception {
      return runWithDeadline(10, this::run);
    }

    private int run() throws Exception {
      Instance instance = parent.createInstance();
      try {
        for (Digest digest :
            digestStrings.stream().map(DigestUtil::parseDigest).collect(Collectors.toList())) {
          ByteString blob =
              getBlob(
                  instance,
                  Compressor.Value.IDENTITY,
                  digest,
                  RequestMetadata.getDefaultInstance());
          dumpQueuedOperation(blob, new DigestUtil(HashFunction.get(digest.getDigestFunction())));
        }
      } finally {
        instance.stop();
      }
      return 0;
    }
  }

  @picocli.CommandLine.Command(
      name = "REDirectoryTree",
      mixinStandardHelpOptions = true,
      description = "Vanilla REAPI Tree [digests...] description (NOT buildfarm Tree with index)")
  static class CatREDirectoryTree implements Callable<Integer> {
    @ParentCommand private Cat parent;

    @Parameters(arity = "1..*", description = "Digests")
    private List<String> digestStrings;

    @Override
    public Integer call() throws Exception {
      return runWithDeadline(10, this::run);
    }

    private int run() throws Exception {
      Instance instance = parent.createInstance();
      try {
        for (Digest digest :
            digestStrings.stream().map(DigestUtil::parseDigest).collect(Collectors.toList())) {
          ByteString blob =
              getBlob(
                  instance,
                  Compressor.Value.IDENTITY,
                  digest,
                  RequestMetadata.getDefaultInstance());
          printREDirectoryTree(
              new DigestUtil(HashFunction.get(digest.getDigestFunction())),
              build.bazel.remote.execution.v2.Tree.parseFrom(blob));
        }
      } finally {
        instance.stop();
      }
      return 0;
    }
  }

  @picocli.CommandLine.Command(
      name = "RETreeLayout",
      mixinStandardHelpOptions = true,
      description = "Vanilla REAPI Tree [digests...] with rich weighting like TreeLayout")
  static class CatRETreeLayout implements Callable<Integer> {
    @ParentCommand private Cat parent;

    @Parameters(arity = "1..*", description = "Digests")
    private List<String> digestStrings;

    @Override
    public Integer call() throws Exception {
      return runWithDeadline(10, this::run);
    }

    private int run() throws Exception {
      Instance instance = parent.createInstance();
      try {
        for (Digest digest :
            digestStrings.stream().map(DigestUtil::parseDigest).collect(Collectors.toList())) {
          ByteString blob =
              getBlob(
                  instance,
                  Compressor.Value.IDENTITY,
                  digest,
                  RequestMetadata.getDefaultInstance());
          printRETreeLayout(
              new DigestUtil(HashFunction.get(digest.getDigestFunction())),
              build.bazel.remote.execution.v2.Tree.parseFrom(blob));
        }
      } finally {
        instance.stop();
      }
      return 0;
    }
  }

  @picocli.CommandLine.Command(
      name = "Command",
      mixinStandardHelpOptions = true,
      description = "Definition of Command [digests...]")
  static class CatRECommand implements Callable<Integer> {
    @ParentCommand private Cat parent;

    @Parameters(arity = "1..*", description = "Digests")
    private List<String> digestStrings;

    @Override
    public Integer call() throws Exception {
      return runWithDeadline(10, this::run);
    }

    private int run() throws Exception {
      Instance instance = parent.createInstance();
      try {
        for (Digest digest :
            digestStrings.stream().map(DigestUtil::parseDigest).collect(Collectors.toList())) {
          ByteString blob =
              getBlob(
                  instance,
                  Compressor.Value.IDENTITY,
                  digest,
                  RequestMetadata.getDefaultInstance());
          printCommand(blob);
        }
      } finally {
        instance.stop();
      }
      return 0;
    }
  }

  @picocli.CommandLine.Command(
      name = "Directory",
      mixinStandardHelpOptions = true,
      description = "Definition of Directory [digests...]")
  static class CatDirectory implements Callable<Integer> {
    @ParentCommand private Cat parent;

    @Parameters(arity = "1..*", description = "Digests")
    private List<String> digestStrings;

    @Override
    public Integer call() throws Exception {
      return runWithDeadline(10, this::run);
    }

    private int run() throws Exception {
      Instance instance = parent.createInstance();
      try {
        for (Digest digest :
            digestStrings.stream().map(DigestUtil::parseDigest).collect(Collectors.toList())) {
          ByteString blob =
              getBlob(
                  instance,
                  Compressor.Value.IDENTITY,
                  digest,
                  RequestMetadata.getDefaultInstance());
          printDirectory(blob, digest.getDigestFunction());
        }
      } finally {
        instance.stop();
      }
      return 0;
    }
  }

  @picocli.CommandLine.Command(
      name = "Fetch",
      mixinStandardHelpOptions = true,
      description = "Request an url fetch via the assets API")
  static class CatFetch implements Callable<Integer> {
    @ParentCommand private Cat parent;

    @Parameters(arity = "1..*", description = "URIs to fetch")
    private List<String> uris;

    @Override
    public Integer call() throws Exception {
      return runWithDeadline(10, this::run);
    }

    private int run() throws Exception {
      Instance instance = parent.createInstance();
      try {
        fetch(instance, uris);
      } finally {
        instance.stop();
      }
      return 0;
    }
  }

  @picocli.CommandLine.Command(
      name = "WriteStatus",
      mixinStandardHelpOptions = true,
      description = "Retrieve write status")
  static class CatWriteStatus implements Callable<Integer> {
    @ParentCommand private Cat parent;

    @Parameters(arity = "1..*", description = "Resource names")
    private List<String> resourceNames;

    @Override
    public Integer call() throws Exception {
      return runWithDeadline(10, this::run);
    }

    private int run() throws Exception {
      Instance instance = parent.createInstance();
      try {
        for (String resourceName : resourceNames) {
          UploadBlobRequest request = parseUploadBlobRequest(resourceName);
          Write write =
              instance.getBlobWrite(
                  request.getBlob().getCompressor(),
                  request.getBlob().getDigest(),
                  UUID.fromString(request.getUuid()),
                  RequestMetadata.getDefaultInstance());
          System.out.println("resourceName: " + resourceName);
          System.out.println("committedSize: " + write.getCommittedSize());
          System.out.println("complete: " + write.isComplete());
        }
      } finally {
        instance.stop();
      }
      return 0;
    }
  }

  public static void main(String[] args) {
    int exitCode = new CommandLine(new Cat()).execute(args);
    System.exit(exitCode);
  }
}
