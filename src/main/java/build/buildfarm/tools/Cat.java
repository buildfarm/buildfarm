// Copyright 2019 The Bazel Authors. All rights reserved.
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
import static build.buildfarm.instance.Utils.getBlob;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Command.EnvironmentVariable;
import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.ExecutedActionMetadata;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.OutputDirectory;
import build.bazel.remote.execution.v2.OutputFile;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.ServerCapabilities;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.ProxyDirectoriesIndex;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.v1test.CompletedOperationMetadata;
import build.buildfarm.v1test.ExecutingOperationMetadata;
import build.buildfarm.v1test.OperationTimesBetweenStages;
import build.buildfarm.v1test.QueuedOperation;
import build.buildfarm.v1test.QueuedOperationMetadata;
import build.buildfarm.v1test.StageInformation;
import build.buildfarm.v1test.Tree;
import build.buildfarm.v1test.WorkerProfileMessage;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
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
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

class Cat {
  private static void printCapabilities(ServerCapabilities capabilities) {
    System.out.println(capabilities);
  }

  private static void printAction(ByteString actionBlob) {
    Action action;
    try {
      action = Action.parseFrom(actionBlob);
    } catch (InvalidProtocolBufferException e) {
      System.out.println("Not an action");
      return;
    }
    printAction(0, action);
  }

  private static void printAction(int level, Action action) {
    indentOut(
        level + 1, "Command Digest: Command " + DigestUtil.toString(action.getCommandDigest()));
    indentOut(
        level, "Input Root Digest: Directory " + DigestUtil.toString(action.getInputRootDigest()));
    indentOut(level, "DoNotCache: " + (action.getDoNotCache() ? "true" : "false"));
    if (action.hasTimeout()) {
      indentOut(
          level,
          "Timeout: "
              + (action.getTimeout().getSeconds() + action.getTimeout().getNanos() / 1e9)
              + "s");
    }
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
    // FIXME platform
    indentOut(level, "Arguments: ('" + String.join("', '", command.getArgumentsList()) + "')");
    if (!command.getEnvironmentVariablesList().isEmpty()) {
      indentOut(level, "Environment Variables:");
      for (EnvironmentVariable env : command.getEnvironmentVariablesList()) {
        indentOut(level, "  " + env.getName() + "='" + env.getValue() + "'");
      }
    }
    indentOut(level, "Platform: " + command.getPlatform());
    indentOut(level, "WorkingDirectory: " + command.getWorkingDirectory());
  }

  private static void indentOut(int level, String msg) {
    System.out.println(Strings.repeat("  ", level) + msg);
  }

  @SuppressWarnings("ConstantConditions")
  private static void printActionResult(ActionResult result, int indentLevel) {
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
              + DigestUtil.toString(outputFile.getDigest()));
    }
    for (OutputDirectory outputDirectory : result.getOutputDirectoriesList()) {
      indentOut(
          indentLevel,
          "Output Directory: "
              + outputDirectory.getPath()
              + " Directory "
              + DigestUtil.toString(outputDirectory.getTreeDigest()));
    }
    indentOut(indentLevel, "Exit Code: " + result.getExitCode());
    if (!result.getStdoutRaw().isEmpty()) {
      indentOut(indentLevel, "Stdout: " + result.getStdoutRaw().toStringUtf8());
    }
    if (result.hasStdoutDigest()) {
      indentOut(indentLevel, "Stdout Digest: " + DigestUtil.toString(result.getStdoutDigest()));
    }
    if (!result.getStderrRaw().isEmpty()) {
      indentOut(indentLevel, "Stderr: " + result.getStderrRaw().toStringUtf8());
    }
    if (result.hasStderrDigest()) {
      indentOut(indentLevel, "Stderr Digest: " + DigestUtil.toString(result.getStderrDigest()));
    }
    if (result.hasExecutionMetadata()) {
      indentOut(indentLevel, "ExecutionMetadata:");
      ExecutedActionMetadata executedActionMetadata = result.getExecutionMetadata();
      indentOut(indentLevel + 1, "Worker: " + executedActionMetadata.getWorker());
      indentOut(
          indentLevel + 1,
          "Queued At: " + Timestamps.toString(executedActionMetadata.getQueuedTimestamp()));
      indentOut(
          indentLevel + 1,
          "Worker Start: " + Timestamps.toString(executedActionMetadata.getWorkerStartTimestamp()));
      indentOut(
          indentLevel + 1,
          "Input Fetch Start: "
              + Timestamps.toString(executedActionMetadata.getInputFetchStartTimestamp()));
      indentOut(
          indentLevel + 1,
          "Input Fetch Completed: "
              + Timestamps.toString(executedActionMetadata.getInputFetchCompletedTimestamp()));
      indentOut(
          indentLevel + 1,
          "Execution Start: "
              + Timestamps.toString(executedActionMetadata.getExecutionStartTimestamp()));
      indentOut(
          indentLevel + 1,
          "Execution Completed: "
              + Timestamps.toString(executedActionMetadata.getExecutionCompletedTimestamp()));
      indentOut(
          indentLevel + 1,
          "Output Upload Start: "
              + Timestamps.toString(executedActionMetadata.getOutputUploadStartTimestamp()));
      indentOut(
          indentLevel + 1,
          "Output Upload Completed: "
              + Timestamps.toString(executedActionMetadata.getOutputUploadCompletedTimestamp()));
      indentOut(
          indentLevel + 1,
          "Worker Completed: "
              + Timestamps.toString(executedActionMetadata.getWorkerCompletedTimestamp()));
    }
  }

  private static void printFindMissing(Instance instance, Iterable<Digest> digests)
      throws ExecutionException, InterruptedException {
    Stopwatch stopwatch = Stopwatch.createStarted();
    Iterable<Digest> missingDigests =
        instance.findMissingBlobs(digests, RequestMetadata.getDefaultInstance()).get();
    long elapsedMicros = stopwatch.elapsed(TimeUnit.MICROSECONDS);

    boolean missing = false;
    for (Digest missingDigest : missingDigests) {
      System.out.printf(
          "Missing: %s Took %gms%n", DigestUtil.toString(missingDigest), elapsedMicros / 1000.0f);
      missing = true;
    }
    if (!missing) {
      System.out.printf("Took %gms%n", elapsedMicros / 1000.0f);
    }
  }

  private static Tree fetchTree(Instance instance, Digest rootDigest) {
    Tree.Builder tree = Tree.newBuilder();
    String pageToken = "";

    do {
      pageToken = instance.getTree(rootDigest, 1024, pageToken, tree);
    } while (!pageToken.isEmpty());

    return tree.build();
  }

  private static long computeDirectoryWeights(
      Digest directoryDigest,
      Map<Digest, Directory> directoriesIndex,
      Map<Digest, Long> directoryWeights) {
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
      Map<Digest, Directory> directoriesIndex,
      long totalWeight,
      Map<Digest, Long> directoryWeights) {
    for (DirectoryNode dirNode : directory.getDirectoriesList()) {
      Directory subDirectory = directoriesIndex.get(dirNode.getDigest());
      long weight = directoryWeights.get(dirNode.getDigest());
      String displayName =
          format("%s/ %d (%d%%)", dirNode.getName(), weight, (int) (weight * 100.0 / totalWeight));
      indentOut(indentLevel, displayName);
      if (subDirectory == null) {
        indentOut(indentLevel + 1, "DIRECTORY MISSING FROM CAS");
      } else {
        printTreeAt(indentLevel + 1, subDirectory, directoriesIndex, totalWeight, directoryWeights);
      }
    }
    for (FileNode fileNode : directory.getFilesList()) {
      String name = fileNode.getName();
      String displayName =
          format(
              "%s%s %s",
              name,
              fileNode.getIsExecutable() ? "*" : "",
              DigestUtil.toString(fileNode.getDigest()));
      indentOut(indentLevel, displayName);
    }
  }

  private static void printRETreeLayout(
      DigestUtil digestUtil, build.bazel.remote.execution.v2.Tree reTree)
      throws IOException, InterruptedException {
    Tree tree = reTreeToTree(digestUtil, reTree);
    printTreeLayout(new ProxyDirectoriesIndex(tree.getDirectoriesMap()), tree.getRootDigest());
  }

  private static void printTreeLayout(Map<Digest, Directory> directoriesIndex, Digest rootDigest) {
    Map<Digest, Long> directoryWeights = Maps.newHashMap();
    long totalWeight = computeDirectoryWeights(rootDigest, directoriesIndex, directoryWeights);

    printTreeAt(
        0, directoriesIndex.get(rootDigest), directoriesIndex, totalWeight, directoryWeights);
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
      printDirectory(1, entry.getValue());
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
    printAction(2, queuedOperation.getAction());
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
    Path blobs = Paths.get("blobs");
    for (Message message : messages.build()) {
      Digest digest = digestUtil.compute(message);
      try (OutputStream out =
          Files.newOutputStream(blobs.resolve(digest.getHash() + "_" + digest.getSizeBytes()))) {
        message.toByteString().writeTo(out);
      }
    }
  }

  private static void printDirectory(ByteString directoryBlob) {
    Directory directory;
    try {
      directory = Directory.parseFrom(directoryBlob);
    } catch (InvalidProtocolBufferException e) {
      System.out.println("Not a directory");
      return;
    }

    printDirectory(0, directory);
  }

  private static void printDirectory(int indentLevel, Directory directory) {
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
          "File: " + displayName + " File " + DigestUtil.toString(fileNode.getDigest()));
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
              + DigestUtil.toString(directoryNode.getDigest()));
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

  private static void listOperations(Instance instance) {
    String pageToken = "";
    int limit = 10;
    int count = 0;
    do {
      ImmutableList.Builder<Operation> operations = new ImmutableList.Builder<>();
      pageToken = instance.listOperations(1024, pageToken, "", operations);
      System.out.println(pageToken);
      System.out.println("Page size: " + operations.build().size());
      for (Operation operation : operations.build()) {
        printOperation(operation);
      }
      if (++count == limit) {
        System.out.println("page limit reached");
        break;
      }
    } while (!pageToken.equals(""));
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

  private static void printExecuteResponse(ExecuteResponse response)
      throws InvalidProtocolBufferException {
    printStatus(response.getStatus());
    if (Code.forNumber(response.getStatus().getCode()) == Code.OK) {
      printActionResult(response.getResult(), 2);
      System.out.println("  CachedResult: " + (response.getCachedResult() ? "true" : "false"));
    }
    // FIXME server_logs
  }

  private static void printOperation(Operation operation) {
    System.out.println("Operation: " + operation.getName());
    System.out.println("Done: " + (operation.getDone() ? "true" : "false"));
    System.out.println("Metadata:");
    try {
      ExecuteOperationMetadata metadata;
      RequestMetadata requestMetadata;
      if (operation.getMetadata().is(QueuedOperationMetadata.class)) {
        QueuedOperationMetadata queuedOperationMetadata =
            operation.getMetadata().unpack(QueuedOperationMetadata.class);
        metadata = queuedOperationMetadata.getExecuteOperationMetadata();
        requestMetadata = queuedOperationMetadata.getRequestMetadata();
      } else if (operation.getMetadata().is(ExecutingOperationMetadata.class)) {
        ExecutingOperationMetadata executingMetadata =
            operation.getMetadata().unpack(ExecutingOperationMetadata.class);
        System.out.println("  Started At: " + new Date(executingMetadata.getStartedAt()));
        System.out.println("  Executing On: " + executingMetadata.getExecutingOn());
        metadata = executingMetadata.getExecuteOperationMetadata();
        requestMetadata = executingMetadata.getRequestMetadata();
      } else if (operation.getMetadata().is(CompletedOperationMetadata.class)) {
        CompletedOperationMetadata completedMetadata =
            operation.getMetadata().unpack(CompletedOperationMetadata.class);
        metadata = completedMetadata.getExecuteOperationMetadata();
        requestMetadata = completedMetadata.getRequestMetadata();
      } else {
        metadata = operation.getMetadata().unpack(ExecuteOperationMetadata.class);
        requestMetadata = null;
      }
      System.out.println("  Stage: " + metadata.getStage());
      System.out.println("  Action: " + DigestUtil.toString(metadata.getActionDigest()));
      System.out.println("  Stdout Stream: " + metadata.getStdoutStreamName());
      System.out.println("  Stderr Stream: " + metadata.getStderrStreamName());
      if (requestMetadata != null) {
        printRequestMetadata(requestMetadata);
      }
    } catch (InvalidProtocolBufferException e) {
      System.out.println("  UNKNOWN TYPE: " + e.getMessage());
    }
    if (operation.getDone()) {
      switch (operation.getResultCase()) {
        case RESPONSE:
          System.out.println("Response:");
          try {
            printExecuteResponse(operation.getResponse().unpack(ExecuteResponse.class));
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
    try {
      instance
          .watchOperation(
              operationName,
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

  private static void getWorkerProfile(Instance instance) {
    WorkerProfileMessage response = instance.getWorkerProfile();
    System.out.println("\nWorkerProfile:");
    String strIntFormat = "%-50s : %d";
    String strFloatFormat = "%-50s : %2.1f";
    long entryCount = response.getCasEntryCount();
    long unreferencedEntryCount = response.getCasUnreferencedEntryCount();
    System.out.printf((strIntFormat) + "%n", "Current Total Entry Count", entryCount);
    System.out.printf((strIntFormat) + "%n", "Current Total Size", response.getCasSize());
    System.out.printf((strIntFormat) + "%n", "Max Size", response.getCasMaxSize());
    System.out.printf((strIntFormat) + "%n", "Max Entry Size", response.getCasMaxEntrySize());
    System.out.printf(
        (strIntFormat) + "%n", "Current Unreferenced Entry Count", unreferencedEntryCount);
    if (entryCount != 0) {
      System.out.println(
          format(
                  strFloatFormat,
                  "Percentage of Unreferenced Entry",
                  100.0f * response.getCasEntryCount() / response.getCasUnreferencedEntryCount())
              + "%");
    }
    System.out.printf(
        (strIntFormat) + "%n",
        "Current DirectoryEntry Count",
        response.getCasDirectoryEntryCount());
    System.out.printf(
        (strIntFormat) + "%n", "Number of Evicted Entries", response.getCasEvictedEntryCount());
    System.out.printf(
        (strIntFormat) + "%n",
        "Total Evicted Entries size in Bytes",
        response.getCasEvictedEntrySize());

    List<StageInformation> stages = response.getStagesList();
    for (StageInformation stage : stages) {
      printStageInformation(stage);
    }

    List<OperationTimesBetweenStages> times = response.getTimesList();
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

  public static void main(String[] args) throws Exception {
    if (args.length < 4) {
      usage();
      System.exit(1);
    } else {
      String host = args[0];
      String instanceName = args[1];
      DigestUtil digestUtil = DigestUtil.forHash(args[2]);
      String type = args[3];
      main(host, instanceName, digestUtil, type, Iterables.skip(Lists.newArrayList(args), 4));
    }
  }

  @SuppressWarnings("ThrowFromFinallyBlock")
  private static void main(
      String host, String instanceName, DigestUtil digestUtil, String type, Iterable<String> args)
      throws Exception {
    ScheduledExecutorService service = newSingleThreadScheduledExecutor();
    Context.CancellableContext ctx =
        Context.current()
            .withDeadlineAfter(deadlineSecondsForType(type), TimeUnit.SECONDS, service);
    Context prevContext = ctx.attach();
    try {
      cancellableMain(host, instanceName, digestUtil, type, args);
    } finally {
      ctx.cancel(null);
      ctx.detach(prevContext);
      if (!shutdownAndAwaitTermination(service, 1, TimeUnit.SECONDS)) {
        throw new RuntimeException("could not shut down service");
      }
    }
  }

  abstract static class CatCommand {
    public abstract void run(Instance instance, Iterable<String> args) throws Exception;

    public abstract String description();

    public String name() {
      return getClass().getSimpleName();
    }
  }

  static class WorkerProfile extends CatCommand {
    @Override
    public String description() {
      return "Status including Execution and CAS statistics (Worker only)";
    }

    @Override
    public void run(Instance instance, Iterable<String> args) {
      getWorkerProfile(instance);
    }
  }

  static class Capabilities extends CatCommand {
    @Override
    public String description() {
      return "List remote capabilities";
    }

    @Override
    public void run(Instance instance, Iterable<String> args) {
      ServerCapabilities capabilities = instance.getCapabilities();
      printCapabilities(capabilities);
    }
  }

  static class Operations extends CatCommand {
    @Override
    public String description() {
      return "List longrunning operations";
    }

    @Override
    public void run(Instance instance, Iterable<String> args) {
      System.out.println("Listing Operations");
      listOperations(instance);
    }
  }

  static class BackplaneStatus extends CatCommand {
    @Override
    public String description() {
      return "Acquire backplane status";
    }

    @Override
    public void run(Instance instance, Iterable<String> args) {
      System.out.println(instance.backplaneStatus());
    }
  }

  static class Missing extends CatCommand {
    @Override
    public String description() {
      return "Query missing blob [digests...]";
    }

    @Override
    public void run(Instance instance, Iterable<String> args) throws Exception {
      printFindMissing(
          instance,
          StreamSupport.stream(args.spliterator(), false)
              .map(DigestUtil::parseDigest)
              .collect(Collectors.toList()));
    }
  }

  abstract static class OperationsCommand extends CatCommand {
    @Override
    public void run(Instance instance, Iterable<String> args) throws Exception {
      for (String operationName : args) {
        run(instance, operationName);
      }
    }

    protected abstract void run(Instance instance, String operationName) throws Exception;
  }

  static class CatOperation extends OperationsCommand {
    @Override
    public String name() {
      return "Operation";
    }

    @Override
    public String description() {
      return "Current status of Operation [names...]";
    }

    @Override
    protected void run(Instance instance, String operationName) {
      printOperation(instance.getOperation(operationName));
    }
  }

  static class Watch extends OperationsCommand {
    @Override
    public String description() {
      return "Wait for updates on Operation [names...] until it is completed";
    }

    @Override
    protected void run(Instance instance, String operationName) throws Exception {
      watchOperation(instance, operationName);
    }
  }

  abstract static class DigestsCommand extends CatCommand {
    @Override
    public void run(Instance instance, Iterable<String> args) throws Exception {
      for (Digest digest :
          StreamSupport.stream(args.spliterator(), false)
              .map(DigestUtil::parseDigest)
              .collect(Collectors.toList())) {
        run(instance, digest);
      }
    }

    protected abstract void run(Instance instance, Digest digest) throws Exception;
  }

  static class CatActionResult extends DigestsCommand {
    @Override
    public String name() {
      return "ActionResult";
    }

    @Override
    public String description() {
      return "Get results of Action [digests...] from the ActionCache";
    }

    @Override
    protected void run(Instance instance, Digest digest) throws Exception {
      ActionResult actionResult =
          instance
              .getActionResult(DigestUtil.asActionKey(digest), RequestMetadata.getDefaultInstance())
              .get();
      if (actionResult != null) {
        printActionResult(actionResult, 0);
      } else {
        System.out.println("ActionResult not found for " + DigestUtil.toString(digest));
      }
    }
  }

  static class DirectoryTree extends DigestsCommand {
    @Override
    public String description() {
      return "Simple recursive root Directory [digests...], missing directories will error";
    }

    @Override
    protected void run(Instance instance, Digest digest) throws Exception {
      printDirectoryTree(instance, digest);
    }
  }

  static class TreeLayout extends DigestsCommand {
    @Override
    public String description() {
      return "Rich tree layout of root directory [digests...], with weighting and missing tolerance";
    }

    @Override
    protected void run(Instance instance, Digest digest) throws Exception {
      Tree tree = fetchTree(instance, digest);
      printTreeLayout(new ProxyDirectoriesIndex(tree.getDirectoriesMap()), digest);
    }
  }

  static class File extends DigestsCommand {
    @Override
    public String description() {
      return "Raw content of blob [digests...] file (60s time limit)";
    }

    @Override
    protected void run(Instance instance, Digest digest) throws Exception {
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
  }

  abstract static class BlobCommand extends DigestsCommand {
    @Override
    protected void run(Instance instance, Digest digest) throws Exception {
      run(
          instance,
          getBlob(
              instance, Compressor.Value.IDENTITY, digest, RequestMetadata.getDefaultInstance()));
    }

    protected abstract void run(Instance instance, ByteString blob) throws Exception;
  }

  static class CatAction extends BlobCommand {
    @Override
    public String name() {
      return "Action";
    }

    @Override
    public String description() {
      return "Definition of Action [digests...]";
    }

    @Override
    protected void run(Instance instance, ByteString blob) {
      printAction(blob);
    }
  }

  static class CatQueuedOperation extends BlobCommand {
    @Override
    public String name() {
      return "QueuedOperation";
    }

    @Override
    public String description() {
      return "Definition of prepared operation [digests...] for execution";
    }

    @Override
    protected void run(Instance instance, ByteString blob) {
      printQueuedOperation(blob, instance.getDigestUtil());
    }
  }

  static class DumpQueuedOperation extends BlobCommand {
    @Override
    public String description() {
      return "Binary QueuedOperation [digests...] content, suitable for retention in local 'blobs' directory and use with bf-executor";
    }

    @Override
    protected void run(Instance instance, ByteString blob) throws Exception {
      dumpQueuedOperation(blob, instance.getDigestUtil());
    }
  }

  static class REDirectoryTree extends BlobCommand {
    @Override
    public String description() {
      return "Vanilla REAPI Tree [digests...] description (NOT buildfarm Tree with index)";
    }

    @Override
    protected void run(Instance instance, ByteString blob) throws Exception {
      printREDirectoryTree(
          instance.getDigestUtil(), build.bazel.remote.execution.v2.Tree.parseFrom(blob));
    }
  }

  static class RETreeLayout extends BlobCommand {
    @Override
    public String description() {
      return "Vanilla REAPI Tree [digests...] with rich weighting like TreeLayout";
    }

    @Override
    protected void run(Instance instance, ByteString blob) throws Exception {
      printRETreeLayout(
          instance.getDigestUtil(), build.bazel.remote.execution.v2.Tree.parseFrom(blob));
    }
  }

  static class CatRECommand extends BlobCommand {
    @Override
    public String name() {
      return "Command";
    }

    @Override
    public String description() {
      return "Definition of Command [digests...]";
    }

    @Override
    protected void run(Instance instance, ByteString blob) {
      printCommand(blob);
    }
  }

  static class CatDirectory extends BlobCommand {
    @Override
    public String name() {
      return "Directory";
    }

    @Override
    public String description() {
      return "Definition of Directory [digests...]";
    }

    @Override
    protected void run(Instance instance, ByteString blob) {
      printDirectory(blob);
    }
  }

  static final CatCommand[] commands = {
    new WorkerProfile(),
    new Capabilities(),
    new Operations(),
    new BackplaneStatus(),
    new Missing(),
    new CatOperation(),
    new Watch(),
    new CatActionResult(),
    new DirectoryTree(),
    new TreeLayout(),
    new File(),
    new CatAction(),
    new CatQueuedOperation(),
    new DumpQueuedOperation(),
    new REDirectoryTree(),
    new RETreeLayout(),
    new CatRECommand(),
    new CatDirectory(),
  };

  static void usage() {
    System.err.println("Usage: bf-cat <host:port> <instance-name> <digest-type> <type>");
    System.err.println("\nRequest, retrieve, and display information related to REAPI services:");
    for (CatCommand command : commands) {
      System.err.printf("\t%s\t%s%n", command.name(), command.description());
    }
  }

  private static void cancellableMain(
      String host, String instanceName, DigestUtil digestUtil, String type, Iterable<String> args)
      throws Exception {
    ManagedChannel channel = createChannel(host);
    Instance instance =
        new StubInstance(instanceName, "bf-cat", digestUtil, channel, Durations.fromSeconds(10));
    // should do something to match caps against requested digests
    try {
      for (CatCommand command : commands) {
        if (command.name().equals(type)) {
          command.run(instance, args);
          return;
        }
      }
      System.err.printf("Unrecognized bf-cat type %s%n", type);
      usage();
    } finally {
      instance.stop();
    }
  }
}
