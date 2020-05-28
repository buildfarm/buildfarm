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

package build.buildfarm;

import static build.buildfarm.instance.Utils.getBlob;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Command.EnvironmentVariable;
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
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.v1test.CompletedOperationMetadata;
import build.buildfarm.v1test.ExecutingOperationMetadata;
import build.buildfarm.v1test.OperationTimesBetweenStages;
import build.buildfarm.v1test.QueuedOperation;
import build.buildfarm.v1test.QueuedOperationMetadata;
import build.buildfarm.v1test.Tree;
import build.buildfarm.v1test.WorkerProfileMessage;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.Code;
import com.google.rpc.PreconditionFailure;
import com.google.rpc.RetryInfo;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
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

class Cat {
  private static ManagedChannel createChannel(String target) {
    NettyChannelBuilder builder =
        NettyChannelBuilder.forTarget(target).negotiationType(NegotiationType.PLAINTEXT);
    return builder.build();
  }

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
  }

  private static void indentOut(int level, String msg) {
    System.out.println(Strings.repeat("  ", level) + msg);
  }

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
        instance
            .findMissingBlobs(digests, directExecutor(), RequestMetadata.getDefaultInstance())
            .get();
    long elapsedMicros = stopwatch.elapsed(TimeUnit.MICROSECONDS);

    boolean missing = false;
    for (Digest missingDigest : missingDigests) {
      System.out.println(
          format(
              "Missing: %s Took %gms",
              DigestUtil.toString(missingDigest), elapsedMicros / 1000.0f));
      missing = true;
    }
    if (!missing) {
      System.out.println(format("Took %gms", elapsedMicros / 1000.0f));
    }
  }

  private static Tree fetchTree(Instance instance, Digest rootDigest)
      throws IOException, InterruptedException {
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
          String.format(
              "%s/ %d (%d%%)", dirNode.getName(), weight, (int) (weight * 100.0 / totalWeight));
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
          String.format(
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
    printTreeLayout(DigestUtil.proxyDirectoriesIndex(tree.getDirectories()), tree.getRootDigest());
  }

  private static void printTreeLayout(Map<Digest, Directory> directoriesIndex, Digest rootDigest)
      throws IOException, InterruptedException {
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
      DigestUtil digestUtil, build.bazel.remote.execution.v2.Tree reTree)
      throws IOException, InterruptedException {
    Tree tree = reTreeToTree(digestUtil, reTree);
    printTree(0, tree, tree.getRootDigest(), digestUtil);
  }

  private static void printDirectoryTree(Instance instance, Digest rootDigest)
      throws IOException, InterruptedException {
    printTree(0, fetchTree(instance, rootDigest), rootDigest, instance.getDigestUtil());
  }

  private static void printTree(int level, Tree tree, Digest rootDigest, DigestUtil digestUtil) {
    indentOut(level, "Directory (Root): " + rootDigest);
    for (Map.Entry<String, Directory> entry : tree.getDirectories().entrySet()) {
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
    printTree(2, queuedOperation.getTree(), queuedOperation.getTree().getRootDigest(), digestUtil);
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
    messages.addAll(queuedOperation.getTree().getDirectories().values());
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
    int limit = 10, count = 0;
    do {
      ImmutableList.Builder<Operation> operations = new ImmutableList.Builder<>();
      pageToken = instance.listOperations(1024, pageToken, "", operations);
      System.out.println(pageToken);
      System.out.println("Page size: " + operations.build().size());
      /*
      for (Operation operation : operations.build()) {
        printOperation(operation);
      }
      */
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
    String strNumFormat = "%-50s : %d";
    System.out.println(
        String.format(strNumFormat, "Current Entry Count", response.getCasEntryCount()));
    System.out.println(
        String.format(
            strNumFormat, "Current DirectoryEntry Count", response.getCasDirectoryEntryCount()));
    System.out.println(
        String.format(
            strNumFormat,
            "Current ContainedDirectories total",
            response.getEntryContainingDirectoriesCount()));
    System.out.println(
        String.format(
            strNumFormat,
            "Current ContainedDirectories Max of single Entry",
            response.getEntryContainingDirectoriesMax()));
    System.out.println(
        String.format(
            strNumFormat, "Number of Evicted Entries", response.getCasEvictedEntryCount()));
    System.out.println(
        String.format(
            strNumFormat, "Total size of Evicted Entries", response.getCasEvictedEntrySize()));

    String strStrFormat = "%-50s : %s";
    System.out.println(
        String.format(
            strStrFormat,
            "Slots usage/configured in InputFetchStage",
            response.getInputFetchStageSlotsUsedOverConfigured()));
    System.out.println(
        String.format(
            strStrFormat,
            "Slots usage/configured in ExecuteActionStage",
            response.getExecuteActionStageSlotsUsedOverConfigured()));

    List<OperationTimesBetweenStages> times = response.getTimesList();
    for (OperationTimesBetweenStages time : times) {
      printOperationTime(time);
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
        System.out.println("The period is UNKNOWN: " + time.getPeriod());
        periodInfo = periodInfo + time.getPeriod() + " seconds";
        break;
    }

    periodInfo += ":";
    System.out.println(periodInfo);
    System.out.println("Number of operations completed: " + time.getNumberOfOperation());
    String strStrNumFormat = "%-28s -> %-28s : %12.2f ms";
    System.out.println(
        String.format(
            strStrNumFormat, "Queued", "MatchStage", durationToMillis(time.getQueuedToMatch())));
    System.out.println(
        String.format(
            strStrNumFormat,
            "MatchStage",
            "InputFetchStage start",
            durationToMillis(time.getMatchToInputFetchStart())));
    System.out.println(
        String.format(
            strStrNumFormat,
            "InputFetchStage Start",
            "InputFetchStage Completed",
            durationToMillis(time.getInputFetchStartToComplete())));
    System.out.println(
        String.format(
            strStrNumFormat,
            "InputFetchStage Completed",
            "ExecutionStage Start",
            durationToMillis(time.getInputFetchCompleteToExecutionStart())));
    System.out.println(
        String.format(
            strStrNumFormat,
            "ExecutionStage Start",
            "ExecutionStage Completed",
            durationToMillis(time.getExecutionStartToComplete())));
    System.out.println(
        String.format(
            strStrNumFormat,
            "ExecutionStage Completed",
            "ReportResultStage Started",
            durationToMillis(time.getExecutionCompleteToOutputUploadStart())));
    System.out.println(
        String.format(
            strStrNumFormat,
            "OutputUploadStage Started",
            "OutputUploadStage Completed",
            durationToMillis(time.getOutputUploadStartToComplete())));
    System.out.println();
  }

  private static float durationToMillis(Duration d) {
    return d.getSeconds() * 1000.0f + d.getSeconds() / (1000.0f * 1000.0f);
  }

  public static void main(String[] args) throws Exception {
    String host = args[0];
    String instanceName = args[1];
    DigestUtil digestUtil = DigestUtil.forHash(args[2]);
    String type = args[3];

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

  static void cancellableMain(
      String host, String instanceName, DigestUtil digestUtil, String type, String[] args)
      throws Exception {
    ManagedChannel channel = createChannel(host);
    Instance instance =
        new StubInstance(instanceName, "bf-cat", digestUtil, channel, 10, TimeUnit.SECONDS);
    try {
      instanceMain(instance, type, args);
    } finally {
      instance.stop();
    }
  }

  static void instanceMain(Instance instance, String type, String[] args) throws Exception {
    if (type.equals("WorkerProfile")) {
      getWorkerProfile(instance);
    }
    if (type.equals("Capabilities")) {
      ServerCapabilities capabilities = instance.getCapabilities();
      printCapabilities(capabilities);
    }
    if (type.equals("Operations") && args.length == 4) {
      System.out.println("Listing Operations");
      listOperations(instance);
    }
    if (type.equals("OperationsStatus") && args.length == 4) {
      System.out.println(instance.operationsStatus());
    }
    // should do something to match caps against requested digests
    if (type.equals("Missing")) {
      ImmutableList.Builder<Digest> digests = ImmutableList.builder();
      for (int i = 4; i < args.length; i++) {
        digests.add(DigestUtil.parseDigest(args[i]));
      }
      printFindMissing(instance, digests.build());
    } else {
      for (int i = 4; i < args.length; i++) {
        if (type.equals("Operation")) {
          printOperation(instance.getOperation(args[i]));
        } else if (type.equals("Watch")) {
          watchOperation(instance, args[i]);
        } else {
          Digest blobDigest = DigestUtil.parseDigest(args[i]);
          if (type.equals("ActionResult")) {
            printActionResult(
                instance
                    .getActionResult(
                        DigestUtil.asActionKey(blobDigest), RequestMetadata.getDefaultInstance())
                    .get(),
                0);
          } else if (type.equals("DirectoryTree")) {
            printDirectoryTree(instance, blobDigest);
          } else if (type.equals("TreeLayout")) {
            Tree tree = fetchTree(instance, blobDigest);
            printTreeLayout(DigestUtil.proxyDirectoriesIndex(tree.getDirectories()), blobDigest);
          } else {
            if (type.equals("File")) {
              try (InputStream in =
                  instance.newBlobInput(
                      blobDigest, 0, 60, TimeUnit.SECONDS, RequestMetadata.getDefaultInstance())) {
                ByteStreams.copy(in, System.out);
              }
            } else {
              ByteString blob = getBlob(instance, blobDigest, RequestMetadata.getDefaultInstance());
              if (type.equals("Action")) {
                printAction(blob);
              } else if (type.equals("QueuedOperation")) {
                printQueuedOperation(blob, instance.getDigestUtil());
              } else if (type.equals("DumpQueuedOperation")) {
                dumpQueuedOperation(blob, instance.getDigestUtil());
              } else if (type.equals("REDirectoryTree")) {
                printREDirectoryTree(
                    instance.getDigestUtil(), build.bazel.remote.execution.v2.Tree.parseFrom(blob));
              } else if (type.equals("RETreeLayout")) {
                printRETreeLayout(
                    instance.getDigestUtil(), build.bazel.remote.execution.v2.Tree.parseFrom(blob));
              } else if (type.equals("Command")) {
                printCommand(blob);
              } else if (type.equals("Directory")) {
                printDirectory(blob);
              } else {
                System.err.println("Unknown type: " + type);
              }
            }
          }
        }
      }
    }
  }
}
