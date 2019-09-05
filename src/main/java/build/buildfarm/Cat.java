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
import build.bazel.remote.execution.v2.Tree;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.v1test.CompletedOperationMetadata;
import build.buildfarm.v1test.ExecutingOperationMetadata;
import build.buildfarm.v1test.QueuedOperation;
import build.buildfarm.v1test.QueuedOperationMetadata;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.Code;
import com.google.rpc.RetryInfo;
import com.google.rpc.PreconditionFailure;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

class Cat {
  private static ManagedChannel createChannel(String target) {
    NettyChannelBuilder builder =
        NettyChannelBuilder.forTarget(target)
            .negotiationType(NegotiationType.PLAINTEXT);
    return builder.build();
  }

  private static void printAction(ByteString actionBlob) {
    Action action;
    try {
      action = Action.parseFrom(actionBlob);
    } catch (InvalidProtocolBufferException e) {
      System.out.println("Not an action");
      return;
    }

    System.out.println("Command Digest: Command " + DigestUtil.toString(action.getCommandDigest()));
    System.out.println("Input Root Digest: Directory " + DigestUtil.toString(action.getInputRootDigest()));
    System.out.println("DoNotCache: " + (action.getDoNotCache() ? "true" : "false"));
    if (action.hasTimeout()) {
      System.out.println("Timeout: " + (action.getTimeout().getSeconds() + action.getTimeout().getNanos() / 1e9) + "s");
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

    for (String outputFile : command.getOutputFilesList()) {
      System.out.println("OutputFile: " + outputFile);
    }
    for (String outputDirectory : command.getOutputDirectoriesList()) {
      System.out.println("OutputDirectory: " + outputDirectory);
    }
    // FIXME platform
    System.out.println("Arguments: ('" + String.join("', '", command.getArgumentsList()) + "')");
    if (!command.getEnvironmentVariablesList().isEmpty()) {
      System.out.println("Environment Variables:");
      for (EnvironmentVariable env : command.getEnvironmentVariablesList()) {
        System.out.println("  " + env.getName() + "='" + env.getValue() + "'");
      }
    }
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
      indentOut(indentLevel, "Output File: " + outputFile.getPath() + attrs + " File " + DigestUtil.toString(outputFile.getDigest()));
    }
    for (OutputDirectory outputDirectory : result.getOutputDirectoriesList()) {
      indentOut(indentLevel, "Output Directory: " + outputDirectory.getPath() + " Directory " + DigestUtil.toString(outputDirectory.getTreeDigest()));
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
      indentOut(indentLevel + 1, "Worker: "                  + executedActionMetadata.getWorker());
      indentOut(indentLevel + 1, "Queued At: "               + Timestamps.toString(executedActionMetadata.getQueuedTimestamp()));
      indentOut(indentLevel + 1, "Worker Start: "            + Timestamps.toString(executedActionMetadata.getWorkerStartTimestamp()));
      indentOut(indentLevel + 1, "Input Fetch Start: "       + Timestamps.toString(executedActionMetadata.getInputFetchStartTimestamp()));
      indentOut(indentLevel + 1, "Input Fetch Completed: "   + Timestamps.toString(executedActionMetadata.getInputFetchCompletedTimestamp()));
      indentOut(indentLevel + 1, "Execution Start: "         + Timestamps.toString(executedActionMetadata.getExecutionStartTimestamp()));
      indentOut(indentLevel + 1, "Execution Completed: "     + Timestamps.toString(executedActionMetadata.getExecutionCompletedTimestamp()));
      indentOut(indentLevel + 1, "Output Upload Start: "     + Timestamps.toString(executedActionMetadata.getOutputUploadStartTimestamp()));
      indentOut(indentLevel + 1, "Output Upload Completed: " + Timestamps.toString(executedActionMetadata.getOutputUploadCompletedTimestamp()));
      indentOut(indentLevel + 1, "Worker Completed: "        + Timestamps.toString(executedActionMetadata.getWorkerCompletedTimestamp()));
    }
  }

  private static void printFindMissing(Instance instance, Iterable<Digest> digests) throws ExecutionException, InterruptedException {
    Stopwatch stopwatch = Stopwatch.createStarted();
    Iterable<Digest> missingDigests = instance.findMissingBlobs(digests, directExecutor(), RequestMetadata.getDefaultInstance()).get();
    long elapsedMicros = stopwatch.elapsed(TimeUnit.MICROSECONDS);

    boolean missing = false;
    for (Digest missingDigest : missingDigests) {
      System.out.println(format("Missing: %s Took %gms", DigestUtil.toString(missingDigest), elapsedMicros / 1000.0f));
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
          weight += computeDirectoryWeights(dirNode.getDigest(), directoriesIndex, directoryWeights);
        }
      }
      // filenodes are already computed
    }
    directoryWeights.put(directoryDigest, weight);
    return weight;
  }

  private static void printTreeAt(int indentLevel, Directory directory, Map<Digest, Directory> directoriesIndex, long totalWeight, Map<Digest, Long> directoryWeights) {
    for (DirectoryNode dirNode : directory.getDirectoriesList()) {
      Directory subDirectory = directoriesIndex.get(dirNode.getDigest());
      long weight = directoryWeights.get(dirNode.getDigest());
      String displayName = String.format(
          "%s/ %d (%d%%)",
          dirNode.getName(),
          weight,
          (int) (weight * 100.0 / totalWeight));
      indentOut(indentLevel, displayName);
      if (subDirectory == null) {
        indentOut(indentLevel+1, "DIRECTORY MISSING FROM CAS");
      } else {
        printTreeAt(indentLevel+1, directoriesIndex.get(dirNode.getDigest()), directoriesIndex, totalWeight, directoryWeights);
      }
    }
    for (FileNode fileNode : directory.getFilesList()) {
      String name = fileNode.getName();
      String displayName = String.format("%s%s %s",
          name,
          fileNode.getIsExecutable() ? "*" : "",
          DigestUtil.toString(fileNode.getDigest()));
      indentOut(indentLevel, displayName);
    }
  }

  private static void printTreeLayout(Map<Digest, Directory> directoriesIndex, Digest rootDigest) throws IOException, InterruptedException {
    Map<Digest, Long> directoryWeights = Maps.newHashMap();
    long totalWeight = computeDirectoryWeights(rootDigest, directoriesIndex, directoryWeights);

    printTreeAt(0, directoriesIndex.get(rootDigest), directoriesIndex, totalWeight, directoryWeights);
  }

  private static void printDirectoryTree(Instance instance, Digest rootDigest) throws IOException, InterruptedException {
    Tree tree = fetchTree(instance, rootDigest);
    System.out.println("Directory (Root): " + rootDigest);
    printDirectory(1, tree.getRoot());
    for (Directory directory : tree.getChildrenList()) {
      System.out.println("Directory: " + DigestUtil.toString(instance.getDigestUtil().compute(directory)));
      printDirectory(1, directory);
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
      indentOut(indentLevel, "File: " + displayName + " File " + DigestUtil.toString(fileNode.getDigest()));
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
      indentOut(indentLevel, "Dir: " + directoryNode.getName() + " Directory " + DigestUtil.toString(directoryNode.getDigest()));
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
    } while(!pageToken.equals(""));
  }

  private static void printRequestMetadata(RequestMetadata metadata) {
    System.out.println("ToolDetails:");
    System.out.println("  ToolName: " + metadata.getToolDetails().getToolName());
    System.out.println("  ToolVersion: " + metadata.getToolDetails().getToolVersion());
    System.out.println("ActionId: " + metadata.getActionId());
    System.out.println("ToolInvocationId: " + metadata.getToolInvocationId());
    System.out.println("CorrelatedInvocationsId: " + metadata.getCorrelatedInvocationsId());
  }

  private static void printStatus(com.google.rpc.Status status) throws InvalidProtocolBufferException {
    System.out.println("  Code: " + Code.forNumber(status.getCode()));
    if (!status.getMessage().isEmpty()) {
      System.out.println("  Message: " + status.getMessage());
    }
    if (status.getDetailsCount() > 0) {
      System.out.println("  Details:");
      for (Any detail : status.getDetailsList()) {
        if (detail.is(RetryInfo.class)) {
          RetryInfo retryInfo = detail.unpack(RetryInfo.class);
          System.out.println("    RetryDelay: " + (retryInfo.getRetryDelay().getSeconds() + retryInfo.getRetryDelay().getNanos() / 1000000000.0f));
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

  private static void printExecuteResponse(ExecuteResponse response) throws InvalidProtocolBufferException {
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
        ExecutingOperationMetadata executingMetadata = operation.getMetadata().unpack(ExecutingOperationMetadata.class);
        System.out.println("  Started At: " + new Date(executingMetadata.getStartedAt()));
        System.out.println("  Executing On: " + executingMetadata.getExecutingOn());
        metadata = executingMetadata.getExecuteOperationMetadata();
        requestMetadata = executingMetadata.getRequestMetadata();
      } else if (operation.getMetadata().is(CompletedOperationMetadata.class)) {
        CompletedOperationMetadata completedMetadata = operation.getMetadata().unpack(CompletedOperationMetadata.class);
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
      switch(operation.getResultCase()) {
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

  private static void watchOperation(Instance instance, String operationName) throws InterruptedException {
    try {
      instance.watchOperation(operationName, (operation) -> {
        if (operation == null) {
          throw Status.NOT_FOUND.asRuntimeException();
        }
        printOperation(operation);
      }).get();
    } catch (ExecutionException e) {
      e.getCause().printStackTrace();
    }
  }

  public static void main(String[] args) throws Exception {
    String host = args[0];
    String instanceName = args[1];
    DigestUtil digestUtil = DigestUtil.forHash(args[2]);
    ManagedChannel channel = createChannel(host);
    ScheduledExecutorService service = newSingleThreadScheduledExecutor();
    Context.CancellableContext ctx = Context.current().withDeadlineAfter(10, TimeUnit.SECONDS, service);
    Context prevContext = ctx.attach();
    Instance instance = new StubInstance(instanceName, "bf-cat", digestUtil, channel, 10, TimeUnit.SECONDS);
    String type = args[3];

    ServerCapabilities capabilities = instance.getCapabilities();
    if (type.equals("Operations") && args.length == 4) {
      System.out.println("Listing Operations");
      listOperations(instance);
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
            printActionResult(instance.getActionResult(DigestUtil.asActionKey(blobDigest)), 0);
          } else if (type.equals("DirectoryTree")) {
            printDirectoryTree(instance, blobDigest);
          } else if (type.equals("TreeLayout")) {
            printTreeLayout(digestUtil.createDirectoriesIndex(fetchTree(instance, blobDigest)), blobDigest);
          } else {
            if (type.equals("File")) {
              try (InputStream in = instance.newBlobInput(blobDigest, 0, 60, TimeUnit.SECONDS, RequestMetadata.getDefaultInstance())) {
                ByteStreams.copy(in, System.out);
              }
            } else {
              ByteString blob = getBlob(instance, blobDigest, RequestMetadata.getDefaultInstance());
              if (type.equals("Action")) {
                printAction(blob);
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
    ctx.cancel(null);
    ctx.detach(prevContext);
    if (!shutdownAndAwaitTermination(service, 1, TimeUnit.SECONDS)) {
      throw new RuntimeException("could not shut down service");
    }
  }
};
