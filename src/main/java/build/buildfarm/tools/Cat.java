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
import java.io.PrintStream;
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
import picocli.CommandLine.Option;
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
  private IndentStream out() {
    return new IndentStream(0, System.out);
  }

  private static class IndentStream {
    private final int level;
    private final PrintStream out;

    IndentStream(int level, PrintStream out) {
      this.level = level;
      this.out = out;
    }

    IndentStream push() {
      return new IndentStream(level + 1, out);
    }

    void println(String s) {
      out.println(Strings.repeat("  ", level) + s);
    }

    void format(String format, Object... args) {
      out.format(Strings.repeat("  ", level) + format, args);
      out.println();
    }
  }

  @Parameters(index = "0", description = CliConstants.BUILDFARM_HOST)
  String host;

  @Parameters(index = "1", description = CliConstants.INSTANCE_NAME)
  String instanceName;

  @Option(
      names = {"-t", "--timeout"},
      description = "Limit overall request context to timeout (seconds), default ${DEFAULT-VALUE}",
      defaultValue = "10")
  private int timeoutSeconds;

  private static void printCapabilities(ServerCapabilities capabilities) {
    System.out.println(capabilities);
  }

  private static void printAction(
      ByteString actionBlob, DigestFunction.Value digestFunction, IndentStream out) {
    Action action;
    try {
      action = Action.parseFrom(actionBlob);
    } catch (InvalidProtocolBufferException e) {
      System.out.println("Not an action");
      return;
    }
    printAction(out, action, digestFunction);
  }

  private static void printAction(
      IndentStream out, Action action, DigestFunction.Value digestFunction) {
    out.format(
        "Command Digest: Command %s",
        DigestUtil.toString(DigestUtil.fromDigest(action.getCommandDigest(), digestFunction)));
    out.format(
        "Input Root Digest: Directory %s",
        DigestUtil.toString(DigestUtil.fromDigest(action.getInputRootDigest(), digestFunction)));
    out.format("DoNotCache: %b", action.getDoNotCache());
    if (action.hasTimeout()) {
      out.format(
          "Timeout: %gs",
          (action.getTimeout().getSeconds() + action.getTimeout().getNanos() / 1e9));
    }
    out.format("Salt: %s", action.getSalt());
    out.format("Platform: %s", action.getPlatform());
  }

  private static void printCommand(ByteString commandBlob, IndentStream out) {
    Command command;
    try {
      command = Command.parseFrom(commandBlob);
    } catch (InvalidProtocolBufferException e) {
      System.out.println("Not a command");
      return;
    }
    printCommand(out, command);
  }

  private static void printCommand(IndentStream out, Command command) {
    for (String outputFile : command.getOutputFilesList()) {
      out.format("OutputFile: %s", outputFile);
    }
    for (String outputDirectory : command.getOutputDirectoriesList()) {
      out.format("OutputDirectory: %s", outputDirectory);
    }
    out.format("Arguments: ('%s')", String.join("', '", command.getArgumentsList()));
    if (!command.getEnvironmentVariablesList().isEmpty()) {
      out.println("Environment Variables:");
      for (Command.EnvironmentVariable env : command.getEnvironmentVariablesList()) {
        out.push().format("%s='%s'", env.getName(), env.getValue());
      }
    }
    if (!command.getPlatform().getPropertiesList().isEmpty()) {
      out.println("Platform:");
      for (build.bazel.remote.execution.v2.Platform.Property property :
          command.getPlatform().getPropertiesList()) {
        out.push().format("%s='%s'", property.getName(), property.getValue());
      }
    } else {
      out.println("Platform: (none)");
    }
    out.format("WorkingDirectory: %s", command.getWorkingDirectory());
  }

  @SuppressWarnings("ConstantConditions")
  private static void printActionResult(
      ActionResult result, DigestFunction.Value digestFunction, IndentStream out) {
    for (OutputFile outputFile : result.getOutputFilesList()) {
      String attrs = "";
      if (outputFile.getIsExecutable()) {
        attrs += (attrs.length() == 0 ? "" : ",") + "executable";
      }
      if (attrs.length() != 0) {
        attrs = " (" + attrs + ")";
      }
      out.format(
          "Output File: %s%s File %s",
          outputFile.getPath(),
          attrs,
          DigestUtil.toString(DigestUtil.fromDigest(outputFile.getDigest(), digestFunction)));
    }
    for (OutputDirectory outputDirectory : result.getOutputDirectoriesList()) {
      out.format(
          "Output Directory: %s Directory %s",
          outputDirectory.getPath(),
          DigestUtil.toString(
              DigestUtil.fromDigest(outputDirectory.getTreeDigest(), digestFunction)));
    }
    out.format("Exit Code: %d", result.getExitCode());
    if (!result.getStdoutRaw().isEmpty()) {
      out.format("Stdout: %s", result.getStdoutRaw().toStringUtf8());
    }
    if (result.hasStdoutDigest()) {
      out.format(
          "Stdout Digest: %s",
          DigestUtil.toString(DigestUtil.fromDigest(result.getStdoutDigest(), digestFunction)));
    }
    if (!result.getStderrRaw().isEmpty()) {
      out.format("Stderr: %s", result.getStderrRaw().toStringUtf8());
    }
    if (result.hasStderrDigest()) {
      out.format(
          "Stderr Digest: %s",
          DigestUtil.toString(DigestUtil.fromDigest(result.getStderrDigest(), digestFunction)));
    }
    if (result.hasExecutionMetadata()) {
      out.println("ExecutionMetadata:");
      printExecutedActionMetadata(result.getExecutionMetadata(), out.push());
    }
  }

  private static void printWorkerMetadata(WorkerExecutedMetadata metadata, IndentStream out) {
    out.format("Fetched Bytes: %d", metadata.getFetchedBytes());
    if (metadata.getLinkedInputDirectoriesCount() > 0) {
      out.format("Linked Input Directories:");
      for (String linkedInputDirectory : metadata.getLinkedInputDirectoriesList()) {
        out.push().println(linkedInputDirectory);
      }
    }
  }

  private static void printExecutedActionMetadata(
      ExecutedActionMetadata metadata, IndentStream out) {
    if (!metadata.getWorker().isEmpty()) {
      out.format("Worker: %s", metadata.getWorker());
    }
    // TODO switch to spans/stalls
    if (metadata.hasQueuedTimestamp()) {
      out.format("Queued At: %s", Timestamps.toString(metadata.getQueuedTimestamp()));
    }
    if (metadata.hasWorkerStartTimestamp()) {
      out.format("Worker Start: %s", Timestamps.toString(metadata.getWorkerStartTimestamp()));
    }
    if (metadata.hasInputFetchStartTimestamp()) {
      out.format(
          "Input Fetch Start: %s", Timestamps.toString(metadata.getInputFetchStartTimestamp()));
    }
    if (metadata.hasInputFetchCompletedTimestamp()) {
      out.format(
          "Input Fetch Completed: %s",
          Timestamps.toString(metadata.getInputFetchCompletedTimestamp()));
    }
    if (metadata.hasExecutionStartTimestamp()) {
      out.format("Execution Start: %s", Timestamps.toString(metadata.getExecutionStartTimestamp()));
    }
    if (metadata.hasExecutionCompletedTimestamp()) {
      out.format(
          "Execution Completed: %s",
          Timestamps.toString(metadata.getExecutionCompletedTimestamp()));
    }
    if (metadata.hasOutputUploadStartTimestamp()) {
      out.format(
          "Output Upload Start: %s", Timestamps.toString(metadata.getOutputUploadStartTimestamp()));
    }
    if (metadata.hasOutputUploadCompletedTimestamp()) {
      out.format(
          "Output Upload Completed: %s",
          Timestamps.toString(metadata.getOutputUploadCompletedTimestamp()));
    }
    if (metadata.hasWorkerCompletedTimestamp()) {
      out.format(
          "Worker Completed: " + Timestamps.toString(metadata.getWorkerCompletedTimestamp()));
    }
    if (metadata.getAuxiliaryMetadataCount() > 0) {
      out.println("Auxiliary Metadata:");
      for (Any auxiliary : metadata.getAuxiliaryMetadataList()) {
        if (auxiliary.is(WorkerExecutedMetadata.class)) {
          try {
            printWorkerMetadata(auxiliary.unpack(WorkerExecutedMetadata.class), out.push());
          } catch (InvalidProtocolBufferException e) {
            // unlikely
            e.printStackTrace();
          }
        } else {
          out.push().format("Unrecognized Metadata: %s", auxiliary);
        }
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
      IndentStream out,
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
      out.println(displayName);
      if (subDirectory == null) {
        out.push().println("DIRECTORY MISSING FROM CAS");
      } else {
        printTreeAt(
            out.push(),
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
      out.println(displayName);
    }
  }

  private static void printRETreeLayout(
      DigestUtil digestUtil, build.bazel.remote.execution.v2.Tree reTree, IndentStream out)
      throws IOException, InterruptedException {
    Tree tree = reTreeToTree(digestUtil, reTree);
    printTreeLayout(new ProxyDirectoriesIndex(tree.getDirectoriesMap()), tree.getRootDigest(), out);
  }

  private static void printTreeLayout(
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      Digest rootDigest,
      IndentStream out) {
    Map<build.bazel.remote.execution.v2.Digest, Long> directoryWeights = Maps.newHashMap();
    long totalWeight =
        computeDirectoryWeights(
            DigestUtil.toDigest(rootDigest), directoriesIndex, directoryWeights);

    printTreeAt(
        out,
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
      DigestUtil digestUtil, build.bazel.remote.execution.v2.Tree reTree, IndentStream out) {
    Tree tree = reTreeToTree(digestUtil, reTree);
    printTree(out, tree, tree.getRootDigest());
  }

  private static void printDirectoryTree(Instance instance, Digest rootDigest, IndentStream out)
      throws IOException, InterruptedException {
    printTree(out, fetchTree(instance, rootDigest), rootDigest);
  }

  private static void printTree(IndentStream out, Tree tree, Digest rootDigest) {
    out.format("Directory (Root): %s", DigestUtil.toString(rootDigest));
    for (Map.Entry<String, Directory> entry : tree.getDirectoriesMap().entrySet()) {
      IndentStream dirOut = out.push();
      dirOut.format("Directory: %s", entry.getKey());
      printDirectory(dirOut, entry.getValue(), rootDigest.getDigestFunction());
    }
  }

  private static void printQueuedOperation(
      ByteString blob, DigestUtil digestUtil, IndentStream out) {
    QueuedOperation queuedOperation;
    try {
      queuedOperation = QueuedOperation.parseFrom(blob);
    } catch (InvalidProtocolBufferException e) {
      out.println("Not a QueuedOperation");
      return;
    }
    out.println("QueuedOperation:");

    out = out.push();
    out.println("Action: " + DigestUtil.toString(digestUtil.compute(queuedOperation.getAction())));
    printAction(out.push(), queuedOperation.getAction(), digestUtil.getDigestFunction());
    out.println("Command:");
    printCommand(out.push(), queuedOperation.getCommand());
    out.println("Tree:");
    printTree(out.push(), queuedOperation.getTree(), queuedOperation.getTree().getRootDigest());
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
      ByteString directoryBlob, DigestFunction.Value digestFunction, IndentStream out) {
    Directory directory;
    try {
      directory = Directory.parseFrom(directoryBlob);
    } catch (InvalidProtocolBufferException e) {
      System.out.println("Not a directory");
      return;
    }

    printDirectory(out, directory, digestFunction);
  }

  private static void printDirectory(
      IndentStream out, Directory directory, DigestFunction.Value digestFunction) {
    boolean filesUnsorted = false;
    String last = "";
    for (FileNode fileNode : directory.getFilesList()) {
      String name = fileNode.getName();
      String displayName = name;
      if (fileNode.getIsExecutable()) {
        displayName = "*" + name + "*";
      }
      out.format(
          "File: %s File %s",
          displayName,
          DigestUtil.toString(DigestUtil.fromDigest(fileNode.getDigest(), digestFunction)));
      if (!filesUnsorted && last.compareTo(name) > 0) {
        filesUnsorted = true;
      } else {
        last = name;
      }
    }
    if (filesUnsorted) {
      out.println("ERROR: file list is not ordered");
    }

    boolean directoriesUnsorted = false;
    last = "";
    for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
      out.format(
          "Dir: %s Directory %s",
          directoryNode.getName(),
          DigestUtil.toString(DigestUtil.fromDigest(directoryNode.getDigest(), digestFunction)));
      if (!directoriesUnsorted && last.compareTo(directoryNode.getName()) > 0) {
        directoriesUnsorted = true;
      } else {
        last = directoryNode.getName();
      }
    }

    if (directoriesUnsorted) {
      out.println("ERROR: directory list is not ordered");
    }
  }

  private static void listOperations(Instance instance, Iterable<String> args, IndentStream out)
      throws IOException {
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
        printOperation(operation, out);
      }
    } while (!pageToken.equals(Instance.SENTINEL_PAGE_TOKEN));
  }

  private static void printRequestMetadata(RequestMetadata metadata, IndentStream out) {
    out.println("ToolDetails:");
    out.push().println("  ToolName: " + metadata.getToolDetails().getToolName());
    out.push().println("  ToolVersion: " + metadata.getToolDetails().getToolVersion());
    out.println("ActionId: " + metadata.getActionId());
    out.println("ToolInvocationId: " + metadata.getToolInvocationId());
    out.println("CorrelatedInvocationsId: " + metadata.getCorrelatedInvocationsId());
    out.println("ActionMnemonic: " + metadata.getActionMnemonic());
    out.println("TargetId: " + metadata.getTargetId());
    out.println("ConfigurationId: " + metadata.getConfigurationId());
  }

  private static void printStatus(com.google.rpc.Status status, IndentStream out)
      throws InvalidProtocolBufferException {
    out.println("Code: " + Code.forNumber(status.getCode()));
    if (!status.getMessage().isEmpty()) {
      out.println("Message: " + status.getMessage());
    }
    if (status.getDetailsCount() > 0) {
      out.println("Details:");
      for (Any detail : status.getDetailsList()) {
        IndentStream detailOut = out.push();
        if (detail.is(RetryInfo.class)) {
          RetryInfo retryInfo = detail.unpack(RetryInfo.class);
          detailOut.println(
              "RetryDelay: "
                  + (retryInfo.getRetryDelay().getSeconds()
                      + retryInfo.getRetryDelay().getNanos() / 1000000000.0f));
        } else if (detail.is(PreconditionFailure.class)) {
          PreconditionFailure preconditionFailure = detail.unpack(PreconditionFailure.class);
          detailOut.println("PreconditionFailure:");
          for (PreconditionFailure.Violation violation : preconditionFailure.getViolationsList()) {
            IndentStream violOut = detailOut.push();
            violOut.println("Violation: " + violation.getType());
            violOut = violOut.push();
            violOut.println("Subject: " + violation.getSubject());
            violOut.println("Description: " + violation.getDescription());
          }
        } else {
          detailOut.push().println("Unknown Detail: " + detail.getTypeUrl());
        }
      }
    }
  }

  private static void printExecuteResponse(
      ExecuteResponse response, DigestFunction.Value digestFunction, IndentStream out)
      throws InvalidProtocolBufferException {
    printStatus(response.getStatus(), out);
    if (response.hasResult()) {
      printActionResult(response.getResult(), digestFunction, out.push());
      out.format("CachedResult: %b", response.getCachedResult());
    }
    if (response.getServerLogsCount() > 0) {
      out.println("Server Logs:");
      IndentStream logOut = out.push();
      for (Map.Entry<String, LogFile> entry : response.getServerLogsMap().entrySet()) {
        LogFile logFile = entry.getValue();
        logOut.format(
            "%s: Log %s%s%n",
            entry.getKey(),
            DigestUtil.toString(DigestUtil.fromDigest(logFile.getDigest(), digestFunction)),
            logFile.getHumanReadable() ? " (human-readable)" : "");
      }
    }
    if (!response.getMessage().isEmpty()) {
      out.println("Message: " + response.getMessage());
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

  private static void printOperation(Operation operation, IndentStream out) {
    out.println("Operation: " + operation.getName());
    if (operation.getDone()) {
      out.println("Done");
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
        printExecutedActionMetadata(metadata.getPartialExecutionMetadata(), out.push());
        out.println("Metadata:");
        IndentStream metadataOut = out.push();
        metadataOut.println("Stage: " + metadata.getStage());
        digestFunction = metadata.getDigestFunction();
        metadataOut.println(
            "Action: "
                + DigestUtil.toString(
                    DigestUtil.fromDigest(metadata.getActionDigest(), digestFunction)));
        metadataOut.println("Stdout Stream: " + metadata.getStdoutStreamName());
        metadataOut.println("Stderr Stream: " + metadata.getStderrStreamName());
      }
      if (requestMetadata != null) {
        printRequestMetadata(requestMetadata, out);
      }
    } catch (InvalidProtocolBufferException e) {
      out.push().println("  UNKNOWN TYPE: " + e.getMessage());
    }
    if (operation.getDone()) {
      switch (operation.getResultCase()) {
        case RESPONSE:
          out.println("Response:");
          try {
            printExecuteResponse(
                operation.getResponse().unpack(ExecuteResponse.class), digestFunction, out.push());
          } catch (InvalidProtocolBufferException e) {
            out.push().println("  UNKNOWN RESPONSE TYPE: " + operation.getResponse());
          }
          break;
        case ERROR:
          out.println("Error: " + Code.forNumber(operation.getError().getCode()));
          break;
        default:
          out.push().println("  UNKNOWN RESULT!");
          break;
      }
    }
  }

  private static void watchOperation(Instance instance, String operationName, IndentStream out)
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
                printOperation(operation, out);
              })
          .get();
    } catch (ExecutionException e) {
      e.getCause().printStackTrace();
    }
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
  static int runWithDeadline(int timeoutSeconds, Callable<Integer> action) throws Exception {
    if (timeoutSeconds <= 0) {
      return action.call();
    }

    ScheduledExecutorService service = newSingleThreadScheduledExecutor();
    Context.CancellableContext ctx =
        Context.current().withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS, service);
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
      description = "Worker Status including Execution and CAS statistics")
  static class CatWorkerProfile implements Callable<Integer> {
    @ParentCommand private Cat parent;

    @Parameters(arity = "0..*", description = "Worker names")
    private List<String> names;

    @Override
    public Integer call() throws Exception {
      return runWithDeadline(parent.timeoutSeconds, this::run);
    }

    private int run() throws Exception {
      Instance instance = parent.createInstance();
      try {
        if (names != null && !Iterables.isEmpty(names)) {
          batchWorkerProfiles(instance, names);
        } else {
          printWorkerProfile(instance.getWorkerProfile("").get());
        }
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
      return runWithDeadline(parent.timeoutSeconds, this::run);
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
      return runWithDeadline(parent.timeoutSeconds, this::run);
    }

    private int run() throws Exception {
      Instance instance = parent.createInstance();
      try {
        System.out.println("Listing Operations");
        listOperations(instance, args != null ? args : ImmutableList.of(), parent.out());
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
      return runWithDeadline(parent.timeoutSeconds, this::run);
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
      return runWithDeadline(parent.timeoutSeconds, this::run);
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
      return runWithDeadline(parent.timeoutSeconds, this::run);
    }

    private int run() throws Exception {
      Instance instance = parent.createInstance();
      try {
        for (String operationName : operationNames) {
          printOperation(instance.getOperation(operationName), parent.out());
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
      return runWithDeadline(parent.timeoutSeconds, this::run);
    }

    private int run() throws Exception {
      Instance instance = parent.createInstance();
      try {
        for (String operationName : operationNames) {
          watchOperation(instance, operationName, parent.out());
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
      return runWithDeadline(parent.timeoutSeconds, this::run);
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
            printActionResult(actionResult, digest.getDigestFunction(), parent.out());
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
      return runWithDeadline(parent.timeoutSeconds, this::run);
    }

    private int run() throws Exception {
      Instance instance = parent.createInstance();
      try {
        for (Digest digest :
            digestStrings.stream().map(DigestUtil::parseDigest).collect(Collectors.toList())) {
          printDirectoryTree(instance, digest, parent.out());
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
      return runWithDeadline(parent.timeoutSeconds, this::run);
    }

    private int run() throws Exception {
      Instance instance = parent.createInstance();
      try {
        for (Digest digest :
            digestStrings.stream().map(DigestUtil::parseDigest).collect(Collectors.toList())) {
          Tree tree = fetchTree(instance, digest);
          printTreeLayout(
              new ProxyDirectoriesIndex(tree.getDirectoriesMap()), digest, parent.out());
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
      return runWithDeadline(parent.timeoutSeconds, this::run);
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
      return runWithDeadline(parent.timeoutSeconds, this::run);
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
          printAction(blob, digest.getDigestFunction(), parent.out());
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
      return runWithDeadline(parent.timeoutSeconds, this::run);
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
          printQueuedOperation(
              blob, new DigestUtil(HashFunction.get(digest.getDigestFunction())), parent.out());
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
      return runWithDeadline(parent.timeoutSeconds, this::run);
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
      return runWithDeadline(parent.timeoutSeconds, this::run);
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
              build.bazel.remote.execution.v2.Tree.parseFrom(blob),
              parent.out());
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
      return runWithDeadline(parent.timeoutSeconds, this::run);
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
              build.bazel.remote.execution.v2.Tree.parseFrom(blob),
              parent.out());
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
      return runWithDeadline(parent.timeoutSeconds, this::run);
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
          printCommand(blob, parent.out());
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
      return runWithDeadline(parent.timeoutSeconds, this::run);
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
          printDirectory(blob, digest.getDigestFunction(), parent.out());
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
      return runWithDeadline(parent.timeoutSeconds, this::run);
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
      return runWithDeadline(parent.timeoutSeconds, this::run);
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
