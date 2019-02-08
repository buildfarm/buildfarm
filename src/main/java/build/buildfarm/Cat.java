package build.buildfarm;

import static build.buildfarm.instance.Utils.getBlob;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Command.EnvironmentVariable;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.OutputFile;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.Retrier;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.v1test.CompletedOperationMetadata;
import build.buildfarm.v1test.ExecutingOperationMetadata;
import build.buildfarm.v1test.QueuedOperation;
import build.buildfarm.v1test.QueuedOperationMetadata;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Durations;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
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
    // FIXME timeout
    System.out.println("DoNotCache: " + (action.getDoNotCache() ? "true" : "false"));
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
    if (command.getEnvironmentVariablesList().isEmpty()) {
      System.out.println("Environment Variables:");
      for (EnvironmentVariable env : command.getEnvironmentVariablesList()) {
        System.out.println("  " + env.getName() + "='" + env.getValue());
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
  }

  private static void printFindMissing(Instance instance, Digest digest) throws ExecutionException, InterruptedException {
    Iterable<Digest> missingDigests = instance.findMissingBlobs(ImmutableList.of(digest), newDirectExecutorService()).get();

    for (Digest missingDigest : missingDigests) {
      System.out.println("Missing: " + DigestUtil.toString(missingDigest));
    }
  }

  private static List<Directory> fetchTree(Instance instance, Digest rootDigest)
      throws IOException, InterruptedException {
    ImmutableList.Builder<Directory> directories = new ImmutableList.Builder<>();
    String pageToken = "";

    do {
      pageToken = instance.getTree(rootDigest, 1024, pageToken, directories);
    } while (!pageToken.isEmpty());

    return directories.build();
  }

  private static Map<Digest, Directory> createDirectoriesIndex(
      Iterable<Directory> directories,
      DigestUtil digestUtil) {
    Set<Digest> directoryDigests = Sets.newHashSet();
    ImmutableMap.Builder<Digest, Directory> directoriesIndex = ImmutableMap.builder();
    for (Directory directory : directories) {
      // double compute here...
      Digest directoryDigest = digestUtil.compute(directory);
      if (!directoryDigests.add(directoryDigest)) {
        continue;
      }
      directoriesIndex.put(directoryDigest, directory);
    }

    return directoriesIndex.build();
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
      String displayName = name;
      if (fileNode.getIsExecutable()) {
        displayName += "*";
      }
      indentOut(indentLevel, displayName);
    }
  }

  private static void printTreeLayout(Instance instance, DigestUtil digestUtil, Digest rootDigest) throws IOException, InterruptedException {
    Map<Digest, Directory> directoriesIndex = createDirectoriesIndex(
        fetchTree(instance, rootDigest), digestUtil);

    Map<Digest, Long> directoryWeights = Maps.newHashMap();
    long totalWeight = computeDirectoryWeights(rootDigest, directoriesIndex, directoryWeights);

    printTreeAt(0, directoriesIndex.get(rootDigest), directoriesIndex, totalWeight, directoryWeights);
  }

  private static void printTree(Instance instance, Digest rootDigest) throws IOException, InterruptedException {
    for (Directory directory : fetchTree(instance, rootDigest)) {
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
        System.out.println("  Completed At: " + new Date(completedMetadata.getCompletedAt()));
        System.out.println("  Executed On: " + completedMetadata.getExecutedOn());
        System.out.println(String.format("  Matched In: %gms", Durations.toNanos(completedMetadata.getMatchedIn()) / 1000000.0));
        System.out.println(String.format("  Fetched In: %gms", Durations.toNanos(completedMetadata.getFetchedIn()) / 1000000.0));
        System.out.println(String.format("  Executed In: %gms", Durations.toNanos(completedMetadata.getExecutedIn()) / 1000000.0));
        System.out.println(String.format("  Reported In: %gms", Durations.toNanos(completedMetadata.getReportedIn()) / 1000000.0));
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
          System.out.println("  Response (ActionResult):");
          try {
            ExecuteResponse response = operation.getResponse().unpack(ExecuteResponse.class);
            printActionResult(response.getResult(), 2);
            System.out.println("    CachedResult: " + (response.getCachedResult() ? "true" : "false"));
          } catch (InvalidProtocolBufferException e) {
            System.out.println("  UNKNOWN RESPONSE TYPE: " + operation.getResponse());
          }
          break;
        case ERROR:
          System.out.println("  Error: " + Code.forNumber(operation.getError().getCode()));
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
    Instance instance = new StubInstance(
        instanceName,
        digestUtil,
        channel,
        10, TimeUnit.SECONDS,
        new ByteStreamUploader("", channel, null, 300, Retrier.NO_RETRIES, null));
    String type = args[3];
    if (type.equals("Operations") && args.length == 4) {
      System.out.println("Listing Operations");
      listOperations(instance);
    }
    for (int i = 4; i < args.length; i+=2) {
      if (type.equals("Operation")) {
        printOperation(instance.getOperation(args[i]));
      } else if (type.equals("Watch")) {
        watchOperation(instance, args[i]);
      } else {
        Digest blobDigest = DigestUtil.parseDigest(args[i]);
        if (type.equals("Missing")) {
          printFindMissing(instance, blobDigest);
        } else if (type.equals("ActionResult")) {
          printActionResult(instance.getActionResult(DigestUtil.asActionKey(blobDigest)), 0);
        } else if (type.equals("Tree")) {
          printTree(instance, blobDigest);
        } else if (type.equals("TreeLayout")) {
          printTreeLayout(instance, digestUtil, blobDigest);
        } else {
          ByteString blob = getBlob(instance, blobDigest, Long.parseLong(args[i+1]));
          if (type.equals("Action")) {
            printAction(blob);
          } else if (type.equals("Command")) {
            printCommand(blob);
          } else if (type.equals("Directory")) {
            printDirectory(blob);
          } else if (type.equals("File")) {
            blob.writeTo(System.out);
          } else {
            System.err.println("Unknown type: " + type);
          }
        }
      }
    }
  }
};
