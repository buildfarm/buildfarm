package build.buildfarm;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.Retrier;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.v1test.CompletedOperationMetadata;
import build.buildfarm.v1test.QueuedOperationMetadata;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Command;
import com.google.devtools.remoteexecution.v1test.Command.EnvironmentVariable;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.DirectoryNode;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata;
import com.google.devtools.remoteexecution.v1test.ExecuteResponse;
import com.google.devtools.remoteexecution.v1test.FileNode;
import com.google.devtools.remoteexecution.v1test.OutputFile;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Durations;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.util.Date;
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
    for (String outputFile : action.getOutputFilesList()) {
      System.out.println("OutputFile: " + outputFile);
    }
    for (String outputDirectory : action.getOutputDirectoriesList()) {
      System.out.println("OutputDirectory: " + outputDirectory);
    }
    // FIXME platform
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
      if (outputFile.getContent().size() == outputFile.getDigest().getSizeBytes()) {
        attrs += "inline";
      }
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
    if (!result.hasStdoutDigest()) {
      indentOut(indentLevel, "Stdout Digest: " + DigestUtil.toString(result.getStdoutDigest()));
    }
    if (!result.getStderrRaw().isEmpty()) {
      indentOut(indentLevel, "Stderr: " + result.getStderrRaw().toStringUtf8());
    }
    if (!result.hasStderrDigest()) {
      indentOut(indentLevel, "Stderr Digest: " + DigestUtil.toString(result.getStderrDigest()));
    }
  }

  private static void printTree(Instance instance, Digest rootDigest) throws InterruptedException, IOException {
    ImmutableList.Builder<Directory> directories = new ImmutableList.Builder<>();
    String pageToken = "";

    do {
      pageToken = instance.getTree(rootDigest, 1024, pageToken, directories, false);
    } while (!pageToken.isEmpty());

    for (Directory directory : directories.build()) {
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
      if (fileNode.getIsExecutable()) {
        name = "*" + name + "*";
      }
      indentOut(indentLevel, "File: " + name + " File " + DigestUtil.toString(fileNode.getDigest()));
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

  private static void printOperation(Operation operation) {
    System.out.println("Operation: " + operation.getName());
    System.out.println("Done: " + (operation.getDone() ? "true" : "false"));
    System.out.println("Metadata:");
    try {
      ExecuteOperationMetadata metadata;
      if (operation.getMetadata().is(QueuedOperationMetadata.class)) {
        QueuedOperationMetadata queuedMetadata = operation.getMetadata().unpack(QueuedOperationMetadata.class);
        metadata = queuedMetadata.getExecuteOperationMetadata();
      } else if (operation.getMetadata().is(CompletedOperationMetadata.class)) {
        CompletedOperationMetadata completedMetadata = operation.getMetadata().unpack(CompletedOperationMetadata.class);
        System.out.println("  Completed At: " + new Date(completedMetadata.getCompletedAt()));
        System.out.println("  Executed On: " + completedMetadata.getExecutedOn());
        System.out.println(String.format("  Fetched In: %gms", Durations.toNanos(completedMetadata.getFetchedIn()) / 1000000.0));
        System.out.println(String.format("  Executed In: %gms", Durations.toNanos(completedMetadata.getExecutedIn()) / 1000000.0));
        System.out.println(String.format("  Reported In: %gms", Durations.toNanos(completedMetadata.getReportedIn()) / 1000000.0));
        metadata = completedMetadata.getExecuteOperationMetadata();
      } else {
        metadata = operation.getMetadata().unpack(ExecuteOperationMetadata.class);
      }
      System.out.println("  Stage: " + metadata.getStage());
      System.out.println("  Action: " + DigestUtil.toString(metadata.getActionDigest()));
      System.out.println("  Stdout Stream: " + metadata.getStdoutStreamName());
      System.out.println("  Stderr Stream: " + metadata.getStderrStreamName());
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
        Retrier.NO_RETRIES,
        new ByteStreamUploader("", channel, null, 300, Retrier.NO_RETRIES, null));
    String type = args[3];
    if (type.equals("Operations") && args.length == 4) {
      System.out.println("Listing Operations");
      listOperations(instance);
    }
    for (int i = 4; i < args.length; i++) {
      if (type.equals("Operation")) {
        printOperation(instance.getOperation(args[i]));
      } else {
        Digest blobDigest = DigestUtil.parseDigest(args[i]);
        if (type.equals("ActionResult")) {
          printActionResult(instance.getActionResult(DigestUtil.asActionKey(blobDigest)), 0);
        } else if (type.equals("Tree")) {
          printTree(instance, blobDigest);
        } else {
          ByteString blob = instance.getBlob(blobDigest);
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
