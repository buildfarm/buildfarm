// Copyright 2025 The Buildfarm Authors. All rights reserved.
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
import static build.buildfarm.server.services.OperationsService.LIST_OPERATIONS_MAXIMUM_PAGE_SIZE;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.filterEntries;
import static com.google.common.collect.Sets.difference;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.BatchReadBlobsResponse.Response;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.ExecutionStage;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.v1test.Digest;
import build.buildfarm.v1test.DispatchedOperation;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.QueuedOperationMetadata;
import build.buildfarm.v1test.Tree;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
import com.google.rpc.Code;
import io.grpc.ManagedChannel;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

class Diff {
  // got some FAILED_PRECONDITIONs on the first try
  private static boolean isSuccess(Operation operation) {
    try {
      ExecuteResponse executeResponse = operation.getResponse().unpack(ExecuteResponse.class);
      return executeResponse.getStatus().getCode() == Code.OK.getNumber();
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  private static List<Operation> getOps(Instance instance, String invocationId) throws IOException {
    String pageToken = Instance.SENTINEL_PAGE_TOKEN;
    ArrayList<Operation> operations = new ArrayList<>();
    do {
      pageToken =
          instance.listOperations(
              "executions",
              LIST_OPERATIONS_MAXIMUM_PAGE_SIZE,
              pageToken,
              "toolInvocationId=" + invocationId,
              operation -> {
                if (isSuccess(operation)) {
                  operations.add(operation);
                }
              });
    } while (!pageToken.equals(Instance.SENTINEL_PAGE_TOKEN));
    return operations;
  }

  private static ExecuteOperationMetadata executeEntryMetadata(
      ExecuteEntry executeEntry, ExecutionStage.Value stage) {
    return ExecuteOperationMetadata.newBuilder()
        .setStage(stage)
        .setActionDigest(DigestUtil.toDigest(executeEntry.getActionDigest()))
        .setDigestFunction(executeEntry.getActionDigest().getDigestFunction())
        .build();
  }

  private static RequestMetadata requestMetadata(Any metadata) throws IOException {
    if (metadata.is(QueuedOperationMetadata.class)) {
      QueuedOperationMetadata queuedOperationMetadata =
          metadata.unpack(QueuedOperationMetadata.class);
      return queuedOperationMetadata.getRequestMetadata();
    }
    if (metadata.is(ExecuteEntry.class)) {
      ExecuteEntry executeEntry = metadata.unpack(ExecuteEntry.class);
      return executeEntry.getRequestMetadata();
    }
    if (metadata.is(QueueEntry.class)) {
      QueueEntry queueEntry = metadata.unpack(QueueEntry.class);
      ExecuteEntry executeEntry = queueEntry.getExecuteEntry();
      return executeEntry.getRequestMetadata();
    }
    if (metadata.is(DispatchedOperation.class)) {
      DispatchedOperation dispatchedOperation = metadata.unpack(DispatchedOperation.class);
      ExecuteEntry executeEntry = dispatchedOperation.getQueueEntry().getExecuteEntry();
      return executeEntry.getRequestMetadata();
    }
    return null;
  }

  private static ExecuteOperationMetadata executeOperationMetadata(Any metadata)
      throws IOException {
    if (metadata.is(QueuedOperationMetadata.class)) {
      QueuedOperationMetadata queuedOperationMetadata =
          metadata.unpack(QueuedOperationMetadata.class);
      return queuedOperationMetadata.getExecuteOperationMetadata();
    }
    if (metadata.is(ExecuteEntry.class)) {
      ExecuteEntry executeEntry = metadata.unpack(ExecuteEntry.class);
      return executeEntryMetadata(executeEntry, ExecutionStage.Value.UNKNOWN);
    }
    if (metadata.is(QueueEntry.class)) {
      QueueEntry queueEntry = metadata.unpack(QueueEntry.class);
      ExecuteEntry executeEntry = queueEntry.getExecuteEntry();
      return executeEntryMetadata(executeEntry, ExecutionStage.Value.QUEUED);
    }
    if (metadata.is(DispatchedOperation.class)) {
      DispatchedOperation dispatchedOperation = metadata.unpack(DispatchedOperation.class);
      ExecuteEntry executeEntry = dispatchedOperation.getQueueEntry().getExecuteEntry();
      return executeEntryMetadata(
          executeEntry, ExecutionStage.Value.QUEUED); // latest we can know about here
    }
    return metadata.unpack(ExecuteOperationMetadata.class);
  }

  private static String operationName(Operation operation) throws IOException {
    RequestMetadata metadata = requestMetadata(operation.getMetadata());
    if (metadata != null) {
      return format("%s %s", metadata.getActionMnemonic(), metadata.getTargetId());
    }
    return operation.getName();
  }

  private static Map<Digest, String> getActionDigests(Iterable<Operation> operations)
      throws IOException {
    ImmutableMap.Builder<Digest, String> actionNames = ImmutableMap.builder();
    for (Operation operation : operations) {
      ExecuteOperationMetadata metadata = executeOperationMetadata(operation.getMetadata());
      Digest digest =
          DigestUtil.fromDigest(metadata.getActionDigest(), metadata.getDigestFunction());
      actionNames.put(digest, operationName(operation));
    }
    return actionNames.build();
  }

  // this goddamned function can't return an iterable of ByteStrings because concat finds
  // incompatible bounds... bullshit
  private static Iterable<Response> allBlobs(
      Instance instance,
      Iterable<build.bazel.remote.execution.v2.Digest> digests,
      DigestFunction.Value digestFunction)
      throws Exception {
    Iterable<Response> blobs = ImmutableList.of();
    Iterator<build.bazel.remote.execution.v2.Digest> iterator = digests.iterator();
    ImmutableList.Builder<build.bazel.remote.execution.v2.Digest> builder = ImmutableList.builder();
    long size = 0;
    long count = 0;
    while (iterator.hasNext()) {
      build.bazel.remote.execution.v2.Digest digest = iterator.next();
      builder.add(digest);
      size += digest.getSizeBytes();
      count++;
      if (size > 2 * 1024 * 1024) {
        // as futures with a transform later?
        blobs = concat(blobs, instance.getAllBlobsFuture(builder.build(), digestFunction).get());
        builder = ImmutableList.builder();
        count = 0;
        size = 0;
      }
    }
    if (count > 0) {
      blobs = concat(blobs, instance.getAllBlobsFuture(builder.build(), digestFunction).get());
    }
    return blobs;
  }

  private static Map<String, Action> actionIndex(
      Instance instance,
      DigestUtil digestUtil,
      Map<Digest, String> actionDigestsNames,
      Map<String, String> names)
      throws Exception {
    ImmutableMap.Builder<String, Action> index = ImmutableMap.builder();
    if (!actionDigestsNames.isEmpty()) {
      System.out.println(format("Fetching actions: %d", actionDigestsNames.size()));
      Iterable<build.bazel.remote.execution.v2.Digest> actionDigests =
          transform(actionDigestsNames.entrySet(), entry -> DigestUtil.toDigest(entry.getKey()));
      Iterable<Action> actions =
          transform(
              allBlobs(instance, actionDigests, digestUtil.getDigestFunction()),
              response -> {
                try {
                  return Action.parseFrom(response.getData());
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
      System.out.println(format("Fetching commands: %d", Iterables.size(actions)));
      Iterable<build.bazel.remote.execution.v2.Digest> commandDigests =
          transform(actions, Action::getCommandDigest);
      Iterable<Command> commands =
          transform(
              allBlobs(instance, commandDigests, digestUtil.getDigestFunction()),
              response -> {
                try {
                  return Command.parseFrom(response.getData());
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
      // need a zip here
      Iterator<Action> actionIter = actions.iterator();
      Iterator<Command> commandIter = commands.iterator();
      while (actionIter.hasNext() && commandIter.hasNext()) {
        Action action = actionIter.next();
        Command command = commandIter.next();

        // TODO prefer output_paths
        String output;
        if (command.getOutputFilesCount() > 0) {
          output = command.getOutputFilesList().get(0);
        } else if (command.getOutputDirectoriesCount() > 0) {
          output = command.getOutputDirectoriesList().get(0);
        } else {
          output = command.getOutputPathsList().get(0);
        }
        index.put(output, action);
        Digest digest = digestUtil.compute(action);
        if (!names.containsKey(output)) {
          names.put(output, actionDigestsNames.get(digest));
        }
      }
    }
    return index.build();
  }

  private static String ds(Digest digest) {
    return DigestUtil.toString(digest);
  }

  private static void diffDir(
      DigestUtil digestUtil,
      String route,
      Directory dirA,
      Directory dirB,
      Map<String, Directory> indexA,
      Map<String, Directory> indexB,
      Map<String, Operation> producersA,
      Map<String, Operation> producersB) {
    Iterator<FileNode> filesA = dirA.getFilesList().iterator();
    Iterator<FileNode> filesB = dirB.getFilesList().iterator();
    FileNode curFileA = null;
    FileNode curFileB = null;
    String routeA = route + "/<nonsense>";
    String routeB = route + "/<nonsense>";
    Digest digestA = null;
    Digest digestB = null;
    while (curFileA != null || curFileB != null || filesA.hasNext() || filesB.hasNext()) {
      if (curFileA == null && filesA.hasNext()) {
        curFileA = filesA.next();
        routeA = route + curFileA.getName();
        digestA = digestUtil.toDigest(curFileA.getDigest());
      }
      if (curFileB == null && filesB.hasNext()) {
        curFileB = filesB.next();
        routeB = route + curFileB.getName();
        digestB = digestUtil.toDigest(curFileB.getDigest());
      }
      int diff;
      if (curFileA != null && curFileB != null) {
        diff = curFileA.getName().compareTo(curFileB.getName());
      } else if (curFileB == null) {
        diff = -1;
      } else {
        diff = 1;
      }
      if (diff < 0) {
        // a < b
        System.out.println(format("%s (%s) Missing in B", routeA, DigestUtil.toString(digestA)));
        curFileA = null;
      } else if (diff == 0) {
        // other fields too...
        if (!digestA.equals(digestB)) {
          System.out.println(
              format(
                  "%s (%s != %s)",
                  routeA, DigestUtil.toString(digestA), DigestUtil.toString(digestB)));
          Operation a = producersA.get(routeA);
          Operation b = producersB.get(routeB);
          if (a != null && b != null) {
            System.out.println(format("  Produced by %s and %s", a.getName(), b.getName()));
          }
        }
        curFileA = null;
        curFileB = null;
      } else {
        // a > b
        System.out.println(format("%s (%s) Missing in A", routeB, DigestUtil.toString(digestB)));
        curFileB = null;
      }
    }

    Iterator<DirectoryNode> dirsA = dirA.getDirectoriesList().iterator();
    Iterator<DirectoryNode> dirsB = dirB.getDirectoriesList().iterator();
    DirectoryNode curDirA = null;
    DirectoryNode curDirB = null;
    while (curDirA != null || curDirB != null || dirsA.hasNext() || dirsB.hasNext()) {
      if (curDirA == null && dirsA.hasNext()) {
        curDirA = dirsA.next();
        routeA = route + curDirA.getName() + "/";
        digestA = digestUtil.toDigest(curDirA.getDigest());
      }
      if (curDirB == null && dirsB.hasNext()) {
        curDirB = dirsB.next();
        routeB = route + curDirB.getName() + "/";
        digestB = digestUtil.toDigest(curDirB.getDigest());
      }
      int diff;
      if (curDirA != null && curDirB != null) {
        diff = curDirA.getName().compareTo(curDirB.getName());
      } else if (curDirB == null) {
        diff = -1;
      } else {
        diff = 1;
      }
      if (diff < 0) {
        // a < b
        System.out.println(format("%s (%s) Missing in B", routeA, DigestUtil.toString(digestA)));
        curDirA = null;
      } else if (diff == 0) {
        // other fields too...
        if (!digestA.equals(digestB)) {
          diffDir(
              digestUtil,
              routeA,
              indexA.get(curDirA.getDigest().getHash()),
              indexB.get(curDirB.getDigest().getHash()),
              indexA,
              indexB,
              producersA,
              producersB);
        }
        curDirA = null;
        curDirB = null;
      } else {
        // a > b
        System.out.println(format("%s (%s) Missing in A", routeB, DigestUtil.toString(digestB)));
        curDirB = null;
      }
    }
  }

  private static Tree tree(Instance instance, Digest digest) {
    Tree.Builder tree = Tree.newBuilder();
    if (!instance.getTree(digest, 1024, "", tree).isEmpty()) {
      throw new RuntimeException("tree did not complete");
    }
    return tree.build();
  }

  private static void diffInputs(
      Instance instance,
      DigestUtil digestUtil,
      Digest inA,
      Digest inB,
      Map<String, Operation> producersA,
      Map<String, Operation> producersB) {
    Tree treeA = tree(instance, inA);
    Tree treeB = tree(instance, inB);
    Map<String, Directory> indexA = treeA.getDirectories();
    Map<String, Directory> indexB = treeB.getDirectories();

    diffDir(
        digestUtil,
        "",
        indexA.get(inA.getHash()),
        indexB.get(inB.getHash()),
        indexA,
        indexB,
        producersA,
        producersB);
  }

  private static void diffAction(
      Instance instance,
      DigestUtil digestUtil,
      String name,
      Action a,
      Action b,
      Map<String, Operation> producersA,
      Map<String, Operation> producersB) {
    Digest digestA = digestUtil.compute(a);
    Digest digestB = digestUtil.compute(b);

    Digest cmdDigestA = digestUtil.toDigest(a.getCommandDigest());
    Digest cmdDigestB = digestUtil.toDigest(b.getCommandDigest());
    if (!cmdDigestA.equals(cmdDigestB)) {
      System.out.println(
          format(
              "%s %s:Command %s != %s:Command %s",
              name, ds(digestA), ds(cmdDigestA), ds(digestB), ds(cmdDigestB)));
    }
    Digest inDigestA = digestUtil.toDigest(a.getInputRootDigest());
    Digest inDigestB = digestUtil.toDigest(b.getInputRootDigest());
    if (!inDigestA.equals(inDigestB)) {
      System.out.println(
          format(
              "%s %s:Input %s != %s:Input %s",
              name, ds(digestA), ds(inDigestA), ds(digestB), ds(inDigestB)));
      diffInputs(instance, digestUtil, inDigestA, inDigestB, producersA, producersB);
    }
  }

  private static List<Operation> loadOrListOps(Instance instance, String invocationId)
      throws IOException {
    Path path = Paths.get(invocationId + ".operations");
    if (Files.exists(path)) {
      ImmutableList.Builder<Operation> ops = ImmutableList.builder();
      try (InputStream in = Files.newInputStream(path)) {
        Operation operation;
        while ((operation = Operation.parseDelimitedFrom(in)) != null) {
          ops.add(operation);
        }
      }
      return ops.build();
    }
    List<Operation> ops = getOps(instance, invocationId);
    try (OutputStream out = Files.newOutputStream(path)) {
      for (Operation operation : ops) {
        operation.writeDelimitedTo(out);
      }
    }
    return ops;
  }

  private static Iterable<String> getProducts(Operation operation) throws IOException {
    ExecuteResponse executeResponse = operation.getResponse().unpack(ExecuteResponse.class);
    return transform(
        executeResponse.getResult().getOutputFilesList(), outputFile -> outputFile.getPath());
  }

  private static Map<String, Operation> getProducers(Iterable<Operation> operations)
      throws IOException {
    ImmutableMap.Builder<String, Operation> producers = ImmutableMap.builder();
    for (Operation operation : operations) {
      for (String product : getProducts(operation)) {
        producers.put(product, operation);
      }
    }
    return producers.build();
  }

  private static void diff(Instance instance, String invocationIdA, String invocationIdB)
      throws Exception {
    List<Operation> opsA = loadOrListOps(instance, invocationIdA);
    List<Operation> opsB = loadOrListOps(instance, invocationIdB);

    if (opsA.isEmpty()) {
      return;
    }

    DigestFunction.Value digestFunction =
        executeOperationMetadata(opsA.get(0).getMetadata()).getDigestFunction();

    if (!opsB.isEmpty()) {
      DigestFunction.Value digestFunctionB =
          executeOperationMetadata(opsB.get(0).getMetadata()).getDigestFunction();
      if (!digestFunction.equals(digestFunctionB)) {
        System.err.println(
            format(
                "DigestFunction does not match between invocations: %s != %s",
                digestFunction.name(), digestFunctionB.name()));
        return;
      }
    }

    DigestUtil digestUtil = new DigestUtil(HashFunction.get(digestFunction));

    Map<String, Operation> producersA = getProducers(opsA);
    Map<String, Operation> producersB = getProducers(opsB);

    if (opsA.size() != opsB.size()) {
      System.out.println(format("Operation Count %d != %d", opsA.size(), opsB.size()));
    } else {
      System.out.println(format("Operations: %d", opsA.size()));
    }

    // we need to also find actions with the same alignment that have different digest outputs
    // these could be indexed by digest, or filename, or something

    Map<Digest, String> actionsA = getActionDigests(opsA);
    Map<Digest, String> actionsB = getActionDigests(opsB);

    Set<Digest> filteredA = difference(actionsA.keySet(), actionsB.keySet());
    Map<Digest, String> differencesAB =
        filterEntries(actionsA, entry -> filteredA.contains(entry.getKey()));
    Set<Digest> filteredB = difference(actionsB.keySet(), actionsA.keySet());
    Map<Digest, String> differencesBA =
        filterEntries(actionsB, entry -> filteredB.contains(entry.getKey()));

    if (differencesAB.size() != differencesBA.size()) {
      System.out.println(
          format(
              "Asymmetric action differences A -> B: %d != B -> A: %d",
              differencesAB.size(), differencesBA.size()));
    }

    Map<String, String> names = new HashMap<>();
    Map<String, Action> outputToActionAB = actionIndex(instance, digestUtil, differencesAB, names);
    Map<String, Action> outputToActionBA = actionIndex(instance, digestUtil, differencesBA, names);
    Set<String> remainingBAOutputs = new HashSet<>(outputToActionBA.keySet());

    System.out.println("Locating differences");
    // align the AB/BA differences with actions on either side
    for (Map.Entry<String, Action> entry : outputToActionAB.entrySet()) {
      String output = entry.getKey();
      if (outputToActionBA.containsKey(output)) {
        // might be redundant to point to the map and fetch out of it
        diffAction(
            instance,
            digestUtil,
            names.get(output),
            entry.getValue(),
            outputToActionBA.get(output),
            producersA,
            producersB);
        remainingBAOutputs.remove(output);
      } else {
        System.out.println(
            format("No alignment for action A -> B: %s with %s", names.get(output), output));
      }
    }

    for (String output : remainingBAOutputs) {
      System.out.println(
          format("No alignment for action B -> A: %s with %s", names.get(output), output));
    }
  }

  private static void usage() {
    System.err.println("Usage: bf-diff <host> <instance> <invocation-A> <invocation-B>");
    System.err.println("Compute difference between collected action executions");
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 4) {
      usage();
      System.exit(1);
    }

    String host = args[0];
    String instanceName = args[1];
    String invocationIdA = args[2];
    String invocationIdB = args[3];

    ManagedChannel channel = createChannel(host);
    Instance instance = new StubInstance(instanceName, "bf-diff", channel, Durations.fromHours(10));
    try {
      diff(instance, invocationIdA, invocationIdB);
    } finally {
      instance.stop();
      channel.shutdown();
      channel.awaitTermination(1, SECONDS);
    }
  }
}
