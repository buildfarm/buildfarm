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
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.BatchUpdateBlobsRequest.Request;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.ExecutionPolicy;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.ResultsCachePolicy;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.Watcher;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.v1test.Digest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.devtools.common.options.Option;
import com.google.devtools.common.options.OptionsBase;
import com.google.devtools.common.options.OptionsParser;
import com.google.devtools.common.options.OptionsParsingException;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

class Bfssh {
  public static class BfsshOptions extends OptionsBase {
    @Option(name = "help", abbrev = 'h', help = "Prints usage info.", defaultValue = "false")
    public boolean help;

    @Option(
        name = "all",
        abbrev = 'a',
        help = "Send an execution for all workers",
        defaultValue = "false")
    public boolean all;

    @Option(
        name = "worker",
        abbrev = 'w',
        help = "Send an execution to a specific worker",
        allowMultiple = true,
        defaultValue = "")
    public List<String> workers;
  }

  private static Digest uploadAction(
      Instance instance,
      DigestUtil digestUtil,
      Iterable<String> args,
      Platform platform,
      RequestMetadata requestMetadata)
      throws Exception {
    Command command = Command.newBuilder().addAllArguments(args).setPlatform(platform).build();
    Digest commandDigest = digestUtil.compute(command);
    Action action =
        Action.newBuilder()
            .setCommandDigest(DigestUtil.toDigest(commandDigest))
            .setPlatform(platform)
            .setDoNotCache(true)
            .build();
    Digest actionDigest = digestUtil.compute(action);
    ImmutableList<Request> requests =
        ImmutableList.of(
            Request.newBuilder()
                .setDigest(DigestUtil.toDigest(commandDigest))
                .setData(command.toByteString())
                .build(),
            Request.newBuilder()
                .setDigest(DigestUtil.toDigest(actionDigest))
                .setData(action.toByteString())
                .build());
    instance.putAllBlobs(requests, digestUtil.getDigestFunction(), requestMetadata);
    return actionDigest;
  }

  static ListenableFuture<ExecuteResponse> executeAction(
      Instance instance, Digest actionDigest, RequestMetadata requestMetadata) {
    AtomicReference<ExecuteResponse> response = new AtomicReference<>();
    Watcher watcher =
        new Watcher() {
          String name;

          @Override
          public void observe(Operation operation) {
            if (name == null) {
              name = operation.getName();
              System.out.println(name);
            }
            if (operation.getDone()) {
              try {
                response.set(operation.getResponse().unpack(ExecuteResponse.class));
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
          }
        };
    ListenableFuture<Void> complete =
        instance.execute(
            actionDigest,
            /* skipCacheLookup= */ true,
            ExecutionPolicy.getDefaultInstance(),
            ResultsCachePolicy.getDefaultInstance(),
            requestMetadata,
            watcher);
    return transform(complete, result -> response.get(), directExecutor());
  }

  static ByteString getBlob(Instance instance, Digest digest, RequestMetadata requestMetadata)
      throws IOException {
    return ByteString.readFrom(
        instance.newBlobInput(Compressor.Value.IDENTITY, digest, 0, 10, SECONDS, requestMetadata));
  }

  private static OptionsParser getOptionsParser(Class clazz, String[] args)
      throws OptionsParsingException {
    OptionsParser parser = OptionsParser.newOptionsParser(clazz);
    parser.parse(args);
    return parser;
  }

  private static Iterable<String> getAllWorkers(Instance stub) {
    return stub.backplaneStatus().getActiveExecuteWorkersList();
  }

  private static void usage() {
    System.err.println("Usage: bfssh <endpoint> <instanceName> [-a|-w workers] cmd...");
    System.exit(1);
  }

  public static void main(String[] args) throws Exception {
    String host = args[0];
    String instanceName = args[1];

    ManagedChannel channel = createChannel(host);

    RequestMetadata requestMetadata = RequestMetadata.getDefaultInstance();
    DigestUtil digestUtil = DigestUtil.forHash("SHA256");

    OptionsParser parser = getOptionsParser(BfsshOptions.class, args);
    BfsshOptions options = parser.getOptions(BfsshOptions.class);

    if (options.help) {
      usage();
    }

    Iterable<String> cmdArgs = parser.getResidue().stream().skip(2).collect(Collectors.toList());

    Instance stub = new StubInstance(instanceName, channel);

    Iterable<String> workers;
    if (options.all) {
      workers = getAllWorkers(stub);
    } else if (options.workers.isEmpty()) {
      // trigger a single worker-agnostic submission
      workers = ImmutableList.of("");
    } else {
      workers = options.workers;
    }

    int exitCode;
    ExecutorService printExecutor = newSingleThreadExecutor();
    try {
      Iterable<ListenableFuture<ExecuteResponse>> responses =
          runOnWorkers(stub, digestUtil, workers, cmdArgs, requestMetadata);
      exitCode =
          printResponses(
              stub, responses, digestUtil.getDigestFunction(), requestMetadata, printExecutor);
    } finally {
      printExecutor.shutdownNow();
      printExecutor.awaitTermination(1, SECONDS);
      channel.shutdown();
      channel.awaitTermination(1, SECONDS);
    }
    System.exit(exitCode);
  }

  private static int printResponses(
      Instance stub,
      Iterable<ListenableFuture<ExecuteResponse>> responses,
      DigestFunction.Value digestFunction,
      RequestMetadata requestMetadata,
      Executor executor)
      throws Exception {
    Iterable<ListenableFuture<Integer>> exitCodes =
        Iterables.transform(
            responses,
            response ->
                transformAsync(
                    response,
                    r -> {
                      ActionResult result = r.getResult();
                      printResult(stub, result, digestFunction, requestMetadata);
                      return immediateFuture(result.getExitCode());
                    },
                    executor));
    int maxExitCode = 0;
    for (ListenableFuture<Integer> exitCode : exitCodes) {
      int code = exitCode.get();
      if (code > maxExitCode) {
        maxExitCode = code;
      }
    }
    return maxExitCode;
  }

  private static void printResult(
      Instance stub,
      ActionResult result,
      DigestFunction.Value digestFunction,
      RequestMetadata requestMetadata)
      throws Exception {
    if (result.getStdoutDigest().getSizeBytes() != 0) {
      String output =
          getBlob(
                  stub,
                  DigestUtil.fromDigest(result.getStdoutDigest(), digestFunction),
                  requestMetadata)
              .toStringUtf8();
      System.out.print(output);
    }
    if (result.getStderrDigest().getSizeBytes() != 0) {
      String output =
          getBlob(
                  stub,
                  DigestUtil.fromDigest(result.getStderrDigest(), digestFunction),
                  requestMetadata)
              .toStringUtf8();
      System.err.print(output);
    }
  }

  private static Iterable<ListenableFuture<ExecuteResponse>> runOnWorkers(
      Instance stub,
      DigestUtil digestUtil,
      Iterable<String> workers,
      Iterable<String> args,
      RequestMetadata requestMetadata)
      throws Exception {
    ImmutableList.Builder<ListenableFuture<ExecuteResponse>> responses = ImmutableList.builder();
    for (String worker : workers) {
      Platform.Builder platform = Platform.newBuilder();
      if (!worker.isEmpty()) {
        platform.addPropertiesBuilder().setName("Worker").setValue(worker);
      }

      Digest actionDigest = uploadAction(stub, digestUtil, args, platform.build(), requestMetadata);
      responses.add(executeAction(stub, actionDigest, requestMetadata));
    }
    return responses.build();
  }
}
