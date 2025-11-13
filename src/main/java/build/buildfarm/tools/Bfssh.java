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
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.BatchUpdateBlobsRequest.Request;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.ExecutionPolicy;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.Platform.Property;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.ResultsCachePolicy;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.Watcher;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.v1test.Digest;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

class Bfssh {
  static Digest uploadAction(
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

  static ExecuteResponse executeAction(
      Instance instance, Digest actionDigest, RequestMetadata requestMetadata) throws Exception {
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
            true,
            ExecutionPolicy.getDefaultInstance(),
            ResultsCachePolicy.getDefaultInstance(),
            requestMetadata,
            watcher);
    return transform(complete, result -> response.get(), directExecutor()).get();
  }

  static ByteString getBlob(Instance instance, Digest digest, RequestMetadata requestMetadata)
      throws IOException {
    return ByteString.readFrom(
        instance.newBlobInput(Compressor.Value.IDENTITY, digest, 0, 10, SECONDS, requestMetadata));
  }

  public static void main(String[] args) throws Exception {
    String host = args[0];
    String instanceName = args[1];
    String worker = args[2];

    ManagedChannel channel = createChannel(host);

    Instance stub = new StubInstance(instanceName, channel);

    RequestMetadata requestMetadata = RequestMetadata.getDefaultInstance();
    DigestUtil digestUtil = DigestUtil.forHash("SHA256");
    int exitCode;

    Platform platform =
        Platform.newBuilder()
            .addProperties(
                Property.newBuilder().setName("Worker").setValue(worker + ":8981").build())
            .build();

    try {
      Digest actionDigest =
          uploadAction(
              stub,
              digestUtil,
              Arrays.stream(args).skip(3).collect(Collectors.toList()),
              platform,
              requestMetadata);
      ExecuteResponse response = executeAction(stub, actionDigest, requestMetadata);
      ActionResult result = response.getResult();

      if (result.getStdoutDigest().getSizeBytes() != 0) {
        String output =
            getBlob(
                    stub,
                    DigestUtil.fromDigest(
                        result.getStdoutDigest(), actionDigest.getDigestFunction()),
                    requestMetadata)
                .toStringUtf8();
        System.out.print(output);
      }
      if (result.getStderrDigest().getSizeBytes() != 0) {
        String output =
            getBlob(
                    stub,
                    DigestUtil.fromDigest(
                        result.getStderrDigest(), actionDigest.getDigestFunction()),
                    requestMetadata)
                .toStringUtf8();
        System.err.print(output);
      }
      exitCode = result.getExitCode();
    } finally {
      channel.shutdown();
      channel.awaitTermination(1, SECONDS);
    }

    System.exit(exitCode);
  }
}
