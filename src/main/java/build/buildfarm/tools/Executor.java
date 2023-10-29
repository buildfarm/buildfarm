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

import static build.bazel.remote.execution.v2.ExecutionStage.Value.EXECUTING;
import static build.buildfarm.common.grpc.Channels.createChannel;
import static build.buildfarm.common.io.Utils.stat;
import static build.buildfarm.instance.stub.ByteStreamUploader.uploadResourceName;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import build.bazel.remote.execution.v2.BatchUpdateBlobsRequest;
import build.bazel.remote.execution.v2.BatchUpdateBlobsRequest.Request;
import build.bazel.remote.execution.v2.BatchUpdateBlobsResponse;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc.ContentAddressableStorageBlockingStub;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteRequest;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.ExecutionGrpc;
import build.bazel.remote.execution.v2.ExecutionGrpc.ExecutionStub;
import build.bazel.remote.execution.v2.FindMissingBlobsRequest;
import build.bazel.remote.execution.v2.FindMissingBlobsResponse;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.Size;
import build.buildfarm.common.io.FileStatus;
import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamStub;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.SettableFuture;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class Executor {
  static class ExecutionObserver implements StreamObserver<Operation> {
    private final AtomicLong countdown;
    private final AtomicInteger[] statusCounts;
    private final ExecutionStub execStub;
    private final String instanceName;
    private final Digest actionDigest;
    private final Stopwatch stopwatch;
    private final ScheduledExecutorService service;

    private boolean complete = false;
    private String operationName = null;
    private ScheduledFuture<?> noticeFuture;

    ExecutionObserver(
        AtomicLong countdown,
        AtomicInteger[] statusCounts,
        ExecutionStub execStub,
        String instanceName,
        Digest actionDigest,
        ScheduledExecutorService service) {
      this.countdown = countdown;
      this.statusCounts = statusCounts;
      this.execStub = execStub;
      this.instanceName = instanceName;
      this.actionDigest = actionDigest;
      this.service = service;
      stopwatch = Stopwatch.createStarted();
    }

    void execute() {
      execStub.execute(
          ExecuteRequest.newBuilder()
              .setInstanceName(instanceName)
              .setActionDigest(actionDigest)
              .setSkipCacheLookup(true)
              .build(),
          this);
      noticeFuture = service.schedule(this::printStillWaiting, 30, SECONDS);
    }

    void print(int code, String responseType, long micros) {
      System.out.printf(
          "Action: %s -> %s: %s %s in %gms%n",
          DigestUtil.toString(actionDigest),
          operationName,
          responseType,
          Code.forNumber(code),
          micros / 1000.0f);
    }

    void printStillWaiting() {
      if (!complete) {
        noticeFuture = service.schedule(this::printStillWaiting, 30, SECONDS);
        if (operationName == null) {
          System.out.println("StillWaitingFor Operation => " + DigestUtil.toString(actionDigest));
        } else {
          System.out.println("StillWaitingFor Results => " + operationName);
        }
      }
    }

    @Override
    public void onNext(Operation operation) {
      noticeFuture.cancel(false);
      if (operationName == null) {
        operationName = operation.getName();
      }
      long micros = stopwatch.elapsed(MICROSECONDS);
      if (operation.getResponse().is(ExecuteResponse.class)) {
        try {
          complete = true;
          ExecuteResponse response = operation.getResponse().unpack(ExecuteResponse.class);
          int code = response.getStatus().getCode();
          String responseType = "response";
          if (code == Code.OK.getNumber()) {
            responseType += " exit code " + response.getResult().getExitCode();
          }
          print(code, responseType, micros);
          statusCounts[code].incrementAndGet();
        } catch (InvalidProtocolBufferException e) {
          System.err.println(
              "An unlikely error has occurred for " + operationName + ": " + e.getMessage());
        }
      } else if (operation.getMetadata().is(ExecuteOperationMetadata.class)) {
        try {
          ExecuteOperationMetadata metadata =
              operation.getMetadata().unpack(ExecuteOperationMetadata.class);
          if (metadata.getStage() == EXECUTING) {
            stopwatch.reset().start();
          }
        } catch (InvalidProtocolBufferException e) {
          e.printStackTrace();
          System.err.println(
              "An unlikely error has occurred for " + operationName + ": " + e.getMessage());
        }
        noticeFuture = service.schedule(this::printStillWaiting, 30, SECONDS);
      } else {
        noticeFuture = service.schedule(this::printStillWaiting, 30, SECONDS);
      }
    }

    @Override
    public void onError(Throwable t) {
      complete = true;
      noticeFuture.cancel(false);
      long micros = stopwatch.elapsed(MICROSECONDS);
      int code = io.grpc.Status.fromThrowable(t).getCode().value();
      print(code, "error", micros);
      statusCounts[code].incrementAndGet();
      countdown.decrementAndGet();
    }

    @Override
    public void onCompleted() {
      complete = true;
      noticeFuture.cancel(false);
      countdown.decrementAndGet();
    }
  }

  static void executeActions(
      String instanceName, List<Digest> actionDigests, ExecutionStub execStub)
      throws InterruptedException {
    ScheduledExecutorService service = newSingleThreadScheduledExecutor();

    AtomicInteger[] statusCounts = new AtomicInteger[18];
    for (int i = 0; i < statusCounts.length; i++) {
      statusCounts[i] = new AtomicInteger(0);
    }
    AtomicLong countdown = new AtomicLong(actionDigests.size());
    for (Digest actionDigest : actionDigests) {
      ExecutionObserver executionObserver =
          new ExecutionObserver(
              countdown, statusCounts, execStub, instanceName, actionDigest, service);
      executionObserver.execute();
      MICROSECONDS.sleep(1);
    }
    while (countdown.get() != 0) {
      SECONDS.sleep(1);
    }
    for (int i = 0; i < statusCounts.length; i++) {
      AtomicInteger statusCount = statusCounts[i];
      if (statusCount.get() != 0) {
        System.out.println(
            "Status " + Code.forNumber(i) + " : " + statusCount.get() + " responses");
      }
    }

    shutdownAndAwaitTermination(service, 1, SECONDS);
  }

  private static void loadFilesIntoCAS(String instanceName, Channel channel, Path blobsDir)
      throws Exception {
    ContentAddressableStorageBlockingStub casStub =
        ContentAddressableStorageGrpc.newBlockingStub(channel);
    List<Digest> missingDigests = findMissingBlobs(instanceName, blobsDir, casStub);
    UUID uploadId = UUID.randomUUID();

    int[] bucketSizes = new int[128];
    BatchUpdateBlobsRequest.Builder[] buckets = new BatchUpdateBlobsRequest.Builder[128];
    for (int i = 0; i < 128; i++) {
      bucketSizes[i] = 0;
      buckets[i] = BatchUpdateBlobsRequest.newBuilder().setInstanceName(instanceName);
    }

    ByteStreamStub bsStub = ByteStreamGrpc.newStub(channel);
    for (Digest missingDigest : missingDigests) {
      Path path = blobsDir.resolve(missingDigest.getHash() + "_" + missingDigest.getSizeBytes());
      if (missingDigest.getSizeBytes() < Size.mbToBytes(1)) {
        Request request =
            Request.newBuilder()
                .setDigest(missingDigest)
                .setData(ByteString.copyFrom(Files.readAllBytes(path)))
                .build();
        int maxBucketSize = 0;
        long minBucketSize = Size.mbToBytes(2) + 1;
        int maxBucketIndex = 0;
        int minBucketIndex = -1;
        int size = (int) missingDigest.getSizeBytes() + 48;
        for (int i = 0; i < 128; i++) {
          int newBucketSize = bucketSizes[i] + size;
          if (newBucketSize < Size.mbToBytes(2) && bucketSizes[i] < minBucketSize) {
            minBucketSize = bucketSizes[i];
            minBucketIndex = i;
          }
          if (bucketSizes[i] > maxBucketSize) {
            maxBucketSize = bucketSizes[i];
            maxBucketIndex = i;
          }
        }
        if (minBucketIndex < 0) {
          bucketSizes[maxBucketIndex] = size;
          BatchUpdateBlobsRequest batchRequest = buckets[maxBucketIndex].build();
          Stopwatch stopwatch = Stopwatch.createStarted();
          BatchUpdateBlobsResponse batchResponse = casStub.batchUpdateBlobs(batchRequest);
          long usecs = stopwatch.elapsed(MICROSECONDS);
          checkState(
              batchResponse.getResponsesList().stream()
                  .allMatch(response -> Code.forNumber(response.getStatus().getCode()) == Code.OK));
          System.out.println(
              "Updated "
                  + batchRequest.getRequestsCount()
                  + " blobs in "
                  + (usecs / 1000.0)
                  + "ms");
          buckets[maxBucketIndex] =
              BatchUpdateBlobsRequest.newBuilder()
                  .setInstanceName(instanceName)
                  .addRequests(request);
        } else {
          bucketSizes[minBucketIndex] += size;
          buckets[minBucketIndex].addRequests(request);
        }
      } else {
        Stopwatch stopwatch = Stopwatch.createStarted();
        SettableFuture<WriteResponse> writtenFuture = SettableFuture.create();
        StreamObserver<WriteRequest> requestObserver =
            bsStub.write(
                new StreamObserver<WriteResponse>() {
                  @Override
                  public void onNext(WriteResponse response) {
                    writtenFuture.set(response);
                  }

                  @Override
                  public void onCompleted() {}

                  @Override
                  public void onError(Throwable t) {
                    writtenFuture.setException(t);
                  }
                });
        HashCode hash = HashCode.fromString(missingDigest.getHash());
        String resourceName =
            uploadResourceName(instanceName, uploadId, hash, missingDigest.getSizeBytes());
        try (InputStream in = Files.newInputStream(path)) {
          boolean first = true;
          long writtenBytes = 0;
          byte[] buf = new byte[64 * 1024];
          while (writtenBytes != missingDigest.getSizeBytes()) {
            int len = in.read(buf);
            WriteRequest.Builder request = WriteRequest.newBuilder();
            if (first) {
              request.setResourceName(resourceName);
            }
            request.setData(ByteString.copyFrom(buf, 0, len)).setWriteOffset(writtenBytes);
            if (writtenBytes + len == missingDigest.getSizeBytes()) {
              request.setFinishWrite(true);
            }
            requestObserver.onNext(request.build());
            writtenBytes += len;
            first = false;
          }
          writtenFuture.get();
          System.out.println(
              "Wrote long "
                  + DigestUtil.toString(missingDigest)
                  + " in "
                  + (stopwatch.elapsed(MICROSECONDS) / 1000.0)
                  + "ms");
        }
      }
    }
    for (int i = 0; i < 128; i++) {
      if (bucketSizes[i] > 0) {
        BatchUpdateBlobsRequest batchRequest = buckets[i].build();
        Stopwatch stopwatch = Stopwatch.createStarted();
        BatchUpdateBlobsResponse batchResponse = casStub.batchUpdateBlobs(batchRequest);
        long usecs = stopwatch.elapsed(MICROSECONDS);
        checkState(
            batchResponse.getResponsesList().stream()
                .allMatch(response -> Code.forNumber(response.getStatus().getCode()) == Code.OK));
        System.out.println(
            "Updated " + batchRequest.getRequestsCount() + " blobs in " + (usecs / 1000.0) + "ms");
      }
    }
  }

  private static List<Digest> findMissingBlobs(
      String instanceName, Path blobsDir, ContentAddressableStorageBlockingStub casStub)
      throws IOException {
    FindMissingBlobsRequest.Builder request =
        FindMissingBlobsRequest.newBuilder().setInstanceName(instanceName);

    int size = 0;

    ImmutableList.Builder<Digest> missingDigests = ImmutableList.builder();

    System.out.println("Looking for missing blobs");

    final int messagesPerRequest = 2 * 1024 * 1024 / 80;

    System.out.println("Looking for missing blobs");

    Stopwatch stopwatch = Stopwatch.createUnstarted();
    FileStore fileStore = Files.getFileStore(blobsDir);
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(blobsDir)) {
      for (Path file : stream) {
        FileStatus stat = stat(file, /* followSymlinks=*/ false, fileStore);

        Digest digest =
            DigestUtil.buildDigest(file.getFileName().toString().split("_")[0], stat.getSize());

        request.addBlobDigests(digest);
        size++;
        if (size == messagesPerRequest) {
          stopwatch.reset().start();
          FindMissingBlobsResponse response = casStub.findMissingBlobs(request.build());
          System.out.println(
              "Found "
                  + response.getMissingBlobDigestsCount()
                  + " missing digests in "
                  + (stopwatch.elapsed(MICROSECONDS) / 1000.0)
                  + "ms");
          missingDigests.addAll(response.getMissingBlobDigestsList());
          request = FindMissingBlobsRequest.newBuilder().setInstanceName(instanceName);
          size = 0;
        }
      }
    }

    if (size > 0) {
      FindMissingBlobsResponse response = casStub.findMissingBlobs(request.build());
      System.out.println("Found " + response.getMissingBlobDigestsCount() + " missing digests");
      missingDigests.addAll(response.getMissingBlobDigestsList());
    }
    return missingDigests.build();
  }

  public static void main(String[] args) throws Exception {
    String host = args[0];
    String instanceName = args[1];
    String blobsDir;
    if (args.length == 3) {
      blobsDir = args[2];
    } else {
      blobsDir = null;
    }

    Scanner scanner = new Scanner(System.in);

    ImmutableList.Builder<Digest> actionDigests = ImmutableList.builder();
    while (scanner.hasNext()) {
      actionDigests.add(DigestUtil.parseDigest(scanner.nextLine()));
    }

    ManagedChannel channel = createChannel(host);

    if (blobsDir != null) {
      System.out.println("Loading blobs into cas");
      loadFilesIntoCAS(instanceName, channel, Paths.get(blobsDir));
    }

    ExecutionStub execStub = ExecutionGrpc.newStub(channel);

    executeActions(instanceName, actionDigests.build(), execStub);

    channel.shutdown();
    channel.awaitTermination(1, SECONDS);
  }
}
