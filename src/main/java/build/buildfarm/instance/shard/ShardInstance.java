// Copyright 2017 The Bazel Authors. All rights reserved.
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

package build.buildfarm.instance.shard;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.ShardBackplane;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.Retrier;
import build.buildfarm.instance.stub.Retrier.Backoff;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.v1test.ShardInstanceConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.OutputFile;
import com.google.devtools.remoteexecution.v1test.Platform;
import com.google.devtools.remoteexecution.v1test.UpdateBlobRequest;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata.Stage;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import javax.naming.ConfigurationException;

public class ShardInstance implements Instance {
  private final String name;
  private DigestUtil digestUtil;
  private final ShardInstanceConfig config;
  private final ShardBackplane backplane;
  private final Map<String, StubInstance> workerStubs;
  private final ListeningScheduledExecutorService retryScheduler =
      MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(1));

  public ShardInstance(String name, DigestUtil digestUtil, ShardInstanceConfig config) throws InterruptedException, ConfigurationException {
    this.name = name;
    this.digestUtil = digestUtil;
    this.config = config;
    ShardInstanceConfig.BackplaneCase backplaneCase = config.getBackplaneCase();
    switch (backplaneCase) {
      default:
      case BACKPLANE_NOT_SET:
        throw new IllegalArgumentException("Shard Backplane not set in config");
      case REDIS_SHARD_BACKPLANE_CONFIG:
        backplane = new RedisShardBackplane(config.getRedisShardBackplaneConfig());
        break;
    }
    workerStubs = new ConcurrentHashMap<>();
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public DigestUtil getDigestUtil() {
    return digestUtil;
  }

  @Override
  public ActionResult getActionResult(ActionKey actionKey) {
    ActionResult actionResult = backplane.getActionResult(actionKey);
    if (actionResult == null) {
      return actionResult;
    }

    // FIXME output dirs
    // FIXME inline content
    Iterable<OutputFile> outputFiles = actionResult.getOutputFilesList();
    Iterable<Digest> outputDigests = Iterables.transform(outputFiles, (outputFile) -> outputFile.getDigest());
    if (Iterables.isEmpty(findMissingBlobs(outputDigests))) {
      return actionResult;
    }

    // some of our outputs are no longer in the CAS, remove the actionResult
    backplane.removeActionResult(actionKey);
    return null;
  }

  @Override
  public void putActionResult(ActionKey actionKey, ActionResult actionResult) {
    backplane.putActionResult(actionKey, actionResult);
  }

  @Override
  public Iterable<Digest> findMissingBlobs(Iterable<Digest> blobDigests) {
    ImmutableList.Builder<Digest> missingDigests = new ImmutableList.Builder<>();
    Map<String, ImmutableList.Builder<Digest>> workerDigestBuilders = new HashMap<>();
    Map<Digest, Integer> blobDigestScores = new HashMap<>();
    for (Digest blobDigest : blobDigests) {
      Set<String> workers = backplane.getBlobLocationSet(blobDigest);
      if (workers.isEmpty()) {
        missingDigests.add(blobDigest);
      }
      blobDigestScores.put(blobDigest, workers.size());
      for (String worker : workers) {
        if (!workerDigestBuilders.containsKey(worker)) {
          workerDigestBuilders.put(worker, new ImmutableList.Builder<>());
        }
        workerDigestBuilders.get(worker).add(blobDigest);
      }
    }

    for (String worker : workerDigestBuilders.keySet()) {
      Iterable<Digest> workerMissingDigests = Iterables.filter(
          workerDigestBuilders.get(worker).build(),
          (blobDigest) -> blobDigestScores.containsKey(blobDigest));
      if (!Iterables.isEmpty(workerMissingDigests) && backplane.isWorker(worker)) {
        try {
          workerMissingDigests = workerStub(worker).findMissingBlobs(workerMissingDigests);
        } catch (StatusRuntimeException ex) {
          if (ex.getStatus().getCode().equals(Status.UNAVAILABLE.getCode())) {
            removeMalfunctioningWorker(worker);
          } else {
            continue;
          }
        }
      }

      for (Digest blobDigest : workerMissingDigests) {
        Integer score = blobDigestScores.get(blobDigest);
        if (score == 1) {
          missingDigests.add(blobDigest);
          blobDigestScores.remove(blobDigest);
        } else {
          blobDigestScores.put(blobDigest, blobDigestScores.get(blobDigest) - 1);
        }

        backplane.removeBlobLocation(blobDigest, worker);
      }

      if (blobDigestScores.isEmpty()) {
        break;
      }
    }
    return missingDigests.build();
  }

  @Override
  public Iterable<Digest> putAllBlobs(Iterable<ByteString> blobs)
      throws IOException, IllegalArgumentException, InterruptedException {
      throw new UnsupportedOperationException();
  }

  @Override
  public String getBlobName(Digest blobDigest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteString getBlob(Digest blobDigest) {
    String worker = backplane.getBlobLocation(blobDigest);
    return workerStub(worker).getBlob(blobDigest);
  }

  @Override
  public ByteString getBlob(Digest blobDigest, long offset, long limit) {
    for (;;) {
      String worker = backplane.getBlobLocation(blobDigest);
      if (worker == null) {
        return null;
      }
      try {
        return workerStub(worker).getBlob(blobDigest, offset, limit);
      } catch (StatusRuntimeException ex) {
        if (ex.getStatus().getCode().equals(Status.UNAVAILABLE.getCode())) {
          backplane.removeBlobLocation(blobDigest, worker);
          removeMalfunctioningWorker(worker);
        }
      }
    }
  }

  private static Channel createChannel(String target) {
    NettyChannelBuilder builder =
        NettyChannelBuilder.forTarget(target)
            .negotiationType(NegotiationType.PLAINTEXT);
    return builder.build();
  }

  private static Retrier createStubRetrier() {
    return new Retrier(
        Backoff.exponential(
            Duration.ofMillis(/*options.experimentalRemoteRetryStartDelayMillis=*/ 100),
            Duration.ofMillis(/*options.experimentalRemoteRetryMaxDelayMillis=*/ 5000),
            /*options.experimentalRemoteRetryMultiplier=*/ 2,
            /*options.experimentalRemoteRetryJitter=*/ 0.1,
            /*options.experimentalRemoteRetryMaxAttempts=*/ 5),
        Retrier.DEFAULT_IS_RETRIABLE);
  }

  private ByteStreamUploader createStubUploader(Channel channel) {
    return new ByteStreamUploader("", channel, null, 300, createStubRetrier(), retryScheduler);
  }

  private synchronized StubInstance workerStub(String worker) {
    StubInstance instance = workerStubs.get(worker);
    if (instance == null) {
      Channel channel = createChannel(worker);
      instance = new StubInstance(
          "", digestUtil, channel,
          60 /* FIXME CONFIG */, TimeUnit.SECONDS,
          createStubUploader(channel));
      workerStubs.put(worker, instance);
    }
    return instance;
  }

  @Override
  public Digest putBlob(ByteString blob)
      throws IOException, IllegalArgumentException, InterruptedException {
    for(;;) {
      String worker = null;
      try {
        worker = backplane.getRandomWorker();
        if (worker == null) {
          // FIXME should we wait for a worker to become available?
          throw new StatusRuntimeException(Status.UNAVAILABLE);
        }
        return workerStub(worker).putBlob(blob);
      } catch (IOException ex) {
        removeMalfunctioningWorker(worker);
      }
    }
  }

  private void removeMalfunctioningWorker(String worker) {
    if (worker == null) {
      return;
    }

    workerStubs.remove(worker);
    backplane.removeWorker(worker);
  }

  @Override
  public String getTree(
      Digest rootDigest,
      int pageSize,
      String pageToken,
      ImmutableList.Builder<Directory> directories) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OutputStream getStreamOutput(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream newStreamInput(String name) {
    throw new UnsupportedOperationException();
  }

  private String createOperationName(String id) {
    return getName() + "/operations/" + id;
  }

  private static ExecuteOperationMetadata createExecuteOperationMetadata(ActionKey actionKey) {
    return ExecuteOperationMetadata.newBuilder()
        .setActionDigest(actionKey.getDigest())
        .build();
  }

  private Operation createOperation(ExecuteOperationMetadata metadata) {
    String name = createOperationName(UUID.randomUUID().toString());

    Operation.Builder operationBuilder = Operation.newBuilder()
        .setName(name)
        .setDone(false)
        .setMetadata(Any.pack(metadata));

    return operationBuilder.build();
  }

  @Override
  public void execute(
      Action action,
      boolean skipCacheLookup,
      int totalInputFileCount,
      long totalInputFileBytes,
      Consumer<Operation> onOperation) {
    ExecuteOperationMetadata metadata = createExecuteOperationMetadata(digestUtil.computeActionKey(action));
    Operation operation = createOperation(metadata);

    backplane.putOperation(operation);

    onOperation.accept(operation);

    // FIXME lookup

    // FIXME validation

    // make the action available to the worker
    try {
      putBlob(action.toByteString());
    } catch (IOException|IllegalArgumentException|InterruptedException ex) {
      ex.printStackTrace();
      return;
    }

    metadata = metadata.toBuilder()
        .setStage(ExecuteOperationMetadata.Stage.QUEUED)
        .build();

    final Operation queuedOperation = operation.toBuilder()
        .setMetadata(Any.pack(metadata))
        .build();

    backplane.putOperation(queuedOperation);
  }

  @Override
  public void match(Platform platform, boolean requeueOnFailure, Predicate<Operation> onMatch) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean putOperation(Operation operation) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean pollOperation(String operationName, Stage stage) {
    throw new UnsupportedOperationException();
  }

  // returns nextPageToken suitable for list restart
  @Override
  public String listOperations(
      int pageSize,
      String pageToken,
      String filter,
      ImmutableList.Builder<Operation> operations) {
    /*
    try (Jedis jedis = backplane.getJedis()) {
      operations.add(parseOperationJson(jedis.get("Operation:" + operation)));
    } catch (InvalidProtocolBufferException ex) {
      ex.printStackTrace();
    }
    return "";
    */
    throw new UnsupportedOperationException();
  }

  @Override
  public Operation getOperation(String name) {
    return backplane.getOperation(name);
  }

  @Override
  public void cancelOperation(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteOperation(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean watchOperation(
      String operationName,
      boolean watchInitialState,
      Predicate<Operation> watcher) {
    if (watchInitialState) {
      Operation operation = getOperation(operationName);
      if (!watcher.test(operation)) {
        return false;
      }
      if (operation == null || operation.getDone()) {
        return true;
      }
    }

    return backplane.watchOperation(operationName, watcher);
  }
}
