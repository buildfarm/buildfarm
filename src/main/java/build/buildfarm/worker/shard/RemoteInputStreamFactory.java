// Copyright 2018 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker.shard;

import static build.buildfarm.instance.shard.Util.correctMissingBlob;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.common.ShardBackplane;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.shard.ShardInstance.WorkersCallback;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.Retrier;
import build.buildfarm.instance.stub.Retrier.Backoff;
import build.buildfarm.instance.stub.StubInstance;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.nio.file.NoSuchFileException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class RemoteInputStreamFactory implements InputStreamFactory {
  private final String publicName;
  private final ShardBackplane backplane;
  private final Random rand;
  private final DigestUtil digestUtil;
  private final Map<String, StubInstance> workerStubs = Maps.newHashMap();
  private final ListeningScheduledExecutorService retryScheduler =
      MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(1));

  RemoteInputStreamFactory(String publicName, ShardBackplane backplane, Random rand, DigestUtil digestUtil) {
    this.publicName = publicName;
    this.backplane = backplane;
    this.rand = rand;
    this.digestUtil = digestUtil;
  }

  private static ManagedChannel createChannel(String target) {
    NettyChannelBuilder builder =
        NettyChannelBuilder.forTarget(target)
            .negotiationType(NegotiationType.PLAINTEXT);
    return builder.build();
  }

  private static Retrier createStubRetrier() {
    return new Retrier(
        Backoff.exponential(
            java.time.Duration.ofMillis(/*options.experimentalRemoteRetryStartDelayMillis=*/ 100),
            java.time.Duration.ofMillis(/*options.experimentalRemoteRetryMaxDelayMillis=*/ 5000),
            /*options.experimentalRemoteRetryMultiplier=*/ 2,
            /*options.experimentalRemoteRetryJitter=*/ 0.1,
            /*options.experimentalRemoteRetryMaxAttempts=*/ 5),
        Retrier.DEFAULT_IS_RETRIABLE);
  }

  private ByteStreamUploader createStubUploader(Channel channel) {
    return new ByteStreamUploader("", channel, null, 300, createStubRetrier(), retryScheduler);
  }

  private StubInstance workerStub(String worker) {
    StubInstance instance = workerStubs.get(worker);
    if (instance == null) {
      ManagedChannel channel = createChannel(worker);
      instance = new StubInstance(
          "", digestUtil, channel,
          10 /* FIXME CONFIG */, TimeUnit.SECONDS,
          createStubUploader(channel));
      workerStubs.put(worker, instance);
    }
    return instance;
  }

  private InputStream fetchBlobFromRemoteWorker(Digest blobDigest, Deque<String> workers, long offset) throws IOException, InterruptedException {
    String worker = workers.removeFirst();
    try {
      Instance instance = workerStub(worker);

      InputStream input = instance.newStreamInput(instance.getBlobName(blobDigest), offset);
      // ensure that if the blob cannot be fetched, that we throw here
      input.available();
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
      return input;
    } catch (StatusRuntimeException e) {
      Status st = Status.fromThrowable(e);
      if (st.getCode().equals(Code.UNAVAILABLE)) {
        // for now, leave this up to schedulers
        // removeMalfunctioningWorker(worker, e, "getBlob(" + DigestUtil.toString(blobDigest) + ")");
      } else if (st.getCode() == Code.NOT_FOUND) {
        // ignore this, the worker will update the backplane eventually
      } else if (Retrier.DEFAULT_IS_RETRIABLE.test(st)) {
        // why not, always
        workers.addLast(worker);
      } else {
        throw e;
      }
    }
    throw new NoSuchFileException(DigestUtil.toString(blobDigest));
  }

  @Override
  public InputStream newInput(Digest blobDigest, long offset) throws IOException, InterruptedException {
    Set<String> remoteWorkers;
    Set<String> locationSet;
    try {
      Set<String> workers = backplane.getWorkers();
      remoteWorkers = Sets.difference(workers, ImmutableSet.<String>of(publicName));
      locationSet = Sets.newHashSet(Sets.intersection(backplane.getBlobLocationSet(blobDigest), workers));
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }

    if (locationSet.remove(publicName)) {
      backplane.removeBlobLocation(blobDigest, publicName);
    }
    List<String> workersList = new ArrayList<>(locationSet);
    boolean emptyWorkerList = workersList.isEmpty();
    final ListenableFuture<List<String>> populatedWorkerListFuture;
    if (emptyWorkerList) {
      populatedWorkerListFuture = transform(
          correctMissingBlob(backplane, remoteWorkers, locationSet, this::workerStub, blobDigest, newDirectExecutorService()),
          (foundOnWorkers) -> {
            Iterables.addAll(workersList, foundOnWorkers);
            return workersList;
          });
    } else {
      populatedWorkerListFuture = immediateFuture(workersList);
    }
    SettableFuture<InputStream> inputStreamFuture = SettableFuture.create();
    addCallback(populatedWorkerListFuture, new WorkersCallback(rand) {
      boolean triedCheck = emptyWorkerList;

      @Override
      public void onQueue(Deque<String> workers) {
        Set<String> locationSet = Sets.newHashSet(workers);
        boolean complete = false;
        while (!complete && !workers.isEmpty()) {
          try {
            inputStreamFuture.set(fetchBlobFromRemoteWorker(blobDigest, workers, offset));
            complete = true;
          } catch (IOException e) {
            if (workers.isEmpty()) {
              if (triedCheck) {
                onFailure(e);
                return;
              }
              triedCheck = true;

              workersList.clear();
              try {
                ListenableFuture<List<String>> checkedWorkerListFuture = transform(
                    correctMissingBlob(backplane, remoteWorkers, locationSet, (worker) -> workerStub(worker), blobDigest, newDirectExecutorService()),
                    (foundOnWorkers) -> {
                      Iterables.addAll(workersList, foundOnWorkers);
                      return workersList;
                    });
                addCallback(checkedWorkerListFuture, this);
                complete = true;
              } catch (IOException checkException) {
                complete = true;
                onFailure(checkException);
              }
            }
          } catch (InterruptedException e) {
            complete = true;
            onFailure(e);
          }
        }
      }

      @Override
      public void onFailure(Throwable t) {
        Status status = Status.fromThrowable(t);
        if (status.getCode() == Code.NOT_FOUND) {
          inputStreamFuture.setException(new NoSuchFileException(DigestUtil.toString(blobDigest)));
        } else {
          inputStreamFuture.setException(t);
        }
      }
    });
    try {
      return inputStreamFuture.get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      if (e.getCause() instanceof InterruptedException) {
        throw (InterruptedException) e.getCause();
      }
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new UncheckedExecutionException(e.getCause());
    }
  }
}
