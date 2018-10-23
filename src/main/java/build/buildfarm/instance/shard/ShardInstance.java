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

import static build.buildfarm.instance.shard.Util.SHARD_IS_RETRIABLE;
import static build.buildfarm.instance.shard.Util.correctMissingBlob;
import static com.google.common.base.Predicates.or;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.ShardBackplane;
import build.buildfarm.common.cache.Cache;
import build.buildfarm.common.cache.CacheBuilder;
import build.buildfarm.common.cache.CacheLoader.InvalidCacheLoadException;
import build.buildfarm.instance.AbstractServerInstance;
import build.buildfarm.instance.GetDirectoryFunction;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.TokenizableIterator;
import build.buildfarm.instance.TreeIterator;
import build.buildfarm.instance.TreeIterator.DirectoryEntry;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.Retrier;
import build.buildfarm.instance.stub.Retrier.Backoff;
import build.buildfarm.instance.stub.RetryException;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.v1test.CompletedOperationMetadata;
import build.buildfarm.v1test.OperationIteratorToken;
import build.buildfarm.v1test.ShardInstanceConfig;
import build.buildfarm.v1test.QueuedOperationMetadata;
import build.buildfarm.v1test.ExecutingOperationMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Command;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.OutputFile;
import com.google.devtools.remoteexecution.v1test.Platform;
import com.google.devtools.remoteexecution.v1test.UpdateBlobRequest;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata.Stage;
import com.google.devtools.remoteexecution.v1test.ExecuteResponse;
import com.google.devtools.remoteexecution.v1test.RequestMetadata;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Channel;
import io.grpc.Context;
import io.grpc.Context.CancellableContext;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.protobuf.StatusProto;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Logger;
import javax.naming.ConfigurationException;

public class ShardInstance extends AbstractServerInstance {
  private final static Logger logger = Logger.getLogger(ShardInstance.class.getName());

  private final Runnable onStop;
  private final ShardBackplane backplane;
  private final LoadingCache<String, Instance> workerStubs;
  private final Thread dispatchedMonitor;
  private final Cache<Digest, Directory> directoryCache = CacheBuilder.newBuilder()
      .maximumSize(64 * 1024)
      .build();
  private final Cache<Digest, Command> commandCache = CacheBuilder.newBuilder()
      .maximumSize(64 * 1024)
      .build();
  private final com.google.common.cache.RemovalListener<String, Instance> instanceRemovalListener = (removal) -> {
    Instance instance = removal.getValue();
    if (instance instanceof StubInstance) {
      ManagedChannel channel = ((StubInstance) instance).getChannel();
      channel.shutdownNow();
      try {
        channel.awaitTermination(0, TimeUnit.SECONDS);
      } catch (InterruptedException intEx) {
        /* impossible, 0 timeout */
        Thread.currentThread().interrupt();
      }
    }
  };
  private final Random rand = new Random();
  private ExecutorService operationDeletionService = Executors.newSingleThreadExecutor();
  private Thread operationQueuer;
  private ListeningExecutorService operationTransformService = listeningDecorator(Executors.newFixedThreadPool(24));

  public ShardInstance(String name, DigestUtil digestUtil, ShardInstanceConfig config, Runnable onStop)
      throws InterruptedException, ConfigurationException {
    this(
        name,
        digestUtil,
        getBackplane(config),
        config.getRunDispatchedMonitor(),
        /* runOperationQueuer=*/ true,
        onStop,
        new CacheLoader<String, Instance>() {
          @Override
          public Instance load(String worker) {
          ManagedChannel channel = createChannel(worker);
          return new StubInstance(
              "", digestUtil, channel,
              60 /* FIXME CONFIG */, TimeUnit.SECONDS,
              createStubRetrier(),
              null);
          }
        });
  }

  private static ShardBackplane getBackplane(ShardInstanceConfig config)
      throws ConfigurationException {
    ShardInstanceConfig.BackplaneCase backplaneCase = config.getBackplaneCase();
    switch (backplaneCase) {
      default:
      case BACKPLANE_NOT_SET:
        throw new IllegalArgumentException("Shard Backplane not set in config");
      case REDIS_SHARD_BACKPLANE_CONFIG:
        return new RedisShardBackplane(
            config.getRedisShardBackplaneConfig(),
            ShardInstance::stripOperation,
            ShardInstance::stripOperation,
            /* isPrequeued=*/ ShardInstance::isUnknown,
            /* isExecuting=*/ or(ShardInstance::isExecuting, ShardInstance::isQueued));
    }
  }

  public ShardInstance(
      String name,
      DigestUtil digestUtil,
      ShardBackplane backplane,
      boolean runDispatchedMonitor,
      boolean runOperationQueuer,
      Runnable onStop,
      CacheLoader<String, Instance> instanceLoader)
      throws InterruptedException {
    super(name, digestUtil, null, null, null, null, null);
    this.backplane = backplane;
    this.onStop = onStop;
    backplane.setOnUnsubscribe(this::stop);
    workerStubs = com.google.common.cache.CacheBuilder.newBuilder()
      .removalListener(instanceRemovalListener)
      .build(instanceLoader);

    if (runDispatchedMonitor) {
      dispatchedMonitor = new Thread(new DispatchedMonitor(
          backplane,
          (operationName) -> {
            try {
              return requeueOperation(operationName);
            } catch (IOException e) {
              e.printStackTrace();
              return false;
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              return true;
            }
          },
          (operationName) -> {
            try {
              errorOperation(operationName, com.google.rpc.Status.newBuilder()
                  .setCode(com.google.rpc.Code.FAILED_PRECONDITION.getNumber())
                  // details...
                  .build());
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }));
    } else {
      dispatchedMonitor = null;
    }

    if (runOperationQueuer) {
      operationQueuer = new Thread(new Runnable() {
        void ensureCanQueue() throws IOException, InterruptedException {
          while (!backplane.canQueue()) {
            TimeUnit.MILLISECONDS.sleep(100);
          }
        }

        void iterate() throws IOException, InterruptedException {
          ensureCanQueue(); // wait for transition to canQueue state
          String operationName = backplane.deprequeueOperation();
          if (operationName == null) {
            System.err.println("Got null from deprequeue...");
          } else {
            try {
              Operation operation = backplane.getOperation(operationName);
              if (operation == null) {
                backplane.deleteOperation(operationName);
              } else if (isUnknown(operation)) {
                queue(operation);
              }
            } catch (IOException e) {
              backplane.prequeueOperation(operationName);
            }
          }
        }

        @Override
        public void run() {
          System.out.println("OperationQueuer: Running");
          try {
            for (;;) {
              try {
                iterate();
              } catch (IOException e) {
                // problems interacting with backplane
              }
            }
          } catch (InterruptedException e) {
            // treat with exit
          } catch (Throwable t) {
            t.printStackTrace();
          } finally {
            System.out.println("OperationQueuer: Exiting");
          }
          operationQueuer = null;
          stop();
        }
      });
    } else {
      operationQueuer = null;
    }
  }

  @Override
  public void start() {
    backplane.start();
    if (dispatchedMonitor != null) {
      dispatchedMonitor.start();
    }
    if (operationQueuer != null) {
      operationQueuer.start();
    }
  }

  @Override
  public void stop() {
    if (operationQueuer != null) {
      operationQueuer.stop();
    }
    if (dispatchedMonitor != null) {
      dispatchedMonitor.stop();
    }
    operationDeletionService.shutdown();
    operationTransformService.shutdown();
    backplane.stop();
    onStop.run();
    try {
      if (!operationDeletionService.awaitTermination(10, TimeUnit.SECONDS)) {
        System.err.println("Could not shut down operation deletion service, some operations may be zombies");
      }
      operationDeletionService.shutdownNow();
    } catch (InterruptedException intEx) {
      Thread.currentThread().interrupt();
    }
    try {
      if (!operationTransformService.awaitTermination(10, TimeUnit.SECONDS)) {
        System.err.println("Could not shut down operation transform service");
      }
      operationTransformService.shutdownNow();
    } catch (InterruptedException intEx) {
      Thread.currentThread().interrupt();
    }
  }

  private ActionResult getActionResultFromBackplane(ActionKey actionKey)
      throws IOException {
    // System.out.println("getActionResult: " + DigestUtil.toString(actionKey.getDigest()));
    ActionResult actionResult = backplane.getActionResult(actionKey);
    if (actionResult == null) {
      return null;
    }

    // FIXME output dirs
    // FIXME inline content
    Iterable<OutputFile> outputFiles = actionResult.getOutputFilesList();
    Iterable<Digest> outputDigests = Iterables.transform(outputFiles, (outputFile) -> outputFile.getDigest());
    try {
      if (Iterables.isEmpty(findMissingBlobs(outputDigests, newDirectExecutorService()).get())) {
        return actionResult;
      }
    } catch (ExecutionException e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new UncheckedExecutionException(e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Status.CANCELLED.asRuntimeException();
    }

    // some of our outputs are no longer in the CAS, remove the actionResult
    backplane.removeActionResult(actionKey);
    return null;
  }

  @Override
  public ActionResult getActionResult(ActionKey actionKey) {
    try {
      return getActionResultFromBackplane(actionKey);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  @Override
  public void putActionResult(ActionKey actionKey, ActionResult actionResult) {
    try {
      backplane.putActionResult(actionKey, actionResult);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  @Override
  public ListenableFuture<Iterable<Digest>> findMissingBlobs(Iterable<Digest> blobDigests, ExecutorService service) {
    Iterable<Digest> nonEmptyDigests = Iterables.filter(blobDigests, (digest) -> digest.getSizeBytes() > 0);
    if (Iterables.isEmpty(nonEmptyDigests)) {
      return Futures.immediateFuture(ImmutableList.of());
    }

    Deque<String> workers;
    try {
      List<String> workersList = new ArrayList<>(backplane.getWorkerSet());
      Collections.shuffle(workersList, rand);
      workers = new ArrayDeque(workersList);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }

    if (workers.isEmpty()) {
      return Futures.immediateFuture(nonEmptyDigests);
    }

    return findMissingBlobsOnWorker(nonEmptyDigests, workers, service);
  }

  private ListenableFuture<Iterable<Digest>> findMissingBlobsOnWorker(Iterable<Digest> blobDigests, Deque<String> workers, ExecutorService service) {
    String worker = workers.removeFirst();
    // FIXME use FluentFuture
    ListenableFuture<Iterable<Digest>> findMissingBlobsFuture = Futures.transformAsync(
        workerStub(worker).findMissingBlobs(blobDigests, service),
        (missingDigests) -> {
          if (Iterables.isEmpty(missingDigests) || workers.isEmpty()) {
            return Futures.immediateFuture(missingDigests);
          }
          return findMissingBlobsOnWorker(missingDigests, workers, service);
        },
        service);
    return Futures.catchingAsync(
        findMissingBlobsFuture,
        Throwable.class,
        (e) -> {
          Status status = Status.fromThrowable(e);
          if (status.getCode() == Code.UNAVAILABLE) {
            removeMalfunctioningWorker(worker, e, "findMissingBlobs(...)");
          } else if (status.getCode() == Code.CANCELLED || Context.current().isCancelled()) {
            // do nothing further if we're cancelled
            throw status.asRuntimeException();
          } else if (SHARD_IS_RETRIABLE.test(status)) {
            // why not, always
            workers.addLast(worker);
          } else {
            throw Status.INTERNAL.withCause(e).asRuntimeException();
          }

          if (workers.isEmpty()) {
            return Futures.immediateFuture(blobDigests);
          } else {
            return findMissingBlobsOnWorker(blobDigests, workers, service);
          }
        },
        service);
  }

  private void fetchBlobFromWorker(
      Digest blobDigest,
      Deque<String> workers,
      long offset,
      long limit,
      StreamObserver<ByteString> blobObserver) {
    String worker = workers.removeFirst();
    workerStub(worker).getBlob(blobDigest, offset, limit, new StreamObserver<ByteString>() {
      @Override
      public void onNext(ByteString nextChunk) {
        blobObserver.onNext(nextChunk);
      }

      @Override
      public void onError(Throwable t) {
        Status status = Status.fromThrowable(t);
        if (Context.current().isCancelled()) {
          blobObserver.onError(t);
          return;
        }
        if (status.getCode() == Code.UNAVAILABLE) {
          removeMalfunctioningWorker(worker, t, "getBlob(" + DigestUtil.toString(blobDigest) + ")");
        } else if (status.getCode() == Code.NOT_FOUND) {
          System.out.println(worker + " did not contain " + DigestUtil.toString(blobDigest));
          // ignore this, the worker will update the backplane eventually
        } else if (status.getCode() == Code.CANCELLED /* yes, gross */ || SHARD_IS_RETRIABLE.test(status)) {
          // why not, always
          workers.addLast(worker);
        } else {
          blobObserver.onError(t);
          return;
        }

        if (workers.isEmpty()) {
          blobObserver.onError(Status.NOT_FOUND.asException());
        } else {
          fetchBlobFromWorker(blobDigest, workers, offset, limit, blobObserver);
        }
      }

      @Override
      public void onCompleted() {
        blobObserver.onCompleted();
      }
    });
  }

  @Override
  public void getBlob(Digest blobDigest, long offset, long limit, StreamObserver<ByteString> blobObserver) {
    List<String> workersList;
    Set<String> workerSet;
    Set<String> locationSet;
    try {
      workerSet = backplane.getWorkerSet();
      locationSet = backplane.getBlobLocationSet(blobDigest);
      workersList = new ArrayList<>(Sets.intersection(locationSet, workerSet));
    } catch (IOException e) {
      blobObserver.onError(e);
      return;
    }
    boolean emptyWorkerList = workersList.isEmpty();
    final ListenableFuture<List<String>> populatedWorkerListFuture;
    if (emptyWorkerList) {
      try {
        populatedWorkerListFuture = Futures.transform(
            // should be sending this the blob location set and worker set we used
            correctMissingBlob(backplane, workerSet, locationSet, this::workerStub, blobDigest, newDirectExecutorService()),
            (foundOnWorkers) -> {
              Iterables.addAll(workersList, foundOnWorkers);
              return workersList;
            });
      } catch (IOException e) {
        blobObserver.onError(e);
        return;
      }
    } else {
      populatedWorkerListFuture = Futures.immediateFuture(workersList);
    }

    StreamObserver<ByteString> chunkObserver = new StreamObserver<ByteString>() {
      boolean triedCheck = emptyWorkerList;

      @Override
      public void onNext(ByteString nextChunk) {
        blobObserver.onNext(nextChunk);
      }

      @Override
      public void onError(Throwable t) {
        Status status = Status.fromThrowable(t);
        if (status.getCode() == Code.NOT_FOUND && !triedCheck) {
          triedCheck = true;
          workersList.clear();
          final ListenableFuture<List<String>> workersListFuture;
          try {
            workersListFuture = Futures.transform(
                correctMissingBlob(backplane, workerSet, locationSet, (worker) -> workerStub(worker), blobDigest, newDirectExecutorService()),
                (foundOnWorkers) -> {
                  Iterables.addAll(workersList, foundOnWorkers);
                  return workersList;
                });
          } catch (IOException e) {
            blobObserver.onError(e);
            return;
          }
          final StreamObserver<ByteString> checkedChunkObserver = this;
          Futures.addCallback(workersListFuture, new WorkersCallback(rand) {
            @Override
            public void onQueue(Deque<String> workers) {
              fetchBlobFromWorker(blobDigest, workers, offset, limit, checkedChunkObserver);
            }

            @Override
            public void onFailure(Throwable t) {
              blobObserver.onError(t);
            }
          });
        } else {
          blobObserver.onError(t);
        }
      }

      @Override
      public void onCompleted() {
        blobObserver.onCompleted();
      }
    };
    Futures.addCallback(populatedWorkerListFuture, new WorkersCallback(rand) {
      @Override
      public void onQueue(Deque<String> workers) {
        fetchBlobFromWorker(blobDigest, workers, offset, limit, chunkObserver);
      }

      @Override
      public void onFailure(Throwable t) {
        blobObserver.onError(t);
      }
    });
  }

  public abstract static class WorkersCallback implements FutureCallback<List<String>> {
    private final Random rand;

    public WorkersCallback(Random rand) {
      this.rand = rand;
    }

    @Override
    public void onSuccess(List<String> workersList) {
      if (workersList.isEmpty()) {
        onFailure(Status.NOT_FOUND.asException());
      } else {
        Collections.shuffle(workersList, rand);
        onQueue(new ArrayDeque<String>(workersList));
      }
    }

    protected abstract void onQueue(Deque<String> workers);
  }

  @Override
  public ByteString getBlob(Digest blobDigest, long offset, long limit) throws IOException, InterruptedException {
    SettableFuture<ByteString> blobFuture = SettableFuture.create();
    getBlob(blobDigest, offset, limit, new StreamObserver<ByteString>() {
      ByteString content = ByteString.EMPTY;

      @Override
      public void onNext(ByteString nextChunk) {
        content = content.concat(nextChunk);
      }

      @Override
      public void onError(Throwable t) {
        blobFuture.setException(t);
      }

      @Override
      public void onCompleted() {
        blobFuture.set(content);
      }
    });
    try {
      return blobFuture.get();
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
      throw new IOException(e.getCause());
    }
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
            Duration.ofMillis(/*options.experimentalRemoteRetryStartDelayMillis=*/ 100),
            Duration.ofMillis(/*options.experimentalRemoteRetryMaxDelayMillis=*/ 5000),
            /*options.experimentalRemoteRetryMultiplier=*/ 2,
            /*options.experimentalRemoteRetryJitter=*/ 0.1,
            /*options.experimentalRemoteRetryMaxAttempts=*/ 5),
        SHARD_IS_RETRIABLE);
  }

  private static ByteStreamUploader createStubUploader(Channel channel, ListeningScheduledExecutorService service) {
    return new ByteStreamUploader("", channel, null, 300, createStubRetrier(), service);
  }

  private synchronized Instance workerStub(String worker) {
    try {
      return workerStubs.get(worker);
    } catch (ExecutionException e) {
      e.printStackTrace();
      return null;
    }
  }

  void updateCaches(Digest digest, ByteString blob) throws InterruptedException {
    boolean known = false;
    try {
      commandCache.get(digest, new Callable<Command>() {
        @Override
        public Command call() throws IOException {
          return Command.parseFrom(blob);
        }
      }).get();
      known = true;
    } catch (ExecutionException e) {
      /* not a command */
    }
    if (!known) {
      try {
        directoryCache.get(digest, new Callable<Directory>() {
          @Override
          public Directory call() throws IOException {
            return Directory.parseFrom(blob);
          }
        }).get();
        known = true;
      } catch (ExecutionException e) {
        /* not a directory */
      }
    }
  }

  CommittingOutputStream createBlobOutputStream(Digest blobDigest) throws IOException {
    // the name might very well be how bytestream is supposed to resume uploads...
    Instance instance = workerStub(backplane.getRandomWorker());
    String resourceName = String.format(
        "uploads/%s/blobs/%s",
        UUID.randomUUID(), DigestUtil.toString(blobDigest));
    return instance.getStreamOutput(resourceName, blobDigest.getSizeBytes());
  }

  private class FailedChunkObserver implements ChunkObserver {
    ListenableFuture<Long> failedFuture;

    FailedChunkObserver(Throwable t) {
      failedFuture = Futures.immediateFailedFuture(t);
    }

    @Override
    public ListenableFuture<Long> getCommittedFuture() {
      return failedFuture;
    }

    @Override
    public long getCommittedSize() {
      return 0l;
    }

    @Override
    public void reset() {
    }

    @Override
    public void onNext(ByteString chunk) {
    }

    @Override
    public void onCompleted() {
    }

    @Override
    public void onError(Throwable t) {
      System.err.println("Received onError in failed chunkObserver");
      t.printStackTrace();
    }
  }

  public ChunkObserver getWriteBlobObserver(Digest blobDigest) {
    final CommittingOutputStream initialOutput;
    try {
      initialOutput = createBlobOutputStream(blobDigest);
    } catch (IOException e) {
      return new FailedChunkObserver(e);
    }

    ChunkObserver chunkObserver = new ChunkObserver() {
      long committedSize = 0l;
      ByteString smallBlob = ByteString.EMPTY;
      CommittingOutputStream out = initialOutput;

      @Override
      public long getCommittedSize() {
        return committedSize;
      }

      @Override
      public ListenableFuture<Long> getCommittedFuture() {
        return out.getCommittedFuture();
      }

      @Override
      public void reset() {
        try {
          out.close();
          out = createBlobOutputStream(blobDigest);
        } catch (IOException e) {
          throw Status.INTERNAL.withCause(e).asRuntimeException();
        }
        committedSize = 0;
        smallBlob = ByteString.EMPTY;
      }

      @Override
      public void onNext(ByteString chunk) {
        try {
          chunk.writeTo(out);
          committedSize += chunk.size();
          if (smallBlob != null && committedSize < 4 * 1024 * 1024) {
            smallBlob = smallBlob.concat(chunk);
          } else {
            smallBlob = null;
          }
        } catch (IOException e) {
          throw Status.INTERNAL.withCause(e).asRuntimeException();
        }
      }

      @Override
      public void onCompleted() {
        try {
          // need to cancel the request if size or digest doesn't match
          out.close();
        } catch (IOException e) {
          throw Status.INTERNAL.withCause(e).asRuntimeException();
        }

        try {
          if (smallBlob != null) {
            updateCaches(blobDigest, smallBlob);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw Status.CANCELLED.withCause(e).asRuntimeException();
        }
      }

      @Override
      public void onError(Throwable t) {
        try {
          out.close(); // yup.
        } catch (IOException e) {
          // ignore?
        }
      }
    };
    Context.current().addListener((context) -> {
      chunkObserver.onError(Status.CANCELLED.asRuntimeException());
    }, directExecutor());
    return chunkObserver;
  }

  protected int getTreeDefaultPageSize() { return 1024; }
  protected int getTreeMaxPageSize() { return 1024; }
  protected TokenizableIterator<DirectoryEntry> createTreeIterator(
      Digest rootDigest, String pageToken) throws IOException, InterruptedException {
    final Function<Digest, ListenableFuture<Directory>> getDirectoryFunction;
    Iterable<Directory> directories = backplane.getTree(rootDigest);
    if (directories != null) {
      // FIXME WE MUST VALIDATE THAT THIS DIRECTORIES TREE CONTAINS EVERY SINGLE DIGEST REQUESTED
      getDirectoryFunction = new Function<Digest, ListenableFuture<Directory>>() {
        Map<Digest, Directory> index = createDirectoriesIndex(directories);

        @Override
        public ListenableFuture<Directory> apply(Digest digest) {
          Directory directory = index.get(digest);
          if (directory == null) {
            throw new IllegalStateException(
                "not technically illegal, but a get for " + DigestUtil.toString(digest)
                + " was called on the cached tree entry for " + DigestUtil.toString(rootDigest)
                + ", this is probably an error");
          }
          return Futures.immediateFuture(directory);
        }
      };
    } else {
      getDirectoryFunction = (directoryBlobDigest) -> expectDirectory(directoryBlobDigest);
    }
    return new TreeIterator(getDirectoryFunction, rootDigest, pageToken);
  }

  @Override
  protected Action expectAction(Operation operation) throws InterruptedException {
    if (operation.getMetadata().is(QueuedOperationMetadata.class)) {
      try {
        QueuedOperationMetadata metadata = operation.getMetadata()
            .unpack(QueuedOperationMetadata.class);
        return metadata.getAction();
      } catch (InvalidProtocolBufferException e) {
        return null;
      }
    }
    return super.expectAction(operation);
  }

  interface Fetcher<T> {
    T fetch() throws InterruptedException;
  }

  @Override
  protected ListenableFuture<Directory> expectDirectory(Digest directoryBlobDigest) {
    final Fetcher<Directory> fetcher = () -> getUnchecked(super.expectDirectory(directoryBlobDigest));
    // is there a better interface to use for the cache with these nice futures?
    return directoryCache.get(directoryBlobDigest, new Callable<Directory>() {
      @Override
      public Directory call() throws InterruptedException {
        return fetcher.fetch();
      }
    });
  }

  @Override
  protected ListenableFuture<Command> expectCommand(Digest commandBlobDigest) {
    final Fetcher<Command> fetcher = () -> getUnchecked(super.expectCommand(commandBlobDigest));
    return Futures.catching(
        commandCache.get(commandBlobDigest, new Callable<Command>() {
          @Override
          public Command call() throws InterruptedException {
            return fetcher.fetch();
          }
        }),
        InvalidCacheLoadException.class,
        (e) -> { return null; });
  }

  private void removeMalfunctioningWorker(String worker, Throwable t, String context) {
    try {
      if (backplane.isWorker(worker)) {
        backplane.removeWorker(worker);

        System.out.println("Removed worker '" + worker + "' during(" + context + ") because of:");
        t.printStackTrace();
      }
    } catch (IOException eIO) {
      throw Status.fromThrowable(eIO).asRuntimeException();
    }

    workerStubs.invalidate(worker);
  }

  @Override
  public CommittingOutputStream getStreamOutput(String name, long expectedSize) {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream newStreamInput(String name, long offset) {
    throw new UnsupportedOperationException();
  }

  private ListenableFuture<QueuedOperationMetadata> buildQueuedOperationMetadata(
      Action action, RequestMetadata requestMetadata, Executor executor) {
    QueuedOperationMetadata.Builder queuedBuilder = QueuedOperationMetadata.newBuilder()
        .setAction(action)
        .setExecuteOperationMetadata(ExecuteOperationMetadata.newBuilder()
            .setActionDigest(digestUtil.compute(action))
            .build())
        .setRequestMetadata(requestMetadata);
    return transformQueuedOperationMetadata(action.getCommandDigest(), action.getInputRootDigest(), queuedBuilder, executor);
  }

  private ListenableFuture<QueuedOperationMetadata> transformQueuedOperationMetadata(
      Digest commandDigest,
      Digest inputRootDigest,
      QueuedOperationMetadata.Builder queuedBuilder,
      Executor executor) {
    return Futures.transform(
        Futures.allAsList(
            Futures.transform(insistCommand(commandDigest), queuedBuilder::setCommand, executor),
            Futures.transform(getTreeDirectories(inputRootDigest), queuedBuilder::addAllDirectories, executor)),
        (result) -> queuedBuilder.build());
  }

  protected Operation createOperation(ActionKey actionKey) { throw new UnsupportedOperationException(); }

  private Operation createOperation(Message metadata) {
    String name = createOperationName(UUID.randomUUID().toString());

    return Operation.newBuilder()
        .setName(name)
        .setMetadata(Any.pack(metadata))
        .build();
  }

  private ListenableFuture<Boolean> validateAndRequeueOperation(
      Operation operation, Action action) throws InterruptedException {
    final QueuedOperationMetadata metadata;

    boolean isQueuedOperationMetadata = operation.getMetadata().is(QueuedOperationMetadata.class);
    if (isQueuedOperationMetadata) {
      try {
        metadata = operation.getMetadata().unpack(QueuedOperationMetadata.class);
      } catch (InvalidProtocolBufferException e) {
        return Futures.immediateFailedFuture(e);
      }
    } else {
      RequestMetadata requestMetadata;
      if (operation.getMetadata().is(ExecutingOperationMetadata.class)) {
        try {
          // we should record where it was previously executing...
          requestMetadata = operation.getMetadata().unpack(ExecutingOperationMetadata.class).getRequestMetadata();
        } catch (InvalidProtocolBufferException e) {
          return Futures.immediateFailedFuture(e);
        }
      } else {
        // losing our attributability
        requestMetadata = RequestMetadata.getDefaultInstance();
      }
      try {
        metadata = buildQueuedOperationMetadata(action, requestMetadata, directExecutor()).get();
      } catch (ExecutionException e) {
        return Futures.immediateFailedFuture(e.getCause());
      }
    }

    SettableFuture<Boolean> requeuedFuture = SettableFuture.create();
    Futures.addCallback(
      validateQueuedOperationMetadata(metadata, operationTransformService),
      new FutureCallback<QueuedOperationMetadata>() {
        @Override
        public void onSuccess(QueuedOperationMetadata metadata) {
          try {
            if (isQueuedOperationMetadata) {
              backplane.requeueDispatchedOperation(operation);
            } else {
              Operation queuedOperation = operation.toBuilder()
                  .setMetadata(Any.pack(metadata))
                  .build();
              backplane.putOperation(queuedOperation, Stage.QUEUED);
            }
            requeuedFuture.set(true);
          } catch (IOException e) {
            // we should probably be observing retriable backplane exceptions...
            onFailure(e);
          }
        }

        @Override
        public void onFailure(Throwable t) {
          t.printStackTrace();
          requeuedFuture.set(false);
        }
      },
      operationTransformService);
    return requeuedFuture;
  }

  private boolean skipCacheLookup(Operation operation) {
    if (!operation.getMetadata().is(QueuedOperationMetadata.class)) {
      return false;
    }
    try {
      return operation.getMetadata().unpack(QueuedOperationMetadata.class)
          .getSkipCacheLookup();
    } catch (InvalidProtocolBufferException e) {
      return false;
    }
  }

  // there's optimization to be had here - we can just use the prequeue to requeue this
  // just need to make it smart enough to see the construction already
  private boolean requeueOperation(String operationName) throws IOException, InterruptedException {
    Operation operation = getOperation(operationName);
    if (operation == null) {
      logger.info("Operation " + operationName + " no longer exists");
      backplane.deleteOperation(operationName); // signal watchers
      return false;
    }
    if (operation.getDone()) {
      logger.info("Operation " + operation.getName() + " has already completed");
      return false;
    }
    ExecuteOperationMetadata executeOperationMetadata = expectExecuteOperationMetadata(operation);
    if (executeOperationMetadata == null) {
      logger.info("Operation " + operation.getName() + " does not contain ExecuteOperationMetadata");
      return false;
    }

    ActionKey actionKey = DigestUtil.asActionKey(executeOperationMetadata.getActionDigest());
    ListenableFuture<Boolean> requeuedFuture;
    SettableFuture<Void> completeFuture = SettableFuture.create();
    if (!skipCacheLookup(operation) && checkCache(actionKey, operation, executeOperationMetadata, completeFuture)) {
      requeuedFuture = Futures.transform(completeFuture, (result) -> true);
    } else {
      Action action = expectAction(operation);
      if (action == null) {
        logger.info("Action for " + operation.getName() + " no longer exists");
        return false;
      }

      requeuedFuture = validateAndRequeueOperation(operation, action);
    }

    try {
      return requeuedFuture.get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new UncheckedExecutionException(e);
    }
  }

  @Override
  public ListenableFuture<Operation> execute(Action action, boolean skipCacheLookup, RequestMetadata requestMetadata) {
    try {
      if (backplane.canPrequeue()) {
        QueuedOperationMetadata metadata = QueuedOperationMetadata.newBuilder()
            .setAction(action)
            .setExecuteOperationMetadata(ExecuteOperationMetadata.newBuilder()
                .setActionDigest(digestUtil.compute(action)))
            .setSkipCacheLookup(skipCacheLookup)
            .setRequestMetadata(requestMetadata)
            .build();
        Operation operation = createOperation(metadata);
        backplane.putOperation(operation, Stage.UNKNOWN);
        return Futures.immediateFuture(stripOperation(operation));
      }
    } catch (IOException e) {
      return Futures.immediateFailedFuture(e);
    }
    return Futures.immediateFailedFuture(Status.RESOURCE_EXHAUSTED.withDescription("cannot currently queue for execution").asRuntimeException());
  }

  private void errorOperation(String operationName, Throwable t, SettableFuture<Void> errorFuture) {
    Status st = Status.fromThrowable(t);
    String description = st.getDescription();
    com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
        .setCode(st.getCode().value())
        .setMessage(description == null ? "" : st.getDescription())
        .build();
    operationDeletionService.execute(new Runnable() {
      // we must make all efforts to delete this thing
      int attempt = 1;

      @Override
      public void run() {
        try {
          errorOperation(operationName, status);
          errorFuture.setException(t);
        } catch (StatusRuntimeException e) {
          if (attempt % 100 == 0) {
            logger.severe(String.format("On attempt %d to cancel %s: %s", attempt, operationName, e.getLocalizedMessage()));
          }
          // hopefully as deferred execution...
          operationDeletionService.execute(this);
          attempt++;
        } catch (InterruptedException e) {
        }
      }
    });
  }

  private boolean checkCache(ActionKey actionKey, Operation operation, ExecuteOperationMetadata metadata, SettableFuture completeFuture) {
    metadata = metadata.toBuilder()
        .setStage(Stage.CACHE_CHECK)
        .build();
    try {
      backplane.putOperation(
          operation.toBuilder()
              .setMetadata(Any.pack(metadata))
              .build(),
          metadata.getStage());

      ActionResult actionResult = getActionResultFromBackplane(actionKey);
      if (actionResult == null) {
        return false;
      }

      metadata = metadata.toBuilder()
          .setStage(Stage.COMPLETED)
          .build();
      Operation completedOperation = operation.toBuilder()
          .setDone(true)
          .setResponse(Any.pack(ExecuteResponse.newBuilder()
              .setResult(actionResult)
              .setStatus(com.google.rpc.Status.newBuilder()
                  .setCode(Code.OK.value())
                  .build())
              .setCachedResult(true)
              .build()))
          .setMetadata(Any.pack(metadata))
          .build();
      backplane.putOperation(completedOperation, metadata.getStage());
      completeFuture.set(null);
    } catch (IOException e) {
      errorOperation(operation.getName(), e, completeFuture);
    }
    return true;
  }

  @VisibleForTesting
  public ListenableFuture<Void> queue(Operation operation) throws InterruptedException {
    final QueuedOperationMetadata prequeueMetadata;
    try {
      prequeueMetadata = operation.getMetadata()
          .unpack(QueuedOperationMetadata.class);
    } catch (InvalidProtocolBufferException e) {
      try {
        cancelOperation(operation.getName());
      } catch (IllegalStateException cancelEx) {
        cancelEx.printStackTrace();
      }
      return Futures.immediateFailedFuture(e);
    }

    ExecuteOperationMetadata metadata = prequeueMetadata.getExecuteOperationMetadata();
    ActionKey actionKey = DigestUtil.asActionKey(metadata.getActionDigest());

    SettableFuture<Void> queueFuture = SettableFuture.create();
    if (!prequeueMetadata.getSkipCacheLookup()
        && checkCache(actionKey, operation, metadata, queueFuture)) {
      return queueFuture;
    }

    Action action = prequeueMetadata.getAction();
    ListenableFuture<QueuedOperationMetadata> queuedFuture =
        transformQueuedOperationMetadata(
            action.getCommandDigest(),
            action.getInputRootDigest(),
            prequeueMetadata.toBuilder(),
            operationTransformService);
    ListenableFuture<QueuedOperationMetadata> validatedFuture = Futures.catching(
        Futures.transformAsync(
            queuedFuture,
            (queuedMetadata) -> validateQueuedOperationMetadata(queuedMetadata, operationTransformService),
            operationTransformService),
        IllegalStateException.class,
        // might be able to add proper details here
        (e) -> { throw Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asRuntimeException(); });
    ListenableFuture<Operation> operationFuture = Futures.transform(
        validatedFuture,
        (queuedMetadata) -> operation.toBuilder().setMetadata(Any.pack(queuedMetadata)).build(),
        operationTransformService);

    ListenableFuture<Long> actionCommittedFuture = Futures.transformAsync(validatedFuture, (queuedMetadata) -> {
      ByteString actionBlob = action.toByteString();
      try (CommittingOutputStream out = createBlobOutputStream(actionKey.getDigest())) {
        actionBlob.writeTo(out);
        return out.getCommittedFuture();
      }
    }, operationTransformService);

    // onQueue call?
    Futures.addCallback(
        // need to have for requeuing
        Futures.transformAsync(actionCommittedFuture, (committedSize) -> operationFuture, operationTransformService),
        new FutureCallback<Operation>() {
          @Override
          public void onSuccess(Operation operation) {
            // if we ever do contexts here, we will need to do the right thing and make it withCancellation
            try {
              backplane.putOperation(operation, Stage.QUEUED);
              queueFuture.set(null);
            } catch (IOException e) {
              onFailure(e.getCause());
            }
          }

          @Override
          public void onFailure(Throwable t) {
            errorOperation(operation.getName(), t, queueFuture);
          }
      }, operationTransformService);
    return queueFuture;
  }

  @Override
  public void match(Platform platform, MatchListener listener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean putOperation(Operation operation) throws InterruptedException {
    if (isErrored(operation)) {
      try {
        Action action = expectAction(operation);
        if (action != null) {
          backplane.removeTree(action.getInputRootDigest());
        }
        return backplane.putOperation(operation, Stage.COMPLETED);
      } catch (IOException e) {
        throw Status.fromThrowable(e).asRuntimeException();
      }
    }
    throw new UnsupportedOperationException();
  }

  protected boolean matchOperation(Operation operation) { throw new UnsupportedOperationException(); }
  protected void enqueueOperation(Operation operation) { throw new UnsupportedOperationException(); }
  protected Object operationLock(String operationName) { throw new UnsupportedOperationException(); }

  @Override
  public boolean pollOperation(String operationName, Stage stage) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected int getListOperationsDefaultPageSize() { return 1024; }

  @Override
  protected int getListOperationsMaxPageSize() { return 1024; }

  @Override
  protected TokenizableIterator<Operation> createOperationsIterator(String pageToken) {
    Iterator<Operation> iter;
    try {
      iter = Iterables.transform(
          backplane.getOperations(),
          (operationName) -> {
            try {
              return backplane.getOperation(operationName);
            } catch (IOException e) {
              throw Status.fromThrowable(e).asRuntimeException();
            }
          }).iterator();
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
    OperationIteratorToken token;
    if (!pageToken.isEmpty()) {
      try {
        token = OperationIteratorToken.parseFrom(
            BaseEncoding.base64().decode(pageToken));
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalArgumentException();
      }
      boolean paged = false;
      while (iter.hasNext() && !paged) {
        paged = iter.next().getName().equals(token.getOperationName());
      }
    } else {
      token = null;
    }
    return new TokenizableIterator<Operation>() {
      private OperationIteratorToken nextToken = token;

      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public Operation next() {
        Operation operation = iter.next();
        nextToken = OperationIteratorToken.newBuilder()
            .setOperationName(operation.getName())
            .build();
        return operation;
      }

      @Override
      public String toNextPageToken() {
        if (hasNext()) {
          return BaseEncoding.base64().encode(nextToken.toByteArray());
        }
        return "";
      }
    };
  }

  @Override
  public Operation getOperation(String name) {
    try {
      return backplane.getOperation(name);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  @Override
  public void deleteOperation(String name) {
    try {
      backplane.deleteOperation(name);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  @Override
  public boolean watchOperation(
      String operationName,
      boolean watchInitialState,
      Predicate<Operation> watcher) {
    if (watchInitialState) {
      Operation operation = getOperation(operationName);
      if (!watcher.test(operation)) {
        // watcher processed completed state
        return true;
      }
      if (operation == null || operation.getDone()) {
        // watcher did not process completed state
        return false;
      }
    }

    try {
      return backplane.watchOperation(operationName, watcher);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  private static Operation stripOperation(Operation operation) {
    return operation.toBuilder()
        .setMetadata(Any.pack(expectExecuteOperationMetadata(operation)))
        .build();
  }
}
