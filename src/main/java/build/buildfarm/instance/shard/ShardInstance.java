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
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.catching;
import static com.google.common.util.concurrent.Futures.catchingAsync;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.OutputFile;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata.Stage;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.ExecutionPolicy;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.ResultsCachePolicy;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.Poller;
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
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.OperationIteratorToken;
import build.buildfarm.v1test.ShardInstanceConfig;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.QueuedOperation;
import build.buildfarm.v1test.QueuedOperationMetadata;
import build.buildfarm.v1test.ProfiledQueuedOperationMetadata;
import build.buildfarm.v1test.ExecutingOperationMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
import com.google.rpc.PreconditionFailure;
import io.grpc.Channel;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.protobuf.StatusProto;
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
  private static final Logger logger = Logger.getLogger(ShardInstance.class.getName());

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
    try {
      instance.stop();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
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
          (queueEntry, statusBuilder) -> {
            try {
              return requeueOperation(queueEntry, statusBuilder);
            } catch (IOException e) {
              statusBuilder.mergeFrom(StatusProto.fromThrowable(e));
              logger.log(SEVERE, "error requeueing " + queueEntry.getExecuteEntry().getOperationName(), e);
              return false;
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              return true;
            }
          },
          (operationName, status) -> {
            try {
              Operation operation = getOperation(operationName);
              if (operation == null) {
                operation = Operation.newBuilder()
                  .setName(operationName)
                  .setMetadata(Any.pack(ExecuteOperationMetadata.getDefaultInstance()))
                  .build();
              }
              errorOperation(operation, status);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }));
    } else {
      dispatchedMonitor = null;
    }

    if (runOperationQueuer) {
      operationQueuer = new Thread(new Runnable() {
        Stopwatch stopwatch = Stopwatch.createUnstarted();

        void iterate() throws IOException, InterruptedException {
          ensureCanQueue(stopwatch); // wait for transition to canQueue state
          long canQueueUSecs = stopwatch.elapsed(MICROSECONDS);
          stopwatch.stop();
          ExecuteEntry executeEntry = backplane.deprequeueOperation();
          stopwatch.start();
          if (executeEntry == null) {
            logger.severe("OperationQueuer: Got null from deprequeue...");
            return;
          }
          // half the watcher expiry, need to expose this from backplane
          Poller poller = new Poller(Durations.fromSeconds(5));
          poller.resume(
              () -> {
                try {
                  backplane.queueing(executeEntry.getOperationName());
                } catch (IOException e) {
                  logger.log(SEVERE, format("error polling %s for queuing", executeEntry.getOperationName()), e);
                  // mostly ignore, we will be stopped at some point later
                }
                return true;
              },
              () -> {},
              Deadline.after(10, DAYS));
          queue(executeEntry, poller);
          long operationTransformDispatchUSecs = stopwatch.elapsed(MICROSECONDS) - canQueueUSecs;
          logger.info(
              format(
                  "OperationQueuer: Dispatched To Transform: %dus in canQueue, %dus in transform dispatch",
                  canQueueUSecs,
                  operationTransformDispatchUSecs));
        }

        @Override
        public void run() {
          logger.info("OperationQueuer: Running");
          try {
            for (;;) {
              stopwatch.start();
              try {
                iterate();
              } catch (IOException e) {
                // problems interacting with backplane
              } finally {
                stopwatch.reset();
              }
            }
          } catch (InterruptedException e) {
            // treat with exit
            operationQueuer = null;
            return;
          } catch (Throwable t) {
            logger.log(SEVERE, "error processing prequeue", t);
          } finally {
            logger.info("OperationQueuer: Exiting");
          }
          operationQueuer = null;
          try {
            stop();
          } catch (InterruptedException e) {
            logger.log(SEVERE, "OperationQueuer: interrupted while stopping", e);
          }
        }
      });
    } else {
      operationQueuer = null;
    }
  }

  private void ensureCanQueue(Stopwatch stopwatch) throws IOException, InterruptedException {
    while (!backplane.canQueue()) {
      stopwatch.stop();
      TimeUnit.MILLISECONDS.sleep(100);
      stopwatch.start();
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
  public void stop() throws InterruptedException {
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
    if (!operationDeletionService.awaitTermination(10, TimeUnit.SECONDS)) {
      logger.severe("Could not shut down operation deletion service, some operations may be zombies");
    }
    operationDeletionService.shutdownNow();
    if (!operationTransformService.awaitTermination(10, TimeUnit.SECONDS)) {
      logger.severe("Could not shut down operation transform service");
    }
    operationTransformService.shutdownNow();
    workerStubs.invalidateAll();
  }

  private ActionResult getActionResultFromBackplane(ActionKey actionKey)
      throws IOException {
    // logger.info("getActionResult: " + DigestUtil.toString(actionKey.getDigest()));
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
      return immediateFuture(ImmutableList.of());
    }

    Deque<String> workers;
    try {
      List<String> workersList = new ArrayList<>(backplane.getWorkers());
      Collections.shuffle(workersList, rand);
      workers = new ArrayDeque(workersList);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }

    if (workers.isEmpty()) {
      return immediateFuture(nonEmptyDigests);
    }

    return findMissingBlobsOnWorker(nonEmptyDigests, workers, service);
  }

  private ListenableFuture<Iterable<Digest>> findMissingBlobsOnWorker(Iterable<Digest> blobDigests, Deque<String> workers, ExecutorService service) {
    String worker = workers.removeFirst();
    // FIXME use FluentFuture
    ListenableFuture<Iterable<Digest>> findMissingBlobsFuture = transformAsync(
        workerStub(worker).findMissingBlobs(blobDigests, service),
        (missingDigests) -> {
          if (Iterables.isEmpty(missingDigests) || workers.isEmpty()) {
            return immediateFuture(missingDigests);
          }
          return findMissingBlobsOnWorker(missingDigests, workers, service);
        },
        service);
    return catchingAsync(
        findMissingBlobsFuture,
        Throwable.class,
        (e) -> {
          Status status = Status.fromThrowable(e);
          if (status.getCode() == Code.UNAVAILABLE || status.getCode() == Code.UNIMPLEMENTED) {
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
            return immediateFuture(blobDigests);
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
          logger.info(worker + " did not contain " + DigestUtil.toString(blobDigest));
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
      workerSet = backplane.getWorkers();
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
        populatedWorkerListFuture = transform(
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
      populatedWorkerListFuture = immediateFuture(workersList);
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
            workersListFuture = transform(
                correctMissingBlob(
                    backplane,
                    workerSet,
                    locationSet,
                    (worker) -> workerStub(worker),
                    blobDigest,
                    newDirectExecutorService()),
                (foundOnWorkers) -> {
                  Iterables.addAll(workersList, foundOnWorkers);
                  return workersList;
                });
          } catch (IOException e) {
            blobObserver.onError(e);
            return;
          }
          final StreamObserver<ByteString> checkedChunkObserver = this;
          addCallback(workersListFuture, new WorkersCallback(rand) {
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
    addCallback(populatedWorkerListFuture, new WorkersCallback(rand) {
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
      logger.log(SEVERE, "error getting worker stub for " + worker, e);
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
    String worker = null;
    while (worker == null) {
      Set<String> workers = backplane.getWorkers();
      if (workers.isEmpty()) {
        throw new IOException("no available workers");
      }
      int index = rand.nextInt(workers.size());
      // best case no allocation average n / 2 selection
      Iterator<String> iter = workers.iterator();
      while (iter.hasNext() && index-- >= 0) {
        worker = iter.next();
      }
    }
    Instance instance = workerStub(worker);
    String resourceName = format(
        "uploads/%s/blobs/%s",
        UUID.randomUUID(), DigestUtil.toString(blobDigest));
    return instance.getStreamOutput(resourceName, blobDigest.getSizeBytes());
  }

  private class FailedChunkObserver implements ChunkObserver {
    ListenableFuture<Long> failedFuture;

    FailedChunkObserver(Throwable t) {
      failedFuture = immediateFailedFuture(t);
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
      logger.log(SEVERE, "Received onError in failed chunkObserver: {}", t);
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
          return immediateFuture(directory);
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
        QueuedOperation queuedOperation = getUnchecked(
            expectQueuedOperation(metadata.getQueuedOperationDigest()));
        if (!queuedOperation.getAction().equals(Action.getDefaultInstance())) {
          return queuedOperation.getAction();
        }
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
    return catching(
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
      if (backplane.removeWorker(worker)) {
        logger.log(WARNING, "Removed worker '" + worker + "' during(" + context + ") because of: {}", t);
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

  private ListenableFuture<QueuedOperation> buildQueuedOperation(
      Action action, ExecutorService service) {
    QueuedOperation.Builder queuedOperationBuilder = QueuedOperation.newBuilder()
        .setAction(action);
    return transformQueuedOperation(
        action,
        action.getCommandDigest(),
        action.getInputRootDigest(),
        queuedOperationBuilder,
        service);
  }

  private QueuedOperationMetadata buildQueuedOperationMetadata(
      ExecuteOperationMetadata executeOperationMetadata,
      QueuedOperation queuedOperation) {
    return QueuedOperationMetadata.newBuilder()
        .setExecuteOperationMetadata(executeOperationMetadata.toBuilder()
            .setStage(Stage.QUEUED))
        .setQueuedOperationDigest(getDigestUtil().compute(queuedOperation))
        .build();
  }

  private ListenableFuture<QueuedOperation> transformQueuedOperation(
      Action action,
      Digest commandDigest,
      Digest inputRootDigest,
      QueuedOperation.Builder queuedOperationBuilder,
      ExecutorService service) {
    return transform(
        allAsList(
            transform(
                insistCommand(commandDigest),
                queuedOperationBuilder::setCommand,
                service),
            transform(
                getTreeDirectories(inputRootDigest, service),
                queuedOperationBuilder::addAllDirectories,
                service)),
        (result) -> queuedOperationBuilder.setAction(action).build(),
        service);
  }

  protected Operation createOperation(ActionKey actionKey) { throw new UnsupportedOperationException(); }

  private static final class QueuedOperationResult {
    public final QueueEntry entry;
    public final QueuedOperationMetadata metadata;

    QueuedOperationResult(
        QueueEntry entry,
        QueuedOperationMetadata metadata) {
      this.entry = entry;
      this.metadata = metadata;
    }
  }

  private ListenableFuture<QueuedOperationResult> uploadQueuedOperation(
      QueuedOperation queuedOperation,
      ExecuteEntry executeEntry,
      ExecutorService service) {
    ByteString queuedOperationBlob = queuedOperation.toByteString();
    Digest queuedOperationDigest = getDigestUtil().compute(queuedOperationBlob);
    QueuedOperationMetadata metadata = QueuedOperationMetadata.newBuilder()
        .setExecuteOperationMetadata(ExecuteOperationMetadata.newBuilder()
            .setActionDigest(executeEntry.getActionDigest())
            .setStdoutStreamName(executeEntry.getStdoutStreamName())
            .setStderrStreamName(executeEntry.getStderrStreamName())
            .setStage(Stage.QUEUED))
        .setQueuedOperationDigest(queuedOperationDigest)
        .build();
    QueueEntry entry = QueueEntry.newBuilder()
        .setExecuteEntry(executeEntry)
        .setQueuedOperationDigest(queuedOperationDigest)
        .build();
    try (CommittingOutputStream out = createBlobOutputStream(queuedOperationDigest)) {
      queuedOperationBlob.writeTo(out);
      return transform(
          out.getCommittedFuture(),
          (committedSize) -> new QueuedOperationResult(entry, metadata),
          service);
    } catch (Throwable t) {
      return immediateFailedFuture(t);
    }
  }

  private ListenableFuture<QueuedOperation> buildQueuedOperation(
      Digest actionDigest,
      ExecutorService service) {
    ListenableFuture<Action> actionFuture = expectAction(actionDigest);
    return transformAsync(
        actionFuture,
        (action) -> buildQueuedOperation(action, service),
        service);
  }


  private ListenableFuture<Boolean> validateAndRequeueOperation(
      QueueEntry queueEntry,
      com.google.rpc.Status.Builder status) {
    ExecuteEntry executeEntry = queueEntry.getExecuteEntry();
    ListenableFuture<QueuedOperation> fetchQueuedOperationFuture =
        expectQueuedOperation(queueEntry.getQueuedOperationDigest());
    ListenableFuture<QueuedOperation> queuedOperationFuture = catchingAsync(
        fetchQueuedOperationFuture,
        Throwable.class,
        (e) -> buildQueuedOperation(executeEntry.getActionDigest(), operationTransformService));
    PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
    ListenableFuture<QueuedOperation> validatedFuture = transformAsync(
        queuedOperationFuture,
        (queuedOperation) -> catching(
            validateQueuedOperationAndInputs(
                queuedOperation,
                preconditionFailure,
                operationTransformService),
            IllegalStateException.class,
            (e) -> {
              status.addDetails(Any.pack(preconditionFailure.build()));
              throw Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asRuntimeException();
            },
            operationTransformService),
        operationTransformService);

    // this little fork ensures that a successfully fetched QueuedOperation
    // will not be reuploaded
    ListenableFuture<QueuedOperationResult> uploadedFuture = transformAsync(
        validatedFuture,
        (queuedOperation) -> catchingAsync(
            transform(
                fetchQueuedOperationFuture,
                (fechedQueuedOperation) -> {
                  QueuedOperationMetadata metadata = QueuedOperationMetadata.newBuilder()
                      .setExecuteOperationMetadata(ExecuteOperationMetadata.newBuilder()
                          .setActionDigest(executeEntry.getActionDigest())
                          .setStdoutStreamName(executeEntry.getStdoutStreamName())
                          .setStderrStreamName(executeEntry.getStderrStreamName())
                          .setStage(Stage.QUEUED))
                      .setQueuedOperationDigest(queueEntry.getQueuedOperationDigest())
                      .build();
                  return new QueuedOperationResult(queueEntry, metadata);
                },
                operationTransformService),
            Throwable.class,
            (e) -> uploadQueuedOperation(queuedOperation, executeEntry, operationTransformService),
            operationTransformService));

    String operationName = executeEntry.getOperationName();
    SettableFuture<Boolean> requeuedFuture = SettableFuture.create();
    addCallback(
        uploadedFuture,
        new FutureCallback<QueuedOperationResult>() {
          @Override
          public void onSuccess(QueuedOperationResult result) {
            Operation queueOperation = Operation.newBuilder()
                .setName(operationName)
                .setMetadata(Any.pack(result.metadata))
                .build();
            try {
              backplane.queue(result.entry, queueOperation);
              requeuedFuture.set(true);
            } catch (IOException e) {
              onFailure(e);
            }
          }

          @Override
          public void onFailure(Throwable t) {
            logger.log(SEVERE, format("failed to requeue: %s", operationName), t);
            status.setCode(Status.fromThrowable(t).getCode().value());
            if (t.getMessage() != null) {
              status.setMessage(t.getMessage());
            }
            requeuedFuture.set(false);
          }
        },
        operationTransformService);
    return requeuedFuture;
  }

  // there's optimization to be had here - we can just use the prequeue to requeue this
  // just need to make it smart enough to see the construction already
  @VisibleForTesting
  public boolean requeueOperation(QueueEntry queueEntry, com.google.rpc.Status.Builder statusBuilder)
      throws IOException, InterruptedException {
    ExecuteEntry executeEntry = queueEntry.getExecuteEntry();
    String operationName = executeEntry.getOperationName();
    Operation operation = getOperation(operationName);
    if (operation == null) {
      logger.info("Operation " + operationName + " no longer exists");
      statusBuilder.setCode(com.google.rpc.Code.NOT_FOUND.getNumber());
      backplane.deleteOperation(operationName); // signal watchers
      return false;
    }
    if (operation.getDone()) {
      logger.info("Operation " + operation.getName() + " has already completed");
      return false;
    }

    ActionKey actionKey = DigestUtil.asActionKey(executeEntry.getActionDigest());
    ListenableFuture<Boolean> requeuedFuture;
    SettableFuture<Void> completeFuture = SettableFuture.create();
    if (!executeEntry.getSkipCacheLookup() && checkCache(actionKey, operation, completeFuture)) {
      requeuedFuture = transform(completeFuture, (result) -> true);
    } else {
      requeuedFuture = validateAndRequeueOperation(queueEntry, statusBuilder);
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
  public void execute(
      Digest actionDigest,
      boolean skipCacheLookup,
      ExecutionPolicy executionPolicy,
      ResultsCachePolicy resultsCachePolicy,
      RequestMetadata requestMetadata,
      Predicate<Operation> watcher) {
    try {
      if (!backplane.canPrequeue()) {
        throw Status.UNAVAILABLE.withDescription("Too many jobs pending").asRuntimeException();
      }

      String operationName = createOperationName(UUID.randomUUID().toString());

      String stdoutStreamName = operationName + "/streams/stdout";
      String stderrStreamName = operationName + "/streams/stderr";
      ExecuteEntry executeEntry = ExecuteEntry.newBuilder()
          .setOperationName(operationName)
          .setActionDigest(actionDigest)
          .setExecutionPolicy(executionPolicy)
          .setResultsCachePolicy(resultsCachePolicy)
          .setSkipCacheLookup(skipCacheLookup)
          .setRequestMetadata(requestMetadata)
          .setStdoutStreamName(stdoutStreamName)
          .setStderrStreamName(stderrStreamName)
          .build();
      ExecuteOperationMetadata metadata = ExecuteOperationMetadata.newBuilder()
          .setActionDigest(actionDigest)
          .setStdoutStreamName(stdoutStreamName)
          .setStderrStreamName(stderrStreamName)
          .build();
      Operation operation = Operation.newBuilder()
          .setName(operationName)
          .setMetadata(Any.pack(metadata))
          .build();
      backplane.prequeue(executeEntry, operation);
      watchOperation(operation.getName(), watcher);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  private void errorOperation(Operation operation, Throwable t, com.google.rpc.Status.Builder status, SettableFuture<Void> errorFuture) {
    status.setCode(Status.fromThrowable(t).getCode().value());
    if (t.getMessage() != null) {
      status.setMessage(t.getMessage());
    }
    operationDeletionService.execute(new Runnable() {
      // we must make all efforts to delete this thing
      int attempt = 1;

      @Override
      public void run() {
        try {
          errorOperation(operation, status.build());
          errorFuture.setException(t);
        } catch (StatusRuntimeException e) {
          if (attempt % 100 == 0) {
            logger.log(
                SEVERE,
                format(
                    "On attempt %d to cancel %s: %s",
                    attempt,
                    operation.getName(),
                    e.getLocalizedMessage()),
                e);
          }
          // hopefully as deferred execution...
          operationDeletionService.execute(this);
          attempt++;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    });
  }

  private boolean checkCache(ActionKey actionKey, Operation operation, SettableFuture completeFuture) {
    ExecuteOperationMetadata metadata =
        ExecuteOperationMetadata.newBuilder()
            .setActionDigest(actionKey.getDigest())
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
      errorOperation(operation, e, com.google.rpc.Status.newBuilder(), completeFuture);
    }
    return true;
  }

  @VisibleForTesting
  public ListenableFuture<Void> queue(
      ExecuteEntry executeEntry,
      Poller poller) throws InterruptedException {
    ExecuteOperationMetadata metadata = ExecuteOperationMetadata.newBuilder()
        .setActionDigest(executeEntry.getActionDigest())
        .setStdoutStreamName(executeEntry.getStdoutStreamName())
        .setStderrStreamName(executeEntry.getStderrStreamName())
        .build();
    Operation operation = Operation.newBuilder()
        .setName(executeEntry.getOperationName())
        .setMetadata(Any.pack(metadata))
        .build();
    Digest actionDigest = executeEntry.getActionDigest();
    ActionKey actionKey = DigestUtil.asActionKey(actionDigest);

    // FIXME make this async and trigger the early return
    Stopwatch stopwatch = Stopwatch.createStarted();
    SettableFuture<Void> queueFuture = SettableFuture.create();
    if (!executeEntry.getSkipCacheLookup()
        && checkCache(actionKey, operation, queueFuture)) {
      // a little out of order, should do this before delivering operation
      poller.pause();
      long checkCacheUSecs = stopwatch.elapsed(MICROSECONDS);
      logger.info(
          format(
              "ShardInstance(%s): checkCache(%s): %dus elapsed",
              getName(),
              operation.getName(),
              checkCacheUSecs));
      return queueFuture;
    }
    long checkCacheUSecs = stopwatch.elapsed(MICROSECONDS);

    com.google.rpc.Status.Builder status = com.google.rpc.Status.newBuilder();
    PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
    long startTransformUSecs = stopwatch.elapsed(MICROSECONDS);
    ListenableFuture<Action> actionFuture = catching(
        expectAction(actionDigest),
        StatusException.class,
        (e) -> {
          Status st = Status.fromThrowable(e);
          if (st.getCode() == Code.NOT_FOUND) {
            preconditionFailure.addViolationsBuilder()
                .setType(VIOLATION_TYPE_MISSING)
                .setSubject(MISSING_INPUT)
                .setDescription("Action " + DigestUtil.toString(actionDigest));
            status.addDetails(Any.pack(preconditionFailure.build()));
            throw Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asRuntimeException();
          }
          throw st.asRuntimeException();
        });
    QueuedOperation.Builder queuedOperationBuilder = QueuedOperation.newBuilder();
    ListenableFuture<ProfiledQueuedOperationMetadata.Builder> queuedFuture = transformAsync(
        actionFuture,
        (action) -> {
          Stopwatch transformStopwatch = Stopwatch.createStarted();
          return transform(
              transformQueuedOperation(
                  action,
                  action.getCommandDigest(),
                  action.getInputRootDigest(),
                  queuedOperationBuilder,
                  operationTransformService),
              (queuedOperation) -> ProfiledQueuedOperationMetadata.newBuilder()
                  .setQueuedOperation(queuedOperation)
                  .setQueuedOperationMetadata(buildQueuedOperationMetadata(
                      metadata,
                      queuedOperation))
                  .setTransformedIn(Durations.fromMicros(transformStopwatch.elapsed(MICROSECONDS))),
              operationTransformService);
        },
        operationTransformService);
    ListenableFuture<ProfiledQueuedOperationMetadata.Builder> validatedFuture = catching(
        transform(
            queuedFuture,
            (profiledQueuedMetadata) -> {
              long startValidateUSecs = stopwatch.elapsed(MICROSECONDS);
              validateQueuedOperation(profiledQueuedMetadata.getQueuedOperation(), preconditionFailure);
              return profiledQueuedMetadata
                  .setValidatedIn(Durations.fromMicros(stopwatch.elapsed(MICROSECONDS) - startValidateUSecs));
            },
            operationTransformService),
        IllegalStateException.class,
        (e) -> {
          status.addDetails(Any.pack(preconditionFailure.build()));
          throw Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asRuntimeException();
        });
    ListenableFuture<ProfiledQueuedOperationMetadata> queuedOperationCommittedFuture = transformAsync(
				validatedFuture,
				(profiledQueuedMetadata) -> {
					ByteString queuedOperationBlob = profiledQueuedMetadata.getQueuedOperation().toByteString();
          Digest queuedOperationDigest = profiledQueuedMetadata
              .getQueuedOperationMetadata()
              .getQueuedOperationDigest();
          long startUploadUSecs = stopwatch.elapsed(MICROSECONDS);
					try (CommittingOutputStream out = createBlobOutputStream(queuedOperationDigest)) {
						queuedOperationBlob.writeTo(out);
						ListenableFuture<Long> committedFuture = out.getCommittedFuture();
            return transform(
                committedFuture,
                (committedSize) -> profiledQueuedMetadata
                    .setUploadedIn(Durations.fromMicros(stopwatch.elapsed(MICROSECONDS) - startUploadUSecs))
                    .build(),
                operationTransformService);
					}
				},
				operationTransformService);

    // onQueue call?
    addCallback(
        queuedOperationCommittedFuture,
        new FutureCallback<ProfiledQueuedOperationMetadata>() {
          @Override
          public void onSuccess(ProfiledQueuedOperationMetadata profiledQueuedMetadata) {
            QueuedOperationMetadata queuedOperationMetadata =
                profiledQueuedMetadata.getQueuedOperationMetadata();
            Operation queueOperation = operation.toBuilder()
                .setMetadata(Any.pack(queuedOperationMetadata))
                .build();
            QueueEntry queueEntry = QueueEntry.newBuilder()
                .setExecuteEntry(executeEntry)
                .setQueuedOperationDigest(queuedOperationMetadata.getQueuedOperationDigest())
                .build();
            try {
              ensureCanQueue(stopwatch);
              long startQueueUSecs = stopwatch.elapsed(MICROSECONDS);
              poller.pause();
              backplane.queue(queueEntry, queueOperation);
              long elapsedUSecs = stopwatch.elapsed(MICROSECONDS);
              long queueUSecs = elapsedUSecs - startQueueUSecs;
              logger.info(
                  format(
                      "ShardInstance(%s): queue(%s): %dus checkCache, %dus transform, %dus validate, %dus upload, %dus queue, %dus elapsed",
                      getName(),
                      queueOperation.getName(),
                      checkCacheUSecs,
                      Durations.toMicros(profiledQueuedMetadata.getTransformedIn()),
                      Durations.toMicros(profiledQueuedMetadata.getValidatedIn()),
                      Durations.toMicros(profiledQueuedMetadata.getUploadedIn()),
                      queueUSecs,
                      elapsedUSecs));
              queueFuture.set(null);
            } catch (IOException e) {
              onFailure(e.getCause() == null ? e : e.getCause());
            } catch (InterruptedException e) {
              // ignore
            }
          }

          @Override
          public void onFailure(Throwable t) {
            poller.pause();
            errorOperation(operation, t, status, queueFuture);
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
      Predicate<Operation> watcher) {
    Operation operation = getOperation(operationName);
    if (!watcher.test(stripOperation(operation))) {
      // watcher processed completed state
      return true;
    }
    if (operation == null || operation.getDone()) {
      // watcher did not process completed state
      return false;
    }

    try {
      return backplane.watchOperation(operationName, watcher);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  private static Operation stripOperation(Operation operation) {
    ExecuteOperationMetadata metadata = expectExecuteOperationMetadata(operation);
    if (metadata == null) {
      metadata = ExecuteOperationMetadata.getDefaultInstance();
    }
    return operation.toBuilder()
        .setMetadata(Any.pack(metadata))
        .build();
  }

  @Override
  protected Logger getLogger() {
    return logger;
  }
}
