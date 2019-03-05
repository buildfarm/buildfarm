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
import static com.google.common.base.Preconditions.checkState;
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
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.logging.Level.INFO;
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
import build.buildfarm.common.TokenizableIterator;
import build.buildfarm.common.TreeIterator;
import build.buildfarm.common.TreeIterator.DirectoryEntry;
import build.buildfarm.common.Watcher;
import build.buildfarm.common.cache.Cache;
import build.buildfarm.common.cache.CacheBuilder;
import build.buildfarm.common.cache.CacheLoader.InvalidCacheLoadException;
import build.buildfarm.common.grpc.Retrier;
import build.buildfarm.common.grpc.Retrier.Backoff;
import build.buildfarm.common.grpc.RetryException;
import build.buildfarm.instance.AbstractServerInstance;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.ByteStreamUploader;
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
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Logger;
import javax.naming.ConfigurationException;

public class ShardInstance extends AbstractServerInstance {
  private static final Logger logger = Logger.getLogger(ShardInstance.class.getName());

  private static ListenableFuture<Void> IMMEDIATE_VOID_FUTURE = Futures.<Void>immediateFuture(null);

  private final Runnable onStop;
  private final ShardBackplane backplane;
  private final com.google.common.cache.LoadingCache<String, Instance> workerStubs;
  private final Thread dispatchedMonitor;
  private final Cache<Digest, Directory> directoryCache = CacheBuilder.newBuilder()
      .maximumSize(64 * 1024)
      .build();
  private final Cache<Digest, Command> commandCache = CacheBuilder.newBuilder()
      .maximumSize(64 * 1024)
      .build();
  private final Cache<Digest, Action> actionCache = CacheBuilder.newBuilder()
      .maximumSize(64 * 1024)
      .build();
  private final Random rand = new Random();

  private final ListeningExecutorService operationTransformService =
      listeningDecorator(newFixedThreadPool(24));

  private ExecutorService operationDeletionService = newSingleThreadExecutor();
  private Thread operationQueuer;
  private boolean stopping = false;
  private boolean stopped = true;

  public ShardInstance(String name, DigestUtil digestUtil, ShardInstanceConfig config, Runnable onStop)
      throws InterruptedException, ConfigurationException {
    this(
        name,
        digestUtil,
        getBackplane(config),
        config.getRunDispatchedMonitor(),
        config.getDispatchedMonitorIntervalSeconds(),
        config.getRunOperationQueuer(),
        onStop,
        WorkerStubs.create(digestUtil));
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
      int dispatchedMonitorIntervalSeconds,
      boolean runOperationQueuer,
      Runnable onStop,
      com.google.common.cache.LoadingCache<String, Instance> workerStubs)
      throws InterruptedException {
    super(name, digestUtil, null, null, null, null, null);
    this.backplane = backplane;
    this.workerStubs = workerStubs;
    this.onStop = onStop;
    backplane.setOnUnsubscribe(this::stop);

    if (runDispatchedMonitor) {
      dispatchedMonitor = new Thread(new DispatchedMonitor(
          backplane,
          this::requeueOperation,
          dispatchedMonitorIntervalSeconds));
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
          String operationName = executeEntry.getOperationName();
          poller.resume(
              () -> {
                try {
                  backplane.queueing(executeEntry.getOperationName());
                } catch (IOException e) {
                  if (!stopping && !stopped) {
                    logger.log(SEVERE, format("error polling %s for queuing", operationName), e);
                  }
                  // mostly ignore, we will be stopped at some point later
                }
                return !stopping && !stopped;
              },
              () -> {},
              Deadline.after(5, MINUTES));
          try {
            logger.info("queueing " + operationName);
            addCallback(
                queue(executeEntry, poller),
                new FutureCallback<Void>() {
                  @Override
                  public void onSuccess(Void result) {
                    logger.info("successfully queued " + operationName);
                    // nothing
                  }

                  @Override
                  public void onFailure(Throwable t) {
                    logger.log(SEVERE, "error queueing " + operationName, t);
                  }
                },
                operationTransformService);
            long operationTransformDispatchUSecs = stopwatch.elapsed(MICROSECONDS) - canQueueUSecs;
            logger.info(
                format(
                    "OperationQueuer: Dispatched To Transform %s: %dus in canQueue, %dus in transform dispatch",
                    operationName,
                    canQueueUSecs,
                    operationTransformDispatchUSecs));
          } catch (Throwable t) {
            poller.pause();
            logger.log(SEVERE, "error queueing " + operationName, t);
          }
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
          } catch (Exception t) {
            logger.log(SEVERE, "OperationQueuer: fatal exception during iteration", t);
          } finally {
            logger.info("OperationQueuer: Exiting");
          }
          operationQueuer = null;
          try {
            stop();
          } catch (InterruptedException e) {
            logger.log(SEVERE, "interrupted while stopping instance " + getName(), e);
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
    stopped = false;
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
    if (stopped) {
      return;
    }
    stopping = true;
    logger.fine(format("Instance %s is stopping", getName()));
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
    logger.fine(format("Instance %s has been stopped", getName()));
    stopping = false;
    stopped = true;
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
    if (actionResult.hasStdoutDigest()) {
      outputDigests = Iterables.concat(outputDigests, ImmutableList.of(actionResult.getStdoutDigest()));
    }
    if (actionResult.hasStderrDigest()) {
      outputDigests = Iterables.concat(outputDigests, ImmutableList.of(actionResult.getStderrDigest()));
    }
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
          } else if (status.getCode() == Code.CANCELLED
              || Context.current().isCancelled()
              || !SHARD_IS_RETRIABLE.test(status)) {
            // do nothing further if we're cancelled
            throw status.asRuntimeException();
          } else {
            // why not, always
            workers.addLast(worker);
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
      long received = 0;

      @Override
      public void onNext(ByteString nextChunk) {
        blobObserver.onNext(nextChunk);
        received += nextChunk.size();
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
          long nextLimit;
          if (limit == 0) {
            nextLimit = 0;
          } else {
            checkState(limit >= received);
            nextLimit = limit - received;
          }
          if (nextLimit == 0 && limit != 0) {
            // be gracious and terminate the blobObserver here
            onCompleted();
          } else {
            fetchBlobFromWorker(blobDigest, workers, offset + received, nextLimit, blobObserver);
          }
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
                    ShardInstance.this::workerStub,
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
  public ByteString getBlob(Digest blobDigest, long offset, long limit) throws InterruptedException {
    SettableFuture<ByteString> blobFuture = SettableFuture.create();
    getBlob(blobDigest, offset, limit, new StreamObserver<ByteString>() {
      ByteString content = ByteString.EMPTY;

      @Override
      public void onNext(ByteString nextChunk) {
        content = content.concat(nextChunk);
      }

      @Override
      public void onError(Throwable t) {
        Status status = Status.fromThrowable(t);
        if (status.getCode() == Code.NOT_FOUND) {
          blobFuture.set(null);
        } else {
          blobFuture.setException(t);
        }
      }

      @Override
      public void onCompleted() {
        blobFuture.set(content);
      }
    });
    try {
      return blobFuture.get();
    } catch (ExecutionException e) {
      logger.log(SEVERE, "error fetching blob", e);
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      return null;
    }
  }

  private Instance workerStub(String worker) {
    try {
      return workerStubs.get(worker);
    } catch (ExecutionException e) {
      logger.log(SEVERE, "error getting worker stub for " + worker, e);
      throw new IllegalStateException("stub instance creation must not fail");
    }
  }

  void updateCaches(Digest digest, ByteString blob) throws InterruptedException {
    try {
      commandCache.get(digest, new Callable<ListenableFuture<? extends Command>>() {
        @Override
        public ListenableFuture<Command> call() throws IOException {
          return immediateFuture(Command.parseFrom(blob));
        }
      }).get();
    } catch (ExecutionException e) {
      /* not a command */
    }

    try {
      actionCache.get(digest, new Callable<ListenableFuture<? extends Action>>() {
        @Override
        public ListenableFuture<Action> call() throws IOException {
          return immediateFuture(Action.parseFrom(blob));
        }
      }).get();
    } catch (ExecutionException e) {
      /* not an action */
    }

    try {
      directoryCache.get(digest, new Callable<ListenableFuture<? extends Directory>>() {
        @Override
        public ListenableFuture<Directory> call() throws IOException {
          return immediateFuture(Directory.parseFrom(blob));
        }
      }).get();
    } catch (ExecutionException e) {
      /* not a directory */
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
    String resourceName = String.format(
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
      logger.log(SEVERE, "Received error in failed chunkObserver", t);
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
        } catch (Throwable t) {
          throw Status.fromThrowable(t).asRuntimeException();
        }
      }

      @Override
      public void onCompleted() {
        try {
          // need to cancel the request if size or digest doesn't match
          out.close();
        } catch (IOException e) {
          throw Status.fromThrowable(e).asRuntimeException();
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
    return chunkObserver;
  }

  protected int getTreeDefaultPageSize() { return 1024; }
  protected int getTreeMaxPageSize() { return 1024; }
  protected TokenizableIterator<DirectoryEntry> createTreeIterator(
      String reason, Digest rootDigest, String pageToken) {
    return new TreeIterator(
        (directoryBlobDigest) -> catching(
            expectDirectory(reason, directoryBlobDigest, newDirectExecutorService()),
            Throwable.class,
            (t) -> {
              logger.log(
                  SEVERE,
                  format(
                      "transformQueuedOperation(%s): error fetching directory %s",
                      reason,
                      DigestUtil.toString(directoryBlobDigest)),
                  t);
              return null;
            },
            directExecutor()),
        rootDigest,
        pageToken);
  }

  @Override
  protected Action expectAction(Operation operation) throws InterruptedException {
    if (operation.getMetadata().is(QueuedOperationMetadata.class)) {
      try {
        QueuedOperationMetadata metadata = operation.getMetadata()
            .unpack(QueuedOperationMetadata.class);
        QueuedOperation queuedOperation = getUnchecked(
            expect(metadata.getQueuedOperationDigest(), QueuedOperation.parser(), directExecutor()));
        if (queuedOperation.hasAction()) {
          return queuedOperation.getAction();
        }
      } catch (InvalidProtocolBufferException e) {
        logger.log(SEVERE, "invalid queued operation format", e);
        return null;
      }
    }
    return super.expectAction(operation);
  }

  private static <V> ListenableFuture<V> notFoundNull(ListenableFuture<V> value) {
    return catchingAsync(
        value,
        Throwable.class,
        (t) -> {
          Status status = Status.fromThrowable(t);
          if (status.getCode() == Code.NOT_FOUND) {
            return immediateFuture(null);
          }
          return immediateFailedFuture(t);
        },
        directExecutor());
  }

  ListenableFuture<Directory> expectDirectory(String reason, Digest directoryBlobDigest, Executor executor) {
    if (directoryBlobDigest.getSizeBytes() == 0) {
      return immediateFuture(Directory.getDefaultInstance());
    }
    Supplier<ListenableFuture<Directory>> fetcher = () -> notFoundNull(expect(directoryBlobDigest, Directory.parser(), executor));
    // is there a better interface to use for the cache with these nice futures?
    return directoryCache.get(directoryBlobDigest, new Callable<ListenableFuture<? extends Directory>>() {
      @Override
      public ListenableFuture<Directory> call() {
        logger.info(
            format(
                "transformQueuedOperation(%s): fetching directory %s",
                reason,
                DigestUtil.toString(directoryBlobDigest)));
        return fetcher.get();
      }
    });
  }

  ListenableFuture<Command> expectCommand(Digest commandBlobDigest, Executor executor) {
    Supplier<ListenableFuture<Command>> fetcher = () -> notFoundNull(expect(commandBlobDigest, Command.parser(), executor));
    return catching(
        commandCache.get(commandBlobDigest, new Callable<ListenableFuture<? extends Command>>() {
          @Override
          public ListenableFuture<Command> call() {
            return fetcher.get();
          }
        }),
        InvalidCacheLoadException.class,
        (e) -> { return null; });
  }

  ListenableFuture<Action> expectAction(Digest actionBlobDigest, Executor executor) {
    Supplier<ListenableFuture<Action>> fetcher = () -> notFoundNull(expect(actionBlobDigest, Action.parser(), executor));
    return catching(
        actionCache.get(actionBlobDigest, new Callable<ListenableFuture<? extends Action>>() {
          @Override
          public ListenableFuture<Action> call() {
            return fetcher.get();
          }
        }),
        InvalidCacheLoadException.class,
        (e) -> { return null; });
  }

  private void removeMalfunctioningWorker(String worker, Throwable t, String context) {
    try {
      if (backplane.removeWorker(worker)) {
        logger.log(WARNING, format("Removed worker '%s' during(%s) due to exception", worker, context), t);
      }
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
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
      String operationName, Action action, ExecutorService service) {
    QueuedOperation.Builder queuedOperationBuilder = QueuedOperation.newBuilder()
        .setAction(action);
    return transformQueuedOperation(
        operationName,
        action,
        action.getCommandDigest(),
        action.getInputRootDigest(),
        queuedOperationBuilder,
        service);
  }

  private QueuedOperationMetadata buildQueuedOperationMetadata(
      ExecuteOperationMetadata executeOperationMetadata,
      RequestMetadata requestMetadata,
      QueuedOperation queuedOperation) {
    return QueuedOperationMetadata.newBuilder()
        .setExecuteOperationMetadata(executeOperationMetadata.toBuilder()
            .setStage(Stage.QUEUED))
        .setRequestMetadata(requestMetadata)
        .setQueuedOperationDigest(getDigestUtil().compute(queuedOperation))
        .build();
  }

  private ListenableFuture<QueuedOperation> transformQueuedOperation(
      String operationName,
      Action action,
      Digest commandDigest,
      Digest inputRootDigest,
      QueuedOperation.Builder queuedOperationBuilder,
      ExecutorService service) {
    return transform(
        allAsList(
            transform(
                expectCommand(commandDigest, service),
                (command) -> {
                  logger.info(format("transformQueuedOperation(%s): fetched command", operationName));
                  if (command != null) {
                    queuedOperationBuilder.setCommand(command);
                  }
                  return queuedOperationBuilder;
                },
                service),
            transform(
                getTreeDirectories(operationName, inputRootDigest, service),
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
    return transform(
        writeBlobFuture(queuedOperationDigest, queuedOperationBlob),
        (committedSize) -> new QueuedOperationResult(entry, metadata),
        service);
  }

  private ListenableFuture<Long> writeBlobFuture(Digest digest, ByteString content) {
    checkState(digest.getSizeBytes() == content.size());
    int attempts = 5;
    for (;;) {
      try (CommittingOutputStream out = createBlobOutputStream(digest)) {
        content.writeTo(out);
        return transformAsync(
            out.getCommittedFuture(),
            (committedSize) -> {
              if (committedSize != content.size()) {
                throw new IOException(
                    String.format(
                        "committedSize %d did not match expected size for %s",
                        committedSize,
                        DigestUtil.toString(digest)));
              }
              return immediateFuture(committedSize);
            });
      } catch (IOException e) {
        return immediateFailedFuture(e);
      } catch (Throwable t) {
        Status status = Status.fromThrowable(t);
        if (--attempts == 0 ||
            !Retrier.DEFAULT_IS_RETRIABLE.test(status)) {
          return immediateFailedFuture(t);
        } else {
          logger.log(INFO, format("failed to output %s, retrying", digest), t);
        }
      }
    }
  }

  private ListenableFuture<QueuedOperation> buildQueuedOperation(
      String operationName,
      Digest actionDigest,
      ExecutorService service) {
    return transformAsync(
        expectAction(actionDigest, service),
        (action) -> {
          if (action == null) {
            return immediateFuture(QueuedOperation.getDefaultInstance());
          }
          return buildQueuedOperation(operationName, action, service);
        },
        service);
  }


  private ListenableFuture<Void> validateAndRequeueOperation(
      Operation operation,
      QueueEntry queueEntry) {
    ExecuteEntry executeEntry = queueEntry.getExecuteEntry();
    String operationName = executeEntry.getOperationName();
    checkState(operationName.equals(operation.getName()));
    ListenableFuture<QueuedOperation> fetchQueuedOperationFuture =
        expect(queueEntry.getQueuedOperationDigest(), QueuedOperation.parser(), operationTransformService);
    Digest actionDigest = executeEntry.getActionDigest();
    ListenableFuture<QueuedOperation> queuedOperationFuture = catchingAsync(
        fetchQueuedOperationFuture,
        Throwable.class,
        (e) -> buildQueuedOperation(operation.getName(), actionDigest, operationTransformService));
    PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
    ListenableFuture<QueuedOperation> validatedFuture = transformAsync(
        queuedOperationFuture,
        (queuedOperation) -> {
          /* sync, throws StatusException - must be serviced via non-OTS */
          validateQueuedOperationAndInputs(
              actionDigest,
              queuedOperation,
              preconditionFailure,
              newDirectExecutorService());
          return immediateFuture(queuedOperation);
        },
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
                      .setRequestMetadata(executeEntry.getRequestMetadata())
                      .build();
                  return new QueuedOperationResult(queueEntry, metadata);
                },
                operationTransformService),
            Throwable.class,
            (e) -> uploadQueuedOperation(queuedOperation, executeEntry, operationTransformService),
            operationTransformService));

    SettableFuture<Void> requeuedFuture = SettableFuture.create();
    addCallback(
        uploadedFuture,
        new FutureCallback<QueuedOperationResult>() {
          @Override
          public void onSuccess(QueuedOperationResult result) {
            Operation queueOperation = operation.toBuilder()
                .setMetadata(Any.pack(result.metadata))
                .build();
            try {
              backplane.queue(result.entry, queueOperation);
              requeuedFuture.set(null);
            } catch (IOException e) {
              onFailure(e);
            }
          }

          @Override
          public void onFailure(Throwable t) {
            logger.log(SEVERE, "failed to requeue: " + operationName, t);
            com.google.rpc.Status status = StatusProto.fromThrowable(t);
            if (status == null) {
              logger.log(SEVERE, "no rpc status from exception for " + operationName, t);
              status = com.google.rpc.Status.newBuilder()
                  .setCode(Status.fromThrowable(t).getCode().value())
                  .build();
            }
            logFailedStatus(actionDigest, status);
            SettableFuture<Void> errorFuture = SettableFuture.create();
            errorOperationFuture(operation, status, errorFuture);
            errorFuture.addListener(
                () -> requeuedFuture.set(null),
                operationTransformService);
          }
        },
        operationTransformService);
    return requeuedFuture;
  }

  @VisibleForTesting
  public ListenableFuture<Void> requeueOperation(QueueEntry queueEntry) {
    ExecuteEntry executeEntry = queueEntry.getExecuteEntry();
    String operationName = executeEntry.getOperationName();
    Operation operation;
    try {
      operation = getOperation(operationName);
      if (operation == null) {
        logger.info("Operation " + operationName + " no longer exists");
        backplane.deleteOperation(operationName); // signal watchers
        return IMMEDIATE_VOID_FUTURE;
      }
    } catch (IOException|StatusRuntimeException e) {
      return immediateFailedFuture(e);
    }
    if (operation.getDone()) {
      logger.info("Operation " + operation.getName() + " has already completed");
      try {
        backplane.completeOperation(operationName);
      } catch (IOException e) {
        return immediateFailedFuture(e);
      }
      return IMMEDIATE_VOID_FUTURE;
    }

    ActionKey actionKey = DigestUtil.asActionKey(executeEntry.getActionDigest());
    ListenableFuture<Boolean> cachedResultFuture;
    if (executeEntry.getSkipCacheLookup()) {
      cachedResultFuture = immediateFuture(false);
    } else {
      cachedResultFuture = checkCacheFuture(actionKey, operation);
    }
    return transformAsync(
        cachedResultFuture,
        (cachedResult) -> {
          if (cachedResult) {
            return IMMEDIATE_VOID_FUTURE;
          }
          return validateAndRequeueOperation(operation, queueEntry);
        },
        operationTransformService);
  }

  @Override
  public ListenableFuture<Void> execute(
      Digest actionDigest,
      boolean skipCacheLookup,
      ExecutionPolicy executionPolicy,
      ResultsCachePolicy resultsCachePolicy,
      RequestMetadata requestMetadata,
      Watcher watcher) {
    try {
      if (!backplane.canPrequeue()) {
        return immediateFailedFuture(Status.UNAVAILABLE.withDescription("Too many jobs pending").asException());
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
      return watchOperation(operation.getName(), watcher);
    } catch (IOException e) {
      return immediateFailedFuture(e);
    }
  }

  private <T> void errorOperationFuture(Operation operation, com.google.rpc.Status status, SettableFuture<T> errorFuture) {
    operationDeletionService.execute(new Runnable() {
      // we must make all efforts to delete this thing
      int attempt = 1;

      @Override
      public void run() {
        try {
          errorOperation(operation, status);
          errorFuture.setException(StatusProto.toStatusException(status));
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

  private boolean checkCache(ActionKey actionKey, Operation operation) throws IOException {
    ExecuteOperationMetadata metadata =
        ExecuteOperationMetadata.newBuilder()
            .setActionDigest(actionKey.getDigest())
            .setStage(Stage.CACHE_CHECK)
            .build();
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
    return true;
  }

  private ListenableFuture<Boolean> checkCacheFuture(ActionKey actionKey, Operation operation) {
    ListenableFuture<Boolean> checkCacheFuture = operationTransformService.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return checkCache(actionKey, operation);
      }
    });
    return catchingAsync(
        checkCacheFuture,
        Throwable.class,
        (e) -> {
          logger.log(SEVERE, "error checking cache for " + operation.getName(), e);
          return immediateFuture(false);
        },
        operationTransformService);
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
    ListenableFuture<Boolean> cachedResultFuture;
    if (executeEntry.getSkipCacheLookup()) {
      cachedResultFuture = immediateFuture(false);
    } else {
      cachedResultFuture = checkCacheFuture(actionKey, operation);
    }
    return transformAsync(
        cachedResultFuture,
        (cachedResult) -> {
          if (cachedResult) {
            poller.pause();
            long checkCacheUSecs = stopwatch.elapsed(MICROSECONDS);
            logger.info(
                format(
                    "ShardInstance(%s): checkCache(%s): %sus elapsed",
                    getName(),
                    operation.getName(),
                    checkCacheUSecs));
            return IMMEDIATE_VOID_FUTURE;
          }
          return transformAndQueue(executeEntry, poller, operation, stopwatch);
        },
        operationTransformService);
  }

  private ListenableFuture<Void> transformAndQueue(
      ExecuteEntry executeEntry,
      Poller poller,
      Operation operation,
      Stopwatch stopwatch) {
    long checkCacheUSecs = stopwatch.elapsed(MICROSECONDS);
    ExecuteOperationMetadata metadata;
    try {
      metadata = operation.getMetadata().unpack(ExecuteOperationMetadata.class);
    } catch (InvalidProtocolBufferException e) {
      return immediateFailedFuture(e);
    }
    Digest actionDigest = metadata.getActionDigest();
    SettableFuture<Void> queueFuture = SettableFuture.create();
    long startTransformUSecs = stopwatch.elapsed(MICROSECONDS);
    logger.info(
        format(
            "ShardInstance(%s): queue(%s): fetching action %s",
            getName(),
            operation.getName(),
            actionDigest.getHash()));
    ListenableFuture<Action> actionFuture = catchingAsync(
        transformAsync(
            expectAction(actionDigest, operationTransformService),
            (action) -> {
              if (action == null) {
                throw Status.NOT_FOUND.asException();
              }
              return immediateFuture(action);
            },
            operationTransformService),
        StatusException.class,
        (e) -> {
          Status st = Status.fromThrowable(e);
          if (st.getCode() == Code.NOT_FOUND) {
            PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
            preconditionFailure.addViolationsBuilder()
                .setType(VIOLATION_TYPE_MISSING)
                .setSubject("blobs/" + DigestUtil.toString(actionDigest))
                .setDescription(MISSING_ACTION);
            checkPreconditionFailure(actionDigest, preconditionFailure.build());
          }
          throw st.asRuntimeException();
        },
        operationTransformService);
    QueuedOperation.Builder queuedOperationBuilder = QueuedOperation.newBuilder();
    ListenableFuture<ProfiledQueuedOperationMetadata.Builder> queuedFuture = transformAsync(
        actionFuture,
        (action) -> {
          logger.info(
              format(
                  "ShardInstance(%s): queue(%s): fetched action %s transforming queuedOperation",
                  getName(),
                  operation.getName(),
                  actionDigest.getHash()));
          Stopwatch transformStopwatch = Stopwatch.createStarted();
          return transform(
              transformQueuedOperation(
                  operation.getName(),
                  action,
                  action.getCommandDigest(),
                  action.getInputRootDigest(),
                  queuedOperationBuilder,
                  operationTransformService),
              (queuedOperation) -> ProfiledQueuedOperationMetadata.newBuilder()
                  .setQueuedOperation(queuedOperation)
                  .setQueuedOperationMetadata(buildQueuedOperationMetadata(
                      metadata,
                      executeEntry.getRequestMetadata(),
                      queuedOperation))
                  .setTransformedIn(Durations.fromMicros(transformStopwatch.elapsed(MICROSECONDS))),
              operationTransformService);
        },
        operationTransformService);
    ListenableFuture<ProfiledQueuedOperationMetadata.Builder> validatedFuture = transformAsync(
        queuedFuture,
        (profiledQueuedMetadata) -> {
          logger.info(
              format(
                  "ShardInstance(%s): queue(%s): queuedOperation %s transformed, validating",
                  getName(),
                  operation.getName(),
                  DigestUtil.toString(profiledQueuedMetadata.getQueuedOperationMetadata().getQueuedOperationDigest())));
          long startValidateUSecs = stopwatch.elapsed(MICROSECONDS);
          /* sync, throws StatusException */
          validateQueuedOperation(actionDigest, profiledQueuedMetadata.getQueuedOperation());
          return immediateFuture(
              profiledQueuedMetadata
                  .setValidatedIn(Durations.fromMicros(stopwatch.elapsed(MICROSECONDS) - startValidateUSecs)));
        },
        operationTransformService);
    ListenableFuture<ProfiledQueuedOperationMetadata> queuedOperationCommittedFuture = transformAsync(
        validatedFuture,
        (profiledQueuedMetadata) -> {
          logger.info(
              format(
                  "ShardInstance(%s): queue(%s): queuedOperation %s validated, uploading",
                  getName(),
                  operation.getName(),
                  DigestUtil.toString(profiledQueuedMetadata.getQueuedOperationMetadata().getQueuedOperationDigest())));
          ByteString queuedOperationBlob = profiledQueuedMetadata.getQueuedOperation().toByteString();
          Digest queuedOperationDigest = profiledQueuedMetadata
              .getQueuedOperationMetadata()
                  .getQueuedOperationDigest();
          long startUploadUSecs = stopwatch.elapsed(MICROSECONDS);
          return transform(
              writeBlobFuture(queuedOperationDigest, queuedOperationBlob),
              (committedSize) -> profiledQueuedMetadata
                  .setUploadedIn(Durations.fromMicros(stopwatch.elapsed(MICROSECONDS) - startUploadUSecs))
                  .build(),
              operationTransformService);
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
            com.google.rpc.Status status = StatusProto.fromThrowable(t);
            if (status == null) {
              logger.log(SEVERE, "no rpc status from exception for " + operation.getName(), t);
              status = com.google.rpc.Status.newBuilder()
                  .setCode(Status.fromThrowable(t).getCode().value())
                  .build();
            }
            logFailedStatus(actionDigest, status);
            errorOperationFuture(operation, status, queueFuture);
          }
        },
        operationTransformService);
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
  public ListenableFuture<Void> watchOperation(
      String operationName,
      Watcher watcher) {
    Operation operation = getOperation(operationName);
    try {
      watcher.observe(stripOperation(operation));
    } catch (Throwable t) {
      return immediateFailedFuture(t);
    }
    if (operation == null || operation.getDone()) {
      return immediateFuture(null);
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
