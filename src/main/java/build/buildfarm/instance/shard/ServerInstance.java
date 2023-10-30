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

import static build.buildfarm.cas.ContentAddressableStorage.NOT_FOUND;
import static build.buildfarm.cas.ContentAddressableStorage.OK;
import static build.buildfarm.common.Actions.asExecutionStatus;
import static build.buildfarm.common.Actions.checkPreconditionFailure;
import static build.buildfarm.common.Actions.invalidActionVerboseMessage;
import static build.buildfarm.common.Errors.VIOLATION_TYPE_INVALID;
import static build.buildfarm.common.Errors.VIOLATION_TYPE_MISSING;
import static build.buildfarm.common.config.Backplane.BACKPLANE_TYPE.SHARD;
import static build.buildfarm.instance.shard.Util.SHARD_IS_RETRIABLE;
import static build.buildfarm.instance.shard.Util.correctMissingBlob;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.catching;
import static com.google.common.util.concurrent.Futures.catchingAsync;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static net.javacrumbs.futureconverter.java8guava.FutureConverter.toCompletableFuture;
import static net.javacrumbs.futureconverter.java8guava.FutureConverter.toListenableFuture;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.BatchReadBlobsResponse.Response;
import build.bazel.remote.execution.v2.CacheCapabilities;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.ExecutionPolicy;
import build.bazel.remote.execution.v2.ExecutionStage;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.Platform.Property;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.ResultsCachePolicy;
import build.bazel.remote.execution.v2.SymlinkAbsolutePathStrategy;
import build.buildfarm.actioncache.ActionCache;
import build.buildfarm.actioncache.ShardActionCache;
import build.buildfarm.backplane.Backplane;
import build.buildfarm.common.BuildfarmExecutors;
import build.buildfarm.common.CasIndexResults;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.EntryLimitException;
import build.buildfarm.common.ExecutionProperties;
import build.buildfarm.common.Poller;
import build.buildfarm.common.TokenizableIterator;
import build.buildfarm.common.TreeIterator;
import build.buildfarm.common.TreeIterator.DirectoryEntry;
import build.buildfarm.common.Watcher;
import build.buildfarm.common.Write;
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.common.grpc.UniformDelegateServerCallStreamObserver;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.MatchListener;
import build.buildfarm.instance.server.NodeInstance;
import build.buildfarm.operations.EnrichedOperation;
import build.buildfarm.operations.FindOperationsResults;
import build.buildfarm.v1test.BackplaneStatus;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.GetClientStartTimeRequest;
import build.buildfarm.v1test.GetClientStartTimeResult;
import build.buildfarm.v1test.OperationIteratorToken;
import build.buildfarm.v1test.ProfiledQueuedOperationMetadata;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.QueueStatus;
import build.buildfarm.v1test.QueuedOperation;
import build.buildfarm.v1test.QueuedOperationMetadata;
import build.buildfarm.v1test.Tree;
import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.PreconditionFailure;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.ServerCallStreamObserver;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.naming.ConfigurationException;
import lombok.extern.java.Log;

@Log
public class ServerInstance extends NodeInstance {
  private static final ListenableFuture<Void> IMMEDIATE_VOID_FUTURE = Futures.immediateFuture(null);

  private static final String TIMEOUT_OUT_OF_BOUNDS =
      "A timeout specified is out of bounds with a configured range";

  private static final int DEFAULT_MAX_LOCAL_ACTION_CACHE_SIZE = 1000000;

  // Prometheus metrics
  private static final Counter executionSuccess =
      Counter.build().name("execution_success").help("Execution success.").register();
  private static final Gauge preQueueSize =
      Gauge.build().name("pre_queue_size").help("Pre queue size.").register();
  private static final Counter casHitCounter =
      Counter.build()
          .name("cas_hit")
          .help("Number of successful CAS hits from worker-worker.")
          .register();
  private static final Counter casMissCounter =
      Counter.build().name("cas_miss").help("Number of CAS misses from worker-worker.").register();
  private static final Counter requeueFailureCounter =
      Counter.build()
          .name("requeue_failure")
          .help("Number of operations that failed to requeue.")
          .register();
  private static final Counter queueFailureCounter =
      Counter.build()
          .name("queue_failure")
          .help("Number of operations that failed to queue.")
          .register();
  // Metrics about the dispatched operations
  private static final Gauge dispatchedOperationsSize =
      Gauge.build()
          .name("dispatched_operations_size")
          .help("Dispatched operations size.")
          .register();

  // Other metrics from the backplane
  private static final Gauge workerPoolSize =
      Gauge.build().name("worker_pool_size").help("Active worker pool size.").register();
  private static final Gauge queueSize =
      Gauge.build().name("queue_size").labelNames("queue_name").help("Queue size.").register();

  private static final Histogram ioMetric =
      Histogram.build()
          .name("io_bytes_read")
          .buckets(new double[] {10, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000})
          .help("Read I/O (bytes)")
          .register();

  private final Runnable onStop;
  private final long maxEntrySizeBytes;
  private final Backplane backplane;
  private final ActionCache actionCache;
  private final RemoteInputStreamFactory remoteInputStreamFactory;
  private final com.google.common.cache.LoadingCache<String, Instance> workerStubs;
  private final Thread dispatchedMonitor;
  private final Duration maxActionTimeout;
  private AsyncCache<Digest, Directory> directoryCache;
  private AsyncCache<Digest, Command> commandCache;
  private AsyncCache<Digest, Action> digestToActionCache;
  private Cache<RequestMetadata, Boolean> recentCacheServedExecutions;

  private final Random rand = new Random();
  private final Writes writes = new Writes(this::writeInstanceSupplier);
  private final int maxCpu;
  private final int maxRequeueAttempts;

  private final ListeningExecutorService operationTransformService =
      BuildfarmExecutors.getTransformServicePool();
  private final ListeningExecutorService actionCacheFetchService;
  private final ScheduledExecutorService contextDeadlineScheduler =
      newSingleThreadScheduledExecutor();
  private final ExecutorService operationDeletionService = newSingleThreadExecutor();
  private final BlockingQueue transformTokensQueue = new LinkedBlockingQueue(256);
  private final boolean useDenyList;
  private Thread operationQueuer;
  private boolean stopping = false;
  private boolean stopped = true;
  private final Thread prometheusMetricsThread;

  private static BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  // TODO: move to config
  private static final Duration queueTimeout = Durations.fromSeconds(60);

  private static Backplane createBackplane(String identifier) throws ConfigurationException {
    if (configs.getBackplane().getType().equals(SHARD)) {
      return new RedisShardBackplane(
          identifier,
          /* subscribeToBackplane=*/ true,
          configs.getServer().isRunFailsafeOperation(),
          ServerInstance::stripOperation,
          ServerInstance::stripQueuedOperation);
    } else {
      throw new IllegalArgumentException("Shard Backplane not set in config");
    }
  }

  public ServerInstance(String name, String identifier, DigestUtil digestUtil, Runnable onStop)
      throws InterruptedException, ConfigurationException {
    this(
        name,
        digestUtil,
        createBackplane(identifier),
        onStop,
        /* actionCacheFetchService=*/ BuildfarmExecutors.getActionCacheFetchServicePool());
  }

  private ServerInstance(
      String name,
      DigestUtil digestUtil,
      Backplane backplane,
      Runnable onStop,
      ListeningExecutorService actionCacheFetchService)
      throws InterruptedException {
    this(
        name,
        digestUtil,
        backplane,
        new ShardActionCache(
            DEFAULT_MAX_LOCAL_ACTION_CACHE_SIZE, backplane, actionCacheFetchService),
        configs.getServer().isRunDispatchedMonitor(),
        configs.getServer().getDispatchedMonitorIntervalSeconds(),
        configs.getServer().isRunOperationQueuer(),
        configs.getMaxEntrySizeBytes(),
        configs.getServer().getMaxCpu(),
        configs.getServer().getMaxRequeueAttempts(),
        Duration.newBuilder().setSeconds(configs.getMaximumActionTimeout()).build(),
        configs.getServer().isUseDenyList(),
        onStop,
        WorkerStubs.create(
            digestUtil,
            Duration.newBuilder().setSeconds(configs.getServer().getGrpcTimeout()).build()),
        actionCacheFetchService,
        configs.getServer().isEnsureOutputsPresent());
  }

  void initializeCaches() {
    directoryCache =
        Caffeine.newBuilder()
            .newBuilder()
            .maximumSize(configs.getServer().getCaches().getDirectoryCacheMaxEntries())
            .buildAsync();
    commandCache =
        Caffeine.newBuilder()
            .newBuilder()
            .maximumSize(configs.getServer().getCaches().getCommandCacheMaxEntries())
            .buildAsync();
    digestToActionCache =
        Caffeine.newBuilder()
            .newBuilder()
            .maximumSize(configs.getServer().getCaches().getDigestToActionCacheMaxEntries())
            .buildAsync();
    recentCacheServedExecutions =
        Caffeine.newBuilder()
            .newBuilder()
            .maximumSize(configs.getServer().getCaches().getRecentServedExecutionsCacheMaxEntries())
            .build();
  }

  public ServerInstance(
      String name,
      DigestUtil digestUtil,
      Backplane backplane,
      ActionCache actionCache,
      boolean runDispatchedMonitor,
      int dispatchedMonitorIntervalSeconds,
      boolean runOperationQueuer,
      long maxEntrySizeBytes,
      int maxCpu,
      int maxRequeueAttempts,
      Duration maxActionTimeout,
      boolean useDenyList,
      Runnable onStop,
      LoadingCache<String, Instance> workerStubs,
      ListeningExecutorService actionCacheFetchService,
      boolean ensureOutputsPresent) {
    super(
        name,
        digestUtil,
        /* contentAddressableStorage=*/ null,
        /* actionCache=*/ actionCache,
        /* outstandingOperations=*/ null,
        /* completedOperations=*/ null,
        /* activeBlobWrites=*/ null,
        ensureOutputsPresent);
    this.backplane = backplane;
    this.actionCache = actionCache;
    this.workerStubs = workerStubs;
    this.onStop = onStop;
    this.maxEntrySizeBytes = maxEntrySizeBytes;
    this.maxCpu = maxCpu;
    this.maxRequeueAttempts = maxRequeueAttempts;
    this.maxActionTimeout = maxActionTimeout;
    this.useDenyList = useDenyList;
    this.actionCacheFetchService = actionCacheFetchService;
    backplane.setOnUnsubscribe(this::stop);

    initializeCaches();

    remoteInputStreamFactory =
        new RemoteInputStreamFactory(
            backplane, rand, workerStubs, this::removeMalfunctioningWorker);

    if (runDispatchedMonitor) {
      dispatchedMonitor =
          new Thread(
              new DispatchedMonitor(
                  backplane, this::requeueOperation, dispatchedMonitorIntervalSeconds));
    } else {
      dispatchedMonitor = null;
    }

    if (runOperationQueuer) {
      operationQueuer =
          new Thread(
              new Runnable() {
                final Stopwatch stopwatch = Stopwatch.createUnstarted();

                ListenableFuture<Void> iterate() throws IOException, InterruptedException {
                  ensureCanQueue(stopwatch); // wait for transition to canQueue state
                  long canQueueUSecs = stopwatch.elapsed(MICROSECONDS);
                  stopwatch.stop();
                  ExecuteEntry executeEntry = backplane.deprequeueOperation();
                  stopwatch.start();
                  if (executeEntry == null) {
                    log.log(Level.SEVERE, "OperationQueuer: Got null from deprequeue...");
                    return immediateFuture(null);
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
                            log.log(
                                Level.SEVERE,
                                format("error polling %s for queuing", operationName),
                                e);
                          }
                          // mostly ignore, we will be stopped at some point later
                        }
                        return !stopping && !stopped;
                      },
                      () -> {},
                      Deadline.after(5, MINUTES));
                  try {
                    log.log(Level.FINER, "queueing " + operationName);
                    ListenableFuture<Void> queueFuture = queue(executeEntry, poller, queueTimeout);
                    addCallback(
                        queueFuture,
                        new FutureCallback<Void>() {
                          @Override
                          public void onSuccess(Void result) {
                            log.log(Level.FINER, "successfully queued " + operationName);
                            // nothing
                          }

                          @Override
                          public void onFailure(Throwable t) {
                            queueFailureCounter.inc();
                            log.log(Level.SEVERE, "error queueing " + operationName, t);
                          }
                        },
                        operationTransformService);
                    long operationTransformDispatchUSecs =
                        stopwatch.elapsed(MICROSECONDS) - canQueueUSecs;
                    log.log(
                        Level.FINER,
                        format(
                            "OperationQueuer: Dispatched To Transform %s: %dus in canQueue, %dus in transform dispatch",
                            operationName, canQueueUSecs, operationTransformDispatchUSecs));
                    return queueFuture;
                  } catch (Throwable t) {
                    poller.pause();
                    queueFailureCounter.inc();
                    log.log(Level.SEVERE, "error queueing " + operationName, t);
                    return immediateFuture(null);
                  }
                }

                @Override
                public void run() {
                  log.log(Level.FINER, "OperationQueuer: Running");
                  try {
                    while (transformTokensQueue.offer(new Object(), 5, MINUTES)) {
                      stopwatch.start();
                      try {
                        iterate()
                            .addListener(
                                () -> {
                                  try {
                                    transformTokensQueue.take();
                                  } catch (InterruptedException e) {
                                    log.log(
                                        Level.SEVERE,
                                        "interrupted while returning transform token",
                                        e);
                                  }
                                },
                                operationTransformService);
                      } catch (IOException e) {
                        transformTokensQueue.take();
                        // problems interacting with backplane
                      } finally {
                        stopwatch.reset();
                      }
                    }
                    log.severe("OperationQueuer: Transform lease token timed out");
                  } catch (InterruptedException e) {
                    // treat with exit
                    operationQueuer = null;
                    return;
                  } catch (Exception t) {
                    log.log(Level.SEVERE, "OperationQueuer: fatal exception during iteration", t);
                  } finally {
                    log.log(Level.FINER, "OperationQueuer: Exiting");
                  }
                  operationQueuer = null;
                  try {
                    stop();
                  } catch (InterruptedException e) {
                    log.log(Level.SEVERE, "interrupted while stopping instance " + getName(), e);
                  }
                }
              });
    } else {
      operationQueuer = null;
    }

    prometheusMetricsThread =
        new Thread(
            () -> {
              while (!Thread.currentThread().isInterrupted()) {
                try {
                  TimeUnit.SECONDS.sleep(30);
                  BackplaneStatus backplaneStatus = backplaneStatus();
                  workerPoolSize.set(backplaneStatus.getActiveWorkersCount());
                  dispatchedOperationsSize.set(backplaneStatus.getDispatchedSize());
                  preQueueSize.set(backplaneStatus.getPrequeue().getSize());
                  updateQueueSizes(backplaneStatus.getOperationQueue().getProvisionsList());
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  break;
                } catch (Exception e) {
                  log.log(Level.SEVERE, "Could not update RedisShardBackplane metrics", e);
                }
              }
            },
            "Prometheus Metrics Collector");
  }

  private void updateQueueSizes(List<QueueStatus> queues) {
    if (queueSize != null) {
      for (QueueStatus queueStatus : queues) {
        queueSize.labels(queueStatus.getName()).set(queueStatus.getSize());
      }
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
  public void start(String publicName) {
    stopped = false;
    try {
      backplane.start(publicName);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (RuntimeException e) {
      try {
        stop();
      } catch (InterruptedException intEx) {
        e.addSuppressed(intEx);
      }
      throw e;
    }
    if (dispatchedMonitor != null) {
      dispatchedMonitor.start();
    }
    if (operationQueuer != null) {
      operationQueuer.start();
    }

    if (prometheusMetricsThread != null) {
      prometheusMetricsThread.start();
    }
  }

  @Override
  public void stop() throws InterruptedException {
    if (stopped || stopping) {
      return;
    }
    stopping = true;
    log.log(Level.FINER, format("Instance %s is stopping", getName()));
    if (operationQueuer != null) {
      operationQueuer.interrupt();
      operationQueuer.join();
    }
    if (dispatchedMonitor != null) {
      dispatchedMonitor.interrupt();
      dispatchedMonitor.join();
    }
    if (prometheusMetricsThread != null) {
      prometheusMetricsThread.interrupt();
    }
    contextDeadlineScheduler.shutdown();
    operationDeletionService.shutdown();
    operationTransformService.shutdown();
    actionCacheFetchService.shutdown();
    onStop.run();
    backplane.stop();
    if (!contextDeadlineScheduler.awaitTermination(10, SECONDS)) {
      log.log(
          Level.SEVERE,
          "Could not shut down operation deletion service, some operations may be zombies");
    }
    if (!operationDeletionService.awaitTermination(10, SECONDS)) {
      log.log(
          Level.SEVERE,
          "Could not shut down operation deletion service, some operations may be zombies");
    }
    operationDeletionService.shutdownNow();
    if (!operationTransformService.awaitTermination(10, SECONDS)) {
      log.log(Level.SEVERE, "Could not shut down operation transform service");
    }
    operationTransformService.shutdownNow();
    if (!actionCacheFetchService.awaitTermination(10, SECONDS)) {
      log.log(Level.SEVERE, "Could not shut down action cache fetch service");
    }
    actionCacheFetchService.shutdownNow();
    workerStubs.invalidateAll();
    log.log(Level.FINER, format("Instance %s has been stopped", getName()));
    stopping = false;
    stopped = true;
  }

  @Override
  public boolean containsBlob(Digest digest, Digest.Builder result, RequestMetadata requestMetadata)
      throws InterruptedException {
    Iterable<Digest> missingOrPopulated;
    try {
      missingOrPopulated = findMissingBlobs(ImmutableList.of(digest), requestMetadata).get();
    } catch (ExecutionException e) {
      throwIfUnchecked(e.getCause());
      throw new RuntimeException(e.getCause());
    }
    if (digest.getSizeBytes() == -1) {
      Digest responseDigest = Iterables.getOnlyElement(missingOrPopulated);
      if (responseDigest.getSizeBytes() == -1) {
        return false;
      }
      result.mergeFrom(responseDigest);
      return true;
    }
    return Iterables.isEmpty(missingOrPopulated);
  }

  @Override
  public ListenableFuture<Iterable<Digest>> findMissingBlobs(
      Iterable<Digest> blobDigests, RequestMetadata requestMetadata) {
    // Some requests have been blocked, and we should tell the client we refuse to perform a lookup.
    try {
      if (inDenyList(requestMetadata)) {
        return immediateFailedFuture(
            Status.UNAVAILABLE
                .withDescription("The action associated with this request is forbidden")
                .asException());
      }
    } catch (IOException e) {
      return immediateFailedFuture(Status.fromThrowable(e).asException());
    }

    // Empty blobs are an exceptional case. Filter them out.
    // If the user only requested empty blobs we can immediately tell them we already have it.
    Iterable<Digest> nonEmptyDigests =
        Iterables.filter(blobDigests, (digest) -> digest.getSizeBytes() != 0);
    if (Iterables.isEmpty(nonEmptyDigests)) {
      return immediateFuture(ImmutableList.of());
    }

    if (configs.getServer().isFindMissingBlobsViaBackplane()) {
      return findMissingBlobsViaBackplane(nonEmptyDigests, requestMetadata);
    }

    return findMissingBlobsQueryingEachWorker(nonEmptyDigests, requestMetadata);
  }

  class FindMissingResponseEntry {
    final String worker;
    final long elapsedMicros;
    final Throwable exception;
    final int stillMissingAfter;

    FindMissingResponseEntry(
        String worker, long elapsedMicros, Throwable exception, int stillMissingAfter) {
      this.worker = worker;
      this.elapsedMicros = elapsedMicros;
      this.exception = exception;
      this.stillMissingAfter = stillMissingAfter;
    }
  }

  // A more accurate way to verify missing blobs is to ask the CAS participants directly if they
  // have the blobs.  To do this, we get all the worker nodes that are participating in the CAS
  // as a random list to begin our search.  If there are no workers available, tell the client all
  // blobs are missing.
  private ListenableFuture<Iterable<Digest>> findMissingBlobsQueryingEachWorker(
      Iterable<Digest> nonEmptyDigests, RequestMetadata requestMetadata) {
    Deque<String> workers;
    try {
      List<String> workersList = new ArrayList<>(backplane.getStorageWorkers());
      Collections.shuffle(workersList, rand);
      workers = new ArrayDeque<>(workersList);
    } catch (IOException e) {
      return immediateFailedFuture(Status.fromThrowable(e).asException());
    }
    if (workers.isEmpty()) {
      return immediateFuture(nonEmptyDigests);
    }

    // Search through all of the workers to decide which CAS blobs are missing.
    SettableFuture<Iterable<Digest>> missingDigestsFuture = SettableFuture.create();
    findMissingBlobsOnWorker(
        UUID.randomUUID().toString(),
        nonEmptyDigests,
        workers,
        ImmutableList.builder(),
        Iterables.size(nonEmptyDigests),
        Context.current().fixedContextExecutor(directExecutor()),
        missingDigestsFuture,
        requestMetadata);
    return missingDigestsFuture;
  }

  // This is a faster strategy to check missing blobs which does not require querying the CAS.
  // With hundreds of worker machines, it may be too expensive to query all of them for "find
  // missing blobs".
  // Workers register themselves with the backplane for a 30-second window, and if they fail to
  // re-register within this time frame, they are automatically removed from the backplane. While
  // this alternative strategy for finding missing blobs is faster and more cost-effective than
  // the exhaustive approach of querying each worker to find the digest, it comes with a higher
  // risk of returning expired workers despite filtering by active workers below. This is because
  // the strategy may return workers that have expired in the last 30 seconds. However, checking
  // workers directly is not a guarantee either since workers could leave the cluster after being
  // queried. Ultimately, it will come down to the client's resiliency if the backplane is
  // out-of-date and the server lies about which blobs are actually present. We provide this
  // alternative strategy for calculating missing blobs.
  private ListenableFuture<Iterable<Digest>> findMissingBlobsViaBackplane(
      Iterable<Digest> nonEmptyDigests, RequestMetadata requestMetadata) {
    try {
      Set<Digest> uniqueDigests = new HashSet<>();
      nonEmptyDigests.forEach(uniqueDigests::add);
      Map<Digest, Set<String>> foundBlobs = backplane.getBlobDigestsWorkers(uniqueDigests);
      Set<String> workerSet = backplane.getStorageWorkers();
      Map<String, Long> workersStartTime = backplane.getWorkersStartTimeInEpochSecs(workerSet);
      Map<Digest, Set<String>> digestAndWorkersMap =
          uniqueDigests.stream()
              .map(
                  digest -> {
                    Set<String> initialWorkers =
                        foundBlobs.getOrDefault(digest, Collections.emptySet());
                    return new AbstractMap.SimpleEntry<>(
                        digest,
                        filterAndAdjustWorkersForDigest(
                            digest, initialWorkers, workerSet, workersStartTime));
                  })
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      ListenableFuture<Iterable<Digest>> missingDigestFuture =
          immediateFuture(
              digestAndWorkersMap.entrySet().stream()
                  .filter(entry -> entry.getValue().isEmpty())
                  .map(Map.Entry::getKey)
                  .collect(Collectors.toList()));
      return transformAsync(
          missingDigestFuture,
          (missingDigest) -> {
            extendLeaseForDigests(digestAndWorkersMap, requestMetadata);
            return immediateFuture(missingDigest);
          },
          // Propagate context values but don't cascade its cancellation for downstream calls.
          Context.current().fork().fixedContextExecutor(directExecutor()));
    } catch (Exception e) {
      log.log(Level.SEVERE, "find missing blob via backplane failed", e);
      return immediateFailedFuture(Status.fromThrowable(e).asException());
    }
  }

  private Set<String> filterAndAdjustWorkersForDigest(
      Digest digest,
      Set<String> originalWorkerSetWithDigest,
      Set<String> activeWorkers,
      Map<String, Long> workersStartTime) {
    long insertTime;
    try {
      insertTime = backplane.getDigestInsertTime(digest);
    } catch (IOException e) {
      log.log(Level.WARNING, format("failed to get digest (%s) insertion time", digest));
      return Collections.emptySet();
    }
    Set<String> activeWorkersWithDigest =
        Sets.intersection(originalWorkerSetWithDigest, activeWorkers);
    Set<String> workersStartedBeforeDigestInsertion =
        activeWorkersWithDigest.stream()
            .filter(
                worker ->
                    workersStartTime.getOrDefault(worker, Instant.now().getEpochSecond())
                        < insertTime)
            .collect(Collectors.toSet());
    Set<String> workersToBeRemoved =
        Sets.difference(originalWorkerSetWithDigest, workersStartedBeforeDigestInsertion)
            .immutableCopy();
    if (!workersToBeRemoved.isEmpty()) {
      try {
        log.log(Level.INFO, format("adjusting locations for the digest %s", digest));
        backplane.adjustBlobLocations(digest, Collections.emptySet(), workersToBeRemoved);
      } catch (IOException e) {
        log.log(
            Level.WARNING,
            format("error adjusting blob location for %s", DigestUtil.toString(digest)),
            e);
      }
    }
    return workersStartedBeforeDigestInsertion;
  }

  private void extendLeaseForDigests(
      Map<Digest, Set<String>> digestAndWorkersMap, RequestMetadata requestMetadata) {
    Map<String, Set<Digest>> workerAndDigestMap = new HashMap<>();
    digestAndWorkersMap.forEach(
        (digest, workers) ->
            workers.forEach(
                worker ->
                    workerAndDigestMap.computeIfAbsent(worker, w -> new HashSet<>()).add(digest)));

    workerAndDigestMap.forEach(
        (worker, digests) -> workerStub(worker).findMissingBlobs(digests, requestMetadata));

    try {
      backplane.updateDigestsExpiry(digestAndWorkersMap.keySet());
    } catch (IOException e) {
      log.log(
          Level.WARNING,
          format(
              "Failed to update expiry duration for digests (%s) insertion time",
              digestAndWorkersMap.keySet()));
    }
  }

  private void findMissingBlobsOnWorker(
      String requestId,
      Iterable<Digest> blobDigests,
      Deque<String> workers,
      ImmutableList.Builder<FindMissingResponseEntry> responses,
      int originalSize,
      Executor executor,
      SettableFuture<Iterable<Digest>> missingDigestsFuture,
      RequestMetadata requestMetadata) {
    String worker = workers.removeFirst();
    ListenableFuture<Iterable<Digest>> workerMissingBlobsFuture =
        workerStub(worker).findMissingBlobs(blobDigests, requestMetadata);

    Stopwatch stopwatch = Stopwatch.createStarted();
    addCallback(
        workerMissingBlobsFuture,
        new FutureCallback<Iterable<Digest>>() {
          @Override
          public void onSuccess(Iterable<Digest> missingDigests) {
            if (Iterables.isEmpty(missingDigests) || workers.isEmpty()) {
              missingDigestsFuture.set(missingDigests);
            } else {
              responses.add(
                  new FindMissingResponseEntry(
                      worker,
                      stopwatch.elapsed(MICROSECONDS),
                      null,
                      Iterables.size(missingDigests)));
              findMissingBlobsOnWorker(
                  requestId,
                  missingDigests,
                  workers,
                  responses,
                  originalSize,
                  executor,
                  missingDigestsFuture,
                  requestMetadata);
            }
          }

          @Override
          public void onFailure(Throwable t) {
            responses.add(
                new FindMissingResponseEntry(
                    worker, stopwatch.elapsed(MICROSECONDS), t, Iterables.size(blobDigests)));
            Status status = Status.fromThrowable(t);
            if (status.getCode() == Code.UNAVAILABLE || status.getCode() == Code.UNIMPLEMENTED) {
              removeMalfunctioningWorker(worker, t, "findMissingBlobs(" + requestId + ")");
            } else if (status.getCode() == Code.DEADLINE_EXCEEDED) {
              for (FindMissingResponseEntry response : responses.build()) {
                log.log(
                    response.exception == null ? Level.WARNING : Level.SEVERE,
                    format(
                        "DEADLINE_EXCEEDED: findMissingBlobs(%s) %s: %d remaining of %d %dus%s",
                        requestId,
                        response.worker,
                        response.stillMissingAfter,
                        originalSize,
                        response.elapsedMicros,
                        response.exception != null ? ": " + response.exception.toString() : ""));
              }
              missingDigestsFuture.setException(status.asException());
            } else if (status.getCode() == Code.CANCELLED
                || Context.current().isCancelled()
                || !SHARD_IS_RETRIABLE.test(status)) {
              // do nothing further if we're cancelled
              missingDigestsFuture.setException(status.asException());
            } else {
              // why not, always
              workers.addLast(worker);
            }

            if (!missingDigestsFuture.isDone()) {
              if (workers.isEmpty()) {
                missingDigestsFuture.set(blobDigests);
              } else {
                findMissingBlobsOnWorker(
                    requestId,
                    blobDigests,
                    workers,
                    responses,
                    originalSize,
                    executor,
                    missingDigestsFuture,
                    requestMetadata);
              }
            }
          }
        },
        executor);
  }

  private void fetchBlobFromWorker(
      Compressor.Value compressor,
      Digest blobDigest,
      Deque<String> workers,
      long offset,
      long count,
      ServerCallStreamObserver<ByteString> blobObserver,
      RequestMetadata requestMetadata) {
    String worker = workers.removeFirst();
    workerStub(worker)
        .getBlob(
            compressor,
            blobDigest,
            offset,
            count,
            new UniformDelegateServerCallStreamObserver<ByteString>(blobObserver) {
              long received = 0;

              @Override
              public void onNext(ByteString nextChunk) {
                blobObserver.onNext(nextChunk);
                received += nextChunk.size();
                ioMetric.observe(nextChunk.size());
              }

              @Override
              public void onError(Throwable t) {
                Status status = Status.fromThrowable(t);
                if (status.getCode() == Code.UNAVAILABLE
                    || status.getCode() == Code.UNIMPLEMENTED) {
                  removeMalfunctioningWorker(
                      worker, t, "getBlob(" + DigestUtil.toString(blobDigest) + ")");
                } else if (status.getCode() == Code.NOT_FOUND) {
                  casMissCounter.inc();
                  log.log(
                      configs.getServer().isEnsureOutputsPresent() ? Level.WARNING : Level.FINER,
                      worker + " did not contain " + DigestUtil.toString(blobDigest));
                  // ignore this, the worker will update the backplane eventually
                } else if (status.getCode() != Code.DEADLINE_EXCEEDED
                    && SHARD_IS_RETRIABLE.test(status)) {
                  // why not, always
                  workers.addLast(worker);
                } else {
                  log.log(
                      Level.WARNING,
                      format(
                          "%s: read(%s) on worker %s after %d bytes of content",
                          status.getCode().name(),
                          DigestUtil.toString(blobDigest),
                          worker,
                          received));
                  blobObserver.onError(t);
                  return;
                }

                if (workers.isEmpty()) {
                  blobObserver.onError(Status.NOT_FOUND.asException());
                } else {
                  if (count < received) {
                    blobObserver.onError(
                        new IllegalArgumentException(
                            format("count (%d) < received (%d)", count, received)));
                  } else {
                    long nextCount = count - received;
                    if (nextCount == 0) {
                      // be gracious and terminate the blobObserver here
                      onCompleted();
                    } else {
                      try {
                        fetchBlobFromWorker(
                            compressor,
                            blobDigest,
                            workers,
                            offset + received,
                            nextCount,
                            blobObserver,
                            requestMetadata);
                      } catch (Exception e) {
                        blobObserver.onError(e);
                      }
                    }
                  }
                }
              }

              @Override
              public void onCompleted() {
                blobObserver.onCompleted();
                casHitCounter.inc();
              }
            },
            requestMetadata);
  }

  @Override
  public ListenableFuture<List<Response>> getAllBlobsFuture(Iterable<Digest> digests) {
    Executor contextExecutor = Context.current().fixedContextExecutor(directExecutor());
    return allAsList(
        Iterables.transform(
            digests,
            digest ->
                catching(
                    transform(
                        getBlobFuture(
                            Compressor.Value.IDENTITY,
                            digest,
                            RequestMetadata.getDefaultInstance()),
                        blob -> {
                          Response.Builder response = Response.newBuilder().setDigest(digest);
                          if (blob == null) {
                            response.setStatus(NOT_FOUND);
                          } else {
                            response.setData(blob).setStatus(OK);
                          }
                          return response.build();
                        },
                        contextExecutor),
                    Exception.class,
                    e ->
                        Response.newBuilder()
                            .setDigest(digest)
                            .setStatus(StatusProto.fromThrowable(e))
                            .build(),
                    contextExecutor)));
  }

  @Override
  public void getBlob(
      Compressor.Value compressor,
      Digest blobDigest,
      long offset,
      long count,
      ServerCallStreamObserver<ByteString> blobObserver,
      RequestMetadata requestMetadata) {
    List<String> workersList;
    Set<String> workerSet;
    Set<String> locationSet;
    try {
      workerSet = backplane.getStorageWorkers();
      locationSet = backplane.getBlobLocationSet(blobDigest);
      workersList = new ArrayList<>(Sets.intersection(locationSet, workerSet));
    } catch (IOException e) {
      blobObserver.onError(e);
      return;
    }
    boolean emptyWorkerList = workersList.isEmpty();
    final ListenableFuture<List<String>> populatedWorkerListFuture;
    if (emptyWorkerList) {
      log.log(
          Level.FINER,
          format(
              "worker list was initially empty for %s, attempting to correct",
              DigestUtil.toString(blobDigest)));
      populatedWorkerListFuture =
          transform(
              correctMissingBlob(
                  backplane,
                  workerSet,
                  locationSet,
                  this::workerStub,
                  blobDigest,
                  directExecutor(),
                  RequestMetadata.getDefaultInstance()),
              (foundOnWorkers) -> {
                log.log(
                    Level.FINER,
                    format(
                        "worker list was corrected for %s to be %s",
                        DigestUtil.toString(blobDigest), foundOnWorkers.toString()));
                Iterables.addAll(workersList, foundOnWorkers);
                return workersList;
              },
              directExecutor());
    } else {
      populatedWorkerListFuture = immediateFuture(workersList);
    }

    Context ctx = Context.current();
    ServerCallStreamObserver<ByteString> chunkObserver =
        new UniformDelegateServerCallStreamObserver<ByteString>(blobObserver) {
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
              log.log(
                  Level.FINER,
                  format(
                      "worker list was depleted for %s, attempting to correct",
                      DigestUtil.toString(blobDigest)));
              workersListFuture =
                  transform(
                      correctMissingBlob(
                          backplane,
                          workerSet,
                          locationSet,
                          ServerInstance.this::workerStub,
                          blobDigest,
                          directExecutor(),
                          RequestMetadata.getDefaultInstance()),
                      (foundOnWorkers) -> {
                        log.log(
                            Level.FINER,
                            format(
                                "worker list was corrected after depletion for %s to be %s",
                                DigestUtil.toString(blobDigest), foundOnWorkers.toString()));
                        Iterables.addAll(workersList, foundOnWorkers);
                        return workersList;
                      },
                      directExecutor());
              final ServerCallStreamObserver<ByteString> checkedChunkObserver = this;
              addCallback(
                  workersListFuture,
                  new WorkersCallback(rand) {
                    @Override
                    public void onQueue(Deque<String> workers) {
                      ctx.run(
                          () -> {
                            try {
                              fetchBlobFromWorker(
                                  compressor,
                                  blobDigest,
                                  workers,
                                  offset,
                                  count,
                                  checkedChunkObserver,
                                  requestMetadata);
                            } catch (Exception e) {
                              onFailure(e);
                            }
                          });
                    }

                    @Override
                    public void onFailure(Throwable t) {
                      blobObserver.onError(t);
                    }
                  },
                  directExecutor());
            } else {
              blobObserver.onError(t);
            }
          }

          @Override
          public void onCompleted() {
            blobObserver.onCompleted();
          }
        };
    addCallback(
        populatedWorkerListFuture,
        new WorkersCallback(rand) {
          @Override
          public void onQueue(Deque<String> workers) {
            ctx.run(
                () -> {
                  try {
                    fetchBlobFromWorker(
                        compressor,
                        blobDigest,
                        workers,
                        offset,
                        count,
                        chunkObserver,
                        requestMetadata);
                  } catch (Exception e) {
                    onFailure(e);
                  }
                });
          }

          @Override
          public void onFailure(Throwable t) {
            blobObserver.onError(t);
          }
        },
        directExecutor());
  }

  public abstract static class WorkersCallback implements FutureCallback<List<String>> {
    private final Random rand;

    public WorkersCallback(Random rand) {
      this.rand = rand;
    }

    @Override
    public void onSuccess(List<String> workersList) {
      if (workersList.isEmpty()) {
        onFailure(Status.NOT_FOUND.withDescription("No workers found.").asException());
      } else {
        Collections.shuffle(workersList, rand);
        onQueue(new ArrayDeque<String>(workersList));
      }
    }

    protected abstract void onQueue(Deque<String> workers);
  }

  private Instance writeInstanceSupplier() {
    String worker = getRandomWorker();
    return workerStub(worker);
  }

  String getRandomWorker() {
    Set<String> workerSet;
    try {
      workerSet = backplane.getStorageWorkers();
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
    if (workerSet.isEmpty()) {
      throw Status.UNAVAILABLE.withDescription("no available workers").asRuntimeException();
    }
    int index = rand.nextInt(workerSet.size());
    // best case no allocation average n / 2 selection
    Iterator<String> iter = workerSet.iterator();
    String worker = null;
    while (iter.hasNext() && index-- >= 0) {
      worker = iter.next();
    }
    return worker;
  }

  private Instance workerStub(String worker) {
    try {
      return workerStubs.get(worker);
    } catch (ExecutionException e) {
      log.log(Level.SEVERE, "error getting worker stub for " + worker, e);
      throw new IllegalStateException("stub instance creation must not fail");
    }
  }

  @Override
  public InputStream newBlobInput(
      Compressor.Value compressor,
      Digest digest,
      long offset,
      long deadlineAfter,
      TimeUnit deadlineAfterUnits,
      RequestMetadata requestMetadata)
      throws IOException {
    return remoteInputStreamFactory.newInput(
        compressor, digest, offset, deadlineAfter, deadlineAfterUnits, requestMetadata);
  }

  @Override
  public Write getBlobWrite(
      Compressor.Value compressor, Digest digest, UUID uuid, RequestMetadata requestMetadata)
      throws EntryLimitException {
    try {
      if (inDenyList(requestMetadata)) {
        throw Status.UNAVAILABLE.withDescription(BLOCK_LIST_ERROR).asRuntimeException();
      }
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
    if (maxEntrySizeBytes > 0 && digest.getSizeBytes() > maxEntrySizeBytes) {
      throw new EntryLimitException(digest.getSizeBytes(), maxEntrySizeBytes);
    }
    // FIXME small blob write to proto cache
    return writes.get(compressor, digest, uuid, requestMetadata);
  }

  protected int getTreeDefaultPageSize() {
    return 1024;
  }

  protected int getTreeMaxPageSize() {
    return 1024;
  }

  protected TokenizableIterator<DirectoryEntry> createTreeIterator(
      String reason, Digest rootDigest, String pageToken) {
    return new TreeIterator(
        (directoryBlobDigest) -> {
          try {
            return catching(
                    expectDirectory(
                        reason, directoryBlobDigest, RequestMetadata.getDefaultInstance()),
                    Exception.class,
                    e -> {
                      log.log(
                          Level.SEVERE,
                          format(
                              "transformQueuedOperation(%s): error fetching directory %s",
                              reason, DigestUtil.toString(directoryBlobDigest)),
                          e);
                      return null;
                    },
                    directExecutor())
                .get();
          } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throwIfUnchecked(cause);
            throw new UncheckedExecutionException(cause);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
          }
        },
        rootDigest,
        pageToken);
  }

  abstract static class TreeCallback implements FutureCallback<DirectoryEntry> {
    private final SettableFuture<Void> future;

    TreeCallback(SettableFuture<Void> future) {
      this.future = future;
    }

    protected abstract void onDirectory(Digest digest, Directory directory);

    abstract boolean next();

    @Override
    public void onSuccess(DirectoryEntry entry) {
      if (entry.getDirectory() != null) {
        onDirectory(entry.getDigest(), entry.getDirectory());
      }
      if (!next()) {
        future.set(null);
      }
    }

    @Override
    public void onFailure(Throwable t) {
      future.setException(t);
    }
  }

  @Override
  protected ListenableFuture<Tree> getTreeFuture(
      String reason, Digest inputRoot, ExecutorService service, RequestMetadata requestMetadata) {
    SettableFuture<Void> future = SettableFuture.create();
    Tree.Builder tree = Tree.newBuilder().setRootDigest(inputRoot);
    Set<Digest> digests = Sets.newConcurrentHashSet();
    Queue<Digest> remaining = new ConcurrentLinkedQueue();
    remaining.offer(inputRoot);
    Context ctx = Context.current();
    TreeCallback callback =
        new TreeCallback(future) {
          @Override
          protected void onDirectory(Digest digest, Directory directory) {
            tree.putDirectories(digest.getHash(), directory);
            for (DirectoryNode childNode : directory.getDirectoriesList()) {
              Digest child = childNode.getDigest();
              if (digests.add(child)) {
                remaining.offer(child);
              }
            }
          }

          @Override
          boolean next() {
            Digest nextDigest = remaining.poll();
            if (!future.isDone() && nextDigest != null) {
              ctx.run(
                  () ->
                      addCallback(
                          transform(
                              expectDirectory(reason, nextDigest, requestMetadata),
                              directory -> new DirectoryEntry(nextDigest, directory),
                              service),
                          this,
                          service));
              return true;
            }
            return false;
          }
        };
    callback.next();
    return transform(future, (result) -> tree.build(), service);
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

  ListenableFuture<Directory> expectDirectory(
      String reason, Digest directoryBlobDigest, RequestMetadata requestMetadata) {
    if (directoryBlobDigest.getSizeBytes() == 0) {
      return immediateFuture(Directory.getDefaultInstance());
    }

    BiFunction<Digest, Executor, CompletableFuture<Directory>> getCallback =
        new BiFunction<Digest, Executor, CompletableFuture<Directory>>() {
          @Override
          public CompletableFuture<Directory> apply(Digest digest, Executor executor) {
            log.log(
                Level.FINER,
                format(
                    "transformQueuedOperation(%s): fetching directory %s",
                    reason, DigestUtil.toString(directoryBlobDigest)));

            Supplier<ListenableFuture<Directory>> fetcher =
                () ->
                    notFoundNull(
                        expect(directoryBlobDigest, Directory.parser(), executor, requestMetadata));
            return toCompletableFuture(fetcher.get());
          }
        };

    return toListenableFuture(directoryCache.get(directoryBlobDigest, getCallback));
  }

  @Override
  protected <T> ListenableFuture<T> expect(
      Digest digest, Parser<T> parser, Executor executor, RequestMetadata requestMetadata) {
    Context.CancellableContext withDeadline =
        Context.current().withDeadlineAfter(60, SECONDS, contextDeadlineScheduler);
    Context previousContext = withDeadline.attach();
    try {
      ListenableFuture<T> future = super.expect(digest, parser, executor, requestMetadata);
      future.addListener(() -> withDeadline.cancel(null), directExecutor());
      return future;
    } catch (RuntimeException e) {
      withDeadline.cancel(null);
      throw e;
    } finally {
      withDeadline.detach(previousContext);
    }
  }

  ListenableFuture<Command> expectCommand(
      Digest commandBlobDigest, RequestMetadata requestMetadata) {
    BiFunction<Digest, Executor, CompletableFuture<Command>> getCallback =
        new BiFunction<Digest, Executor, CompletableFuture<Command>>() {
          @Override
          public CompletableFuture<Command> apply(Digest digest, Executor executor) {
            Supplier<ListenableFuture<Command>> fetcher =
                () ->
                    notFoundNull(
                        expect(commandBlobDigest, Command.parser(), executor, requestMetadata));
            return toCompletableFuture(fetcher.get());
          }
        };

    return toListenableFuture(commandCache.get(commandBlobDigest, getCallback));
  }

  ListenableFuture<Action> expectAction(Digest actionBlobDigest, RequestMetadata requestMetadata) {
    BiFunction<Digest, Executor, CompletableFuture<Action>> getCallback =
        new BiFunction<Digest, Executor, CompletableFuture<Action>>() {
          @Override
          public CompletableFuture<Action> apply(Digest digest, Executor executor) {
            Supplier<ListenableFuture<Action>> fetcher =
                () ->
                    notFoundNull(
                        expect(actionBlobDigest, Action.parser(), executor, requestMetadata));
            return toCompletableFuture(fetcher.get());
          }
        };

    return toListenableFuture(digestToActionCache.get(actionBlobDigest, getCallback));
  }

  private void removeMalfunctioningWorker(String worker, Throwable t, String context) {
    try {
      if (backplane.removeWorker(worker, format("%s: %s", context, t.getMessage()))) {
        log.log(
            Level.WARNING,
            format("Removed worker '%s' during(%s) due to exception", worker, context),
            t);
      }
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }

    workerStubs.invalidate(worker);
  }

  @Override
  public Write getOperationStreamWrite(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream newOperationStreamInput(
      String name, long offset, RequestMetadata requestMetadata) {
    throw new UnsupportedOperationException();
  }

  private ListenableFuture<QueuedOperation> buildQueuedOperation(
      String operationName,
      Action action,
      ExecutorService service,
      RequestMetadata requestMetadata) {
    QueuedOperation.Builder queuedOperationBuilder = QueuedOperation.newBuilder().setAction(action);
    return transformQueuedOperation(
        operationName,
        action,
        action.getCommandDigest(),
        action.getInputRootDigest(),
        queuedOperationBuilder,
        service,
        requestMetadata);
  }

  private QueuedOperationMetadata buildQueuedOperationMetadata(
      ExecuteOperationMetadata executeOperationMetadata,
      RequestMetadata requestMetadata,
      QueuedOperation queuedOperation) {
    return QueuedOperationMetadata.newBuilder()
        .setExecuteOperationMetadata(
            executeOperationMetadata.toBuilder().setStage(ExecutionStage.Value.QUEUED))
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
      ExecutorService service,
      RequestMetadata requestMetadata) {
    return transform(
        allAsList(
            transform(
                expectCommand(commandDigest, requestMetadata),
                (command) -> {
                  log.log(
                      Level.FINER,
                      format("transformQueuedOperation(%s): fetched command", operationName));
                  if (command != null) {
                    queuedOperationBuilder.setCommand(command);
                  }
                  return queuedOperationBuilder;
                },
                service),
            transform(
                getTreeFuture(operationName, inputRootDigest, service, requestMetadata),
                queuedOperationBuilder::setTree,
                service)),
        (result) -> queuedOperationBuilder.setAction(action).build(),
        service);
  }

  protected Operation createOperation(ActionKey actionKey) {
    throw new UnsupportedOperationException();
  }

  private static final class QueuedOperationResult {
    public final QueueEntry entry;
    public final QueuedOperationMetadata metadata;

    QueuedOperationResult(QueueEntry entry, QueuedOperationMetadata metadata) {
      this.entry = entry;
      this.metadata = metadata;
    }
  }

  ExecuteOperationMetadata executeOperationMetadata(
      ExecuteEntry executeEntry, ExecutionStage.Value stage) {
    return ExecuteOperationMetadata.newBuilder()
        .setActionDigest(executeEntry.getActionDigest())
        .setStdoutStreamName(executeEntry.getStdoutStreamName())
        .setStderrStreamName(executeEntry.getStderrStreamName())
        .setStage(stage)
        .build();
  }

  private ListenableFuture<QueuedOperationResult> uploadQueuedOperation(
      QueuedOperation queuedOperation,
      ExecuteEntry executeEntry,
      ExecutorService service,
      Duration timeout)
      throws EntryLimitException {
    ByteString queuedOperationBlob = queuedOperation.toByteString();
    Digest queuedOperationDigest = getDigestUtil().compute(queuedOperationBlob);
    QueuedOperationMetadata metadata =
        QueuedOperationMetadata.newBuilder()
            .setExecuteOperationMetadata(
                executeOperationMetadata(executeEntry, ExecutionStage.Value.QUEUED))
            .setQueuedOperationDigest(queuedOperationDigest)
            .build();
    QueueEntry entry =
        QueueEntry.newBuilder()
            .setExecuteEntry(executeEntry)
            .setQueuedOperationDigest(queuedOperationDigest)
            .setPlatform(queuedOperation.getCommand().getPlatform())
            .build();
    return transform(
        writeBlobFuture(
            queuedOperationDigest, queuedOperationBlob, executeEntry.getRequestMetadata(), timeout),
        (committedSize) -> new QueuedOperationResult(entry, metadata),
        service);
  }

  private ListenableFuture<Long> writeBlobFuture(
      Digest digest, ByteString content, RequestMetadata requestMetadata, Duration timeout)
      throws EntryLimitException {
    checkState(digest.getSizeBytes() == content.size());
    SettableFuture<Long> writtenFuture = SettableFuture.create();
    Write write =
        getBlobWrite(Compressor.Value.IDENTITY, digest, UUID.randomUUID(), requestMetadata);
    addCallback(
        write.getFuture(),
        new FutureCallback<Long>() {
          @Override
          public void onSuccess(Long committedSize) {
            writtenFuture.set(committedSize);
          }

          @Override
          public void onFailure(Throwable t) {
            writtenFuture.setException(t);
          }
        },
        directExecutor());
    try (OutputStream out = write.getOutput(timeout.getSeconds(), SECONDS, () -> {})) {
      content.writeTo(out);
    } catch (IOException e) {
      // if the stream is complete already, we will have already set the future value
      writtenFuture.setException(e);
    }
    return writtenFuture;
  }

  private ListenableFuture<QueuedOperation> buildQueuedOperation(
      String operationName,
      Digest actionDigest,
      ExecutorService service,
      RequestMetadata requestMetadata) {
    return transformAsync(
        expectAction(actionDigest, requestMetadata),
        (action) -> {
          if (action == null) {
            return immediateFuture(QueuedOperation.getDefaultInstance());
          }
          return buildQueuedOperation(operationName, action, service, requestMetadata);
        },
        service);
  }

  @Override
  protected void validatePlatform(
      Platform platform, PreconditionFailure.Builder preconditionFailure) {
    int minCores = 0;
    int maxCores = -1;

    // check that the platform properties correspond to valid provisions for the OperationQeueue.
    // if the operation is eligible to be put anywhere in the OperationQueue, it passes validation.
    boolean validForOperationQueue =
        backplane.propertiesEligibleForQueue(platform.getPropertiesList());
    if (!validForOperationQueue) {
      preconditionFailure
          .addViolationsBuilder()
          .setType(VIOLATION_TYPE_INVALID)
          .setSubject(INVALID_PLATFORM)
          .setDescription(
              format(
                  "properties are not valid for queue eligibility: %s.  If you think your queue should still accept these poperties without them being specified in queue configuration, consider configuring the queue with `allow_unmatched: True`",
                  platform.getPropertiesList()));
    }

    for (Property property : platform.getPropertiesList()) {
      /* FIXME generalize with config */
      if (property.getName().equals(ExecutionProperties.MIN_CORES)
          || property.getName().equals(ExecutionProperties.MAX_CORES)) {
        try {
          int intValue = Integer.parseInt(property.getValue());
          if (intValue <= 0 || (maxCpu != 0 && intValue > maxCpu)) {
            preconditionFailure
                .addViolationsBuilder()
                .setType(VIOLATION_TYPE_INVALID)
                .setSubject(INVALID_PLATFORM)
                .setDescription(
                    format(
                        "property '%s' value was out of range: %d", property.getName(), intValue));
          }
          if (property.getName().equals(ExecutionProperties.MIN_CORES)) {
            minCores = intValue;
          } else {
            maxCores = intValue;
          }
        } catch (NumberFormatException e) {
          preconditionFailure
              .addViolationsBuilder()
              .setType(VIOLATION_TYPE_INVALID)
              .setSubject(INVALID_PLATFORM)
              .setDescription(
                  format(
                      "property '%s' value was not a valid integer: %s",
                      property.getName(), property.getValue()));
        }
      }
    }
    if (maxCores != -1 && minCores > 0 && maxCores < minCores) {
      preconditionFailure
          .addViolationsBuilder()
          .setType(VIOLATION_TYPE_INVALID)
          .setSubject(INVALID_PLATFORM)
          .setDescription(
              format(
                  "%s (%d) must be >= %s (%d)",
                  ExecutionProperties.MAX_CORES,
                  maxCores,
                  ExecutionProperties.MIN_CORES,
                  minCores));
    }
  }

  private boolean hasMaxActionTimeout() {
    return maxActionTimeout.getSeconds() > 0 || maxActionTimeout.getNanos() > 0;
  }

  @Override
  protected void validateAction(
      Action action,
      @Nullable Command command,
      Map<Digest, Directory> directoriesIndex,
      Consumer<Digest> onInputDigest,
      PreconditionFailure.Builder preconditionFailure) {
    if (action.hasTimeout() && hasMaxActionTimeout()) {
      Duration timeout = action.getTimeout();
      if (timeout.getSeconds() > maxActionTimeout.getSeconds()
          || (timeout.getSeconds() == maxActionTimeout.getSeconds()
              && timeout.getNanos() > maxActionTimeout.getNanos())) {
        preconditionFailure
            .addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(Durations.toString(timeout) + " > " + Durations.toString(maxActionTimeout))
            .setDescription(TIMEOUT_OUT_OF_BOUNDS);
      }
    }

    super.validateAction(action, command, directoriesIndex, onInputDigest, preconditionFailure);
  }

  private ListenableFuture<Void> validateAndRequeueOperation(
      Operation operation, QueueEntry queueEntry, Duration timeout) {
    ExecuteEntry executeEntry = queueEntry.getExecuteEntry();
    String operationName = executeEntry.getOperationName();
    checkState(operationName.equals(operation.getName()));
    RequestMetadata requestMetadata = executeEntry.getRequestMetadata();
    ListenableFuture<QueuedOperation> fetchQueuedOperationFuture =
        expect(
            queueEntry.getQueuedOperationDigest(),
            QueuedOperation.parser(),
            operationTransformService,
            requestMetadata);
    Digest actionDigest = executeEntry.getActionDigest();
    ListenableFuture<QueuedOperation> queuedOperationFuture =
        catchingAsync(
            fetchQueuedOperationFuture,
            Throwable.class,
            (e) ->
                buildQueuedOperation(
                    operation.getName(), actionDigest, operationTransformService, requestMetadata),
            directExecutor());
    PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
    ListenableFuture<QueuedOperation> validatedFuture =
        transformAsync(
            queuedOperationFuture,
            (queuedOperation) -> {
              /* sync, throws StatusException - must be serviced via non-OTS */
              validateQueuedOperationAndInputs(
                  actionDigest, queuedOperation, preconditionFailure, requestMetadata);
              return immediateFuture(queuedOperation);
            },
            operationTransformService);

    // this little fork ensures that a successfully fetched QueuedOperation
    // will not be reuploaded
    ListenableFuture<QueuedOperationResult> uploadedFuture =
        transformAsync(
            validatedFuture,
            (queuedOperation) ->
                catchingAsync(
                    transform(
                        fetchQueuedOperationFuture,
                        (fechedQueuedOperation) -> {
                          QueuedOperationMetadata metadata =
                              QueuedOperationMetadata.newBuilder()
                                  .setExecuteOperationMetadata(
                                      executeOperationMetadata(
                                          executeEntry, ExecutionStage.Value.QUEUED))
                                  .setQueuedOperationDigest(queueEntry.getQueuedOperationDigest())
                                  .setRequestMetadata(requestMetadata)
                                  .build();
                          return new QueuedOperationResult(queueEntry, metadata);
                        },
                        operationTransformService),
                    Throwable.class,
                    (e) ->
                        uploadQueuedOperation(
                            queuedOperation, executeEntry, operationTransformService, timeout),
                    operationTransformService),
            directExecutor());

    SettableFuture<Void> requeuedFuture = SettableFuture.create();
    addCallback(
        uploadedFuture,
        new FutureCallback<QueuedOperationResult>() {
          @Override
          public void onSuccess(QueuedOperationResult result) {
            Operation queueOperation =
                operation.toBuilder().setMetadata(Any.pack(result.metadata)).build();
            try {
              backplane.queue(result.entry, queueOperation);
              requeuedFuture.set(null);
            } catch (IOException e) {
              onFailure(e);
            }
          }

          @Override
          public void onFailure(Throwable t) {
            requeueFailureCounter.inc();
            log.log(Level.SEVERE, "failed to requeue: " + operationName, t);
            com.google.rpc.Status status = StatusProto.fromThrowable(t);
            if (status == null) {
              log.log(Level.SEVERE, "no rpc status from exception for " + operationName, t);
              status = asExecutionStatus(t);
            } else if (com.google.rpc.Code.forNumber(status.getCode())
                == com.google.rpc.Code.DEADLINE_EXCEEDED) {
              log.log(
                  Level.WARNING,
                  "an rpc status was thrown with DEADLINE_EXCEEDED for "
                      + operationName
                      + ", discarding it",
                  t);
              status =
                  com.google.rpc.Status.newBuilder()
                      .setCode(com.google.rpc.Code.UNAVAILABLE.getNumber())
                      .setMessage("SUPPRESSED DEADLINE_EXCEEDED: " + t.getMessage())
                      .build();
            }
            logFailedStatus(actionDigest, status);
            SettableFuture<Void> errorFuture = SettableFuture.create();
            errorOperationFuture(operation, requestMetadata, status, errorFuture);
            errorFuture.addListener(() -> requeuedFuture.set(null), operationTransformService);
          }
        },
        operationTransformService);
    return requeuedFuture;
  }

  String operationBlockedError(String operationName) {
    return String.format(NO_REQUEUE_BLOCKED_ERROR, operationName);
  }

  String tooManyRequeuesError(String operationName, int currentAttempt, int maxRequeueAttempts) {
    // If an operation fails from excessive requeue, show this error to the client.  Multiple
    // requeue failures are likely caused by another issue, however its helpful to show the requeue
    // amount to the user in case the attempt amount are improperly configured.
    return String.format(
        NO_REQUEUE_TOO_MANY_ERROR, operationName, currentAttempt, maxRequeueAttempts);
  }

  String operationMissingMessage(String operationName) {
    return String.format(NO_REQUEUE_MISSING_MESSAGE, operationName);
  }

  String operationCompleteMessage(String operationName) {
    return String.format(NO_REQUEUE_COMPLETE_MESSAGE, operationName);
  }

  void putFailedOperation(ExecuteEntry executeEntry, String errorMessage) {
    // Create a failed operation which will be reported back to the client.
    Operation.Builder failedOperation =
        Operation.newBuilder()
            .setName(executeEntry.getOperationName())
            .setDone(true)
            .setMetadata(
                Any.pack(executeOperationMetadata(executeEntry, ExecutionStage.Value.COMPLETED)));

    // put the operation back into the backplane with a failed precondition.
    putOperation(
        failedOperation
            .setResponse(Any.pack(denyActionResponse(executeEntry.getActionDigest(), errorMessage)))
            .build());
  }

  private boolean canOperationBeRequeued(
      QueueEntry queueEntry, ExecuteEntry executeEntry, Operation operation) throws IOException {
    String operationName = executeEntry.getOperationName();

    // Skip requeuing and fail the operation if its in a deny list.
    if (inDenyList(executeEntry.getRequestMetadata())) {
      String msg = operationBlockedError(operationName);
      requeueFailureCounter.inc();
      log.log(Level.WARNING, msg);
      putFailedOperation(executeEntry, msg);
      return false;
    }

    // Skip requeuing and fail the operation if its already been requeued too many times.
    if (queueEntry.getRequeueAttempts() > maxRequeueAttempts) {
      String msg =
          tooManyRequeuesError(operationName, queueEntry.getRequeueAttempts(), maxRequeueAttempts);
      requeueFailureCounter.inc();
      log.log(Level.WARNING, msg);
      putFailedOperation(executeEntry, msg);
      return false;
    }

    // Skip requeuing and fail the operation if we couldn't find it.
    // This would prevent us from being able to requeue it anyways.
    if (operation == null) {
      String msg = operationMissingMessage(operationName);
      requeueFailureCounter.inc();
      log.log(Level.WARNING, msg);
      backplane.deleteOperation(operationName); // signal watchers
      return false;
    }

    // Skip requeuing the operation if its already done.
    // Perhaps the operation was just completed by a worker.
    if (operation.getDone()) {
      String msg = operationCompleteMessage(operationName);
      log.log(Level.INFO, msg);
      backplane.completeOperation(operationName);
      return false;
    }

    return true;
  }

  @VisibleForTesting
  public ListenableFuture<Void> requeueOperation(QueueEntry queueEntry, Duration timeout) {
    ListenableFuture<Void> future;
    ExecuteEntry executeEntry = queueEntry.getExecuteEntry();
    Operation operation = getOperation(executeEntry.getOperationName());

    try {
      // check preconditions before trying to requeue.
      boolean canRequeue = canOperationBeRequeued(queueEntry, executeEntry, operation);
      if (!canRequeue) {
        return IMMEDIATE_VOID_FUTURE;
      }

      // Requeue the action as long as the result is not already cached.
      ActionKey actionKey = DigestUtil.asActionKey(executeEntry.getActionDigest());
      ListenableFuture<Boolean> cachedResultFuture;
      if (executeEntry.getSkipCacheLookup()) {
        cachedResultFuture = immediateFuture(false);
      } else {
        cachedResultFuture =
            checkCacheFuture(actionKey, operation, executeEntry.getRequestMetadata());
      }
      future =
          transformAsync(
              cachedResultFuture,
              (cachedResult) -> {
                if (cachedResult) {
                  return IMMEDIATE_VOID_FUTURE;
                }
                return validateAndRequeueOperation(operation, queueEntry, timeout);
              },
              operationTransformService);

    } catch (IOException | StatusRuntimeException e) {
      return immediateFailedFuture(e);
    }
    return future;
  }

  Watcher newActionResultWatcher(ActionKey actionKey, Watcher watcher) {
    return new Watcher() {
      boolean writeThrough = true; // default case for action, default here

      @Override
      public void observe(Operation operation) {
        if (operation != null && writeThrough) {
          ActionResult actionResult = getCacheableActionResult(operation);
          if (actionResult != null) {
            actionCache.readThrough(actionKey, actionResult);
          } else if (wasCompletelyExecuted(operation)) {
            // we want to avoid presenting any results for an action which
            // was not completely executed
            actionCache.invalidate(actionKey);
          }
        }
        if (operation != null && operation.getMetadata().is(Action.class)) {
          // post-action validation sequence, do not propagate to watcher
          try {
            writeThrough = !operation.getMetadata().unpack(Action.class).getDoNotCache();
          } catch (InvalidProtocolBufferException e) {
            // unlikely
          }
        } else {
          watcher.observe(operation);
        }
      }
    };
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
        return immediateFailedFuture(
            Status.RESOURCE_EXHAUSTED.withDescription("Too many jobs pending").asException());
      }

      String operationName = createOperationName(UUID.randomUUID().toString());

      executionSuccess.inc();
      log.log(
          Level.FINER,
          new StringBuilder()
              .append("ExecutionSuccess: ")
              .append(requestMetadata.getToolInvocationId())
              .append(" -> ")
              .append(operationName)
              .append(": ")
              .append(DigestUtil.toString(actionDigest))
              .toString());

      actionCache.invalidate(DigestUtil.asActionKey(actionDigest));
      if (!skipCacheLookup && recentCacheServedExecutions.getIfPresent(requestMetadata) != null) {
        log.log(
            Level.FINER,
            format("Operation %s will have skip_cache_lookup = true due to retry", operationName));
        skipCacheLookup = true;
      }

      String stdoutStreamName = operationName + "/streams/stdout";
      String stderrStreamName = operationName + "/streams/stderr";
      ExecuteEntry executeEntry =
          ExecuteEntry.newBuilder()
              .setOperationName(operationName)
              .setActionDigest(actionDigest)
              .setExecutionPolicy(executionPolicy)
              .setResultsCachePolicy(resultsCachePolicy)
              .setSkipCacheLookup(skipCacheLookup)
              .setRequestMetadata(requestMetadata)
              .setStdoutStreamName(stdoutStreamName)
              .setStderrStreamName(stderrStreamName)
              .setQueuedTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
              .build();
      ExecuteOperationMetadata metadata =
          ExecuteOperationMetadata.newBuilder()
              .setActionDigest(actionDigest)
              .setStdoutStreamName(stdoutStreamName)
              .setStderrStreamName(stderrStreamName)
              .build();
      Operation operation =
          Operation.newBuilder().setName(operationName).setMetadata(Any.pack(metadata)).build();
      try {
        watcher.observe(operation);
      } catch (Exception e) {
        return immediateFailedFuture(e);
      }

      if (inDenyList(requestMetadata)) {
        watcher.observe(
            operation
                .toBuilder()
                .setDone(true)
                .setResponse(Any.pack(denyActionResponse(actionDigest, BLOCK_LIST_ERROR)))
                .build());
        return immediateFuture(null);
      }
      backplane.prequeue(executeEntry, operation);
      return watchOperation(
          operation,
          newActionResultWatcher(DigestUtil.asActionKey(actionDigest), watcher),
          /* initial=*/ false);
    } catch (IOException e) {
      return immediateFailedFuture(e);
    }
  }

  private static ExecuteResponse denyActionResponse(Digest actionDigest, String description) {
    PreconditionFailure.Builder preconditionFailureBuilder = PreconditionFailure.newBuilder();
    preconditionFailureBuilder
        .addViolationsBuilder()
        .setType(VIOLATION_TYPE_MISSING)
        .setSubject("blobs/" + DigestUtil.toString(actionDigest))
        .setDescription(description);
    PreconditionFailure preconditionFailure = preconditionFailureBuilder.build();
    return ExecuteResponse.newBuilder()
        .setStatus(
            com.google.rpc.Status.newBuilder()
                .setCode(Code.FAILED_PRECONDITION.value())
                .setMessage(invalidActionVerboseMessage(actionDigest, preconditionFailure))
                .addDetails(Any.pack(preconditionFailure))
                .build())
        .build();
  }

  private <T> void errorOperationFuture(
      Operation operation,
      RequestMetadata requestMetadata,
      com.google.rpc.Status status,
      SettableFuture<T> errorFuture) {
    operationDeletionService.execute(
        new Runnable() {
          // we must make all efforts to delete this thing
          int attempt = 1;

          @Override
          public void run() {
            try {
              errorOperation(operation, requestMetadata, status);
              errorFuture.setException(StatusProto.toStatusException(status));
            } catch (StatusRuntimeException e) {
              if (attempt % 100 == 0) {
                log.log(
                    Level.SEVERE,
                    format(
                        "On attempt %d to cancel %s: %s",
                        attempt, operation.getName(), e.getLocalizedMessage()),
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

  private void deliverCachedActionResult(
      ActionResult actionResult,
      ActionKey actionKey,
      Operation operation,
      RequestMetadata requestMetadata)
      throws Exception {
    recentCacheServedExecutions.put(requestMetadata, true);

    ExecuteOperationMetadata completeMetadata =
        ExecuteOperationMetadata.newBuilder()
            .setActionDigest(actionKey.getDigest())
            .setStage(ExecutionStage.Value.COMPLETED)
            .build();

    Operation completedOperation =
        operation
            .toBuilder()
            .setDone(true)
            .setResponse(
                Any.pack(
                    ExecuteResponse.newBuilder()
                        .setResult(actionResult)
                        .setStatus(
                            com.google.rpc.Status.newBuilder().setCode(Code.OK.value()).build())
                        .setCachedResult(true)
                        .build()))
            .setMetadata(Any.pack(completeMetadata))
            .build();
    backplane.putOperation(completedOperation, completeMetadata.getStage());
  }

  private ListenableFuture<Boolean> checkCacheFuture(
      ActionKey actionKey, Operation operation, RequestMetadata requestMetadata) {
    ExecuteOperationMetadata metadata =
        ExecuteOperationMetadata.newBuilder()
            .setActionDigest(actionKey.getDigest())
            .setStage(ExecutionStage.Value.CACHE_CHECK)
            .build();
    try {
      backplane.putOperation(
          operation.toBuilder().setMetadata(Any.pack(metadata)).build(), metadata.getStage());
    } catch (IOException e) {
      return immediateFailedFuture(e);
    }

    Context.CancellableContext withDeadline =
        Context.current().withDeadlineAfter(60, SECONDS, contextDeadlineScheduler);
    try {
      return checkCacheFutureCancellable(actionKey, operation, requestMetadata, withDeadline);
    } catch (RuntimeException e) {
      withDeadline.cancel(null);
      throw e;
    }
  }

  private ListenableFuture<Boolean> checkCacheFutureCancellable(
      ActionKey actionKey,
      Operation operation,
      RequestMetadata requestMetadata,
      Context.CancellableContext ctx) {
    ListenableFuture<Boolean> checkCacheFuture =
        transformAsync(
            getActionResult(actionKey, requestMetadata),
            actionResult -> {
              try {
                return immediateFuture(
                    ctx.call(
                        () -> {
                          if (actionResult != null) {
                            deliverCachedActionResult(
                                actionResult, actionKey, operation, requestMetadata);
                          }
                          return actionResult != null;
                        }));
              } catch (Exception e) {
                return immediateFailedFuture(e);
              }
            },
            operationTransformService);
    checkCacheFuture.addListener(() -> ctx.cancel(null), operationTransformService);
    return catching(
        checkCacheFuture,
        Exception.class,
        (e) -> {
          log.log(Level.SEVERE, "error checking cache for " + operation.getName(), e);
          return false;
        },
        operationTransformService);
  }

  @VisibleForTesting
  public ListenableFuture<Void> queue(ExecuteEntry executeEntry, Poller poller, Duration timeout) {
    ExecuteOperationMetadata metadata =
        ExecuteOperationMetadata.newBuilder()
            .setActionDigest(executeEntry.getActionDigest())
            .setStdoutStreamName(executeEntry.getStdoutStreamName())
            .setStderrStreamName(executeEntry.getStderrStreamName())
            .build();
    Operation operation =
        Operation.newBuilder()
            .setName(executeEntry.getOperationName())
            .setMetadata(Any.pack(metadata))
            .build();
    Digest actionDigest = executeEntry.getActionDigest();
    ActionKey actionKey = DigestUtil.asActionKey(actionDigest);

    Stopwatch stopwatch = Stopwatch.createStarted();
    ListenableFuture<Boolean> cachedResultFuture;
    if (executeEntry.getSkipCacheLookup()) {
      cachedResultFuture = immediateFuture(false);
    } else {
      cachedResultFuture =
          checkCacheFuture(actionKey, operation, executeEntry.getRequestMetadata());
    }
    return transformAsync(
        cachedResultFuture,
        (cachedResult) -> {
          if (cachedResult) {
            poller.pause();
            long checkCacheUSecs = stopwatch.elapsed(MICROSECONDS);
            log.log(
                Level.FINER,
                format(
                    "ServerInstance(%s): checkCache(%s): %sus elapsed",
                    getName(), operation.getName(), checkCacheUSecs));
            return IMMEDIATE_VOID_FUTURE;
          }
          return transformAndQueue(executeEntry, poller, operation, stopwatch, timeout);
        },
        operationTransformService);
  }

  private ListenableFuture<Void> transformAndQueue(
      ExecuteEntry executeEntry,
      Poller poller,
      Operation operation,
      Stopwatch stopwatch,
      Duration timeout) {
    long checkCacheUSecs = stopwatch.elapsed(MICROSECONDS);
    ExecuteOperationMetadata metadata;
    try {
      metadata = operation.getMetadata().unpack(ExecuteOperationMetadata.class);
    } catch (InvalidProtocolBufferException e) {
      return immediateFailedFuture(e);
    }
    Digest actionDigest = metadata.getActionDigest();
    SettableFuture<Void> queueFuture = SettableFuture.create();
    log.log(
        Level.FINER,
        format(
            "ServerInstance(%s): queue(%s): fetching action %s",
            getName(), operation.getName(), actionDigest.getHash()));
    RequestMetadata requestMetadata = executeEntry.getRequestMetadata();
    ListenableFuture<Action> actionFuture =
        catchingAsync(
            transformAsync(
                expectAction(actionDigest, requestMetadata),
                (action) -> {
                  if (action == null) {
                    throw Status.NOT_FOUND.asException();
                  } else if (action.getDoNotCache()) {
                    // invalidate our action cache result as well as watcher owner
                    actionCache.invalidate(DigestUtil.asActionKey(actionDigest));
                    backplane.putOperation(
                        operation.toBuilder().setMetadata(Any.pack(action)).build(),
                        metadata.getStage());
                  }
                  return immediateFuture(action);
                },
                operationTransformService),
            StatusException.class,
            (e) -> {
              Status st = Status.fromThrowable(e);
              if (st.getCode() == Code.NOT_FOUND) {
                PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
                preconditionFailure
                    .addViolationsBuilder()
                    .setType(VIOLATION_TYPE_MISSING)
                    .setSubject("blobs/" + DigestUtil.toString(actionDigest))
                    .setDescription(MISSING_ACTION);
                checkPreconditionFailure(actionDigest, preconditionFailure.build());
              }
              throw st.asRuntimeException();
            },
            operationTransformService);
    QueuedOperation.Builder queuedOperationBuilder = QueuedOperation.newBuilder();
    ListenableFuture<ProfiledQueuedOperationMetadata.Builder> queuedFuture =
        transformAsync(
            actionFuture,
            (action) -> {
              log.log(
                  Level.FINER,
                  format(
                      "ServerInstance(%s): queue(%s): fetched action %s transforming queuedOperation",
                      getName(), operation.getName(), actionDigest.getHash()));
              Stopwatch transformStopwatch = Stopwatch.createStarted();
              return transform(
                  transformQueuedOperation(
                      operation.getName(),
                      action,
                      action.getCommandDigest(),
                      action.getInputRootDigest(),
                      queuedOperationBuilder,
                      operationTransformService,
                      requestMetadata),
                  (queuedOperation) ->
                      ProfiledQueuedOperationMetadata.newBuilder()
                          .setQueuedOperation(queuedOperation)
                          .setQueuedOperationMetadata(
                              buildQueuedOperationMetadata(
                                  metadata, requestMetadata, queuedOperation))
                          .setTransformedIn(
                              Durations.fromMicros(transformStopwatch.elapsed(MICROSECONDS))),
                  operationTransformService);
            },
            operationTransformService);
    ListenableFuture<ProfiledQueuedOperationMetadata.Builder> validatedFuture =
        transformAsync(
            queuedFuture,
            (profiledQueuedMetadata) -> {
              log.log(
                  Level.FINER,
                  format(
                      "ServerInstance(%s): queue(%s): queuedOperation %s transformed, validating",
                      getName(),
                      operation.getName(),
                      DigestUtil.toString(
                          profiledQueuedMetadata
                              .getQueuedOperationMetadata()
                              .getQueuedOperationDigest())));
              long startValidateUSecs = stopwatch.elapsed(MICROSECONDS);
              /* sync, throws StatusException */
              validateQueuedOperation(actionDigest, profiledQueuedMetadata.getQueuedOperation());
              return immediateFuture(
                  profiledQueuedMetadata.setValidatedIn(
                      Durations.fromMicros(stopwatch.elapsed(MICROSECONDS) - startValidateUSecs)));
            },
            operationTransformService);
    ListenableFuture<ProfiledQueuedOperationMetadata> queuedOperationCommittedFuture =
        transformAsync(
            validatedFuture,
            (profiledQueuedMetadata) -> {
              log.log(
                  Level.FINER,
                  format(
                      "ServerInstance(%s): queue(%s): queuedOperation %s validated, uploading",
                      getName(),
                      operation.getName(),
                      DigestUtil.toString(
                          profiledQueuedMetadata
                              .getQueuedOperationMetadata()
                              .getQueuedOperationDigest())));
              ByteString queuedOperationBlob =
                  profiledQueuedMetadata.getQueuedOperation().toByteString();
              Digest queuedOperationDigest =
                  profiledQueuedMetadata.getQueuedOperationMetadata().getQueuedOperationDigest();
              long startUploadUSecs = stopwatch.elapsed(MICROSECONDS);
              return transform(
                  writeBlobFuture(
                      queuedOperationDigest, queuedOperationBlob, requestMetadata, timeout),
                  (committedSize) ->
                      profiledQueuedMetadata
                          .setUploadedIn(
                              Durations.fromMicros(
                                  stopwatch.elapsed(MICROSECONDS) - startUploadUSecs))
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
            Operation queueOperation =
                operation.toBuilder().setMetadata(Any.pack(queuedOperationMetadata)).build();
            QueueEntry queueEntry =
                QueueEntry.newBuilder()
                    .setExecuteEntry(executeEntry)
                    .setQueuedOperationDigest(queuedOperationMetadata.getQueuedOperationDigest())
                    .setPlatform(
                        profiledQueuedMetadata.getQueuedOperation().getCommand().getPlatform())
                    .build();
            try {
              ensureCanQueue(stopwatch);
              long startQueueUSecs = stopwatch.elapsed(MICROSECONDS);
              poller.pause();
              backplane.queue(queueEntry, queueOperation);
              long elapsedUSecs = stopwatch.elapsed(MICROSECONDS);
              long queueUSecs = elapsedUSecs - startQueueUSecs;
              log.log(
                  Level.FINER,
                  format(
                      "ServerInstance(%s): queue(%s): %dus checkCache, %dus transform, %dus validate, %dus upload, %dus queue, %dus elapsed",
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
              log.log(Level.SEVERE, "no rpc status from exception for " + operation.getName(), t);
              status = asExecutionStatus(t);
            } else if (com.google.rpc.Code.forNumber(status.getCode())
                == com.google.rpc.Code.DEADLINE_EXCEEDED) {
              log.log(
                  Level.WARNING,
                  "an rpc status was thrown with DEADLINE_EXCEEDED for "
                      + operation.getName()
                      + ", discarding it",
                  t);
              status =
                  com.google.rpc.Status.newBuilder()
                      .setCode(com.google.rpc.Code.UNAVAILABLE.getNumber())
                      .setMessage("SUPPRESSED DEADLINE_EXCEEDED: " + t.getMessage())
                      .build();
            }
            logFailedStatus(actionDigest, status);
            errorOperationFuture(operation, requestMetadata, status, queueFuture);
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
  public BackplaneStatus backplaneStatus() {
    try {
      return backplane.backplaneStatus();
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  @Override
  public boolean putOperation(Operation operation) {
    if (isErrored(operation)) {
      try {
        return backplane.putOperation(operation, ExecutionStage.Value.COMPLETED);
      } catch (IOException e) {
        throw Status.fromThrowable(e).asRuntimeException();
      }
    }
    throw new UnsupportedOperationException();
  }

  protected boolean matchOperation(Operation operation) {
    throw new UnsupportedOperationException();
  }

  protected void enqueueOperation(Operation operation) {
    throw new UnsupportedOperationException();
  }

  protected Object operationLock() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean pollOperation(String operationName, ExecutionStage.Value stage) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected int getListOperationsDefaultPageSize() {
    return 1024;
  }

  @Override
  protected int getListOperationsMaxPageSize() {
    return 1024;
  }

  @Override
  protected TokenizableIterator<Operation> createOperationsIterator(String pageToken) {
    Iterator<Operation> iter;
    iter =
        Iterables.transform(
                backplane.getOperations(),
                (operationName) -> {
                  try {
                    return backplane.getOperation(operationName);
                  } catch (IOException e) {
                    throw Status.fromThrowable(e).asRuntimeException();
                  }
                })
            .iterator();
    OperationIteratorToken token;
    if (!pageToken.isEmpty()) {
      try {
        token = OperationIteratorToken.parseFrom(BaseEncoding.base64().decode(pageToken));
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
        nextToken =
            OperationIteratorToken.newBuilder().setOperationName(operation.getName()).build();
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

  ListenableFuture<Void> watchOperation(Operation operation, Watcher watcher, boolean initial) {
    if (initial) {
      try {
        watcher.observe(stripOperation(operation));
      } catch (Exception e) {
        return immediateFailedFuture(e);
      }
    }
    if (operation == null || operation.getDone()) {
      return immediateFuture(null);
    }

    return backplane.watchOperation(operation.getName(), watcher);
  }

  @Override
  public ListenableFuture<Void> watchOperation(String operationName, Watcher watcher) {
    Operation operation = getOperation(operationName);
    if (operation == null) {
      return immediateFailedFuture(
          Status.NOT_FOUND
              .withDescription(String.format("Operation not found: %s", operationName))
              .asException());
    }
    return watchOperation(operation, watcher, /* initial=*/ true);
  }

  private static Operation stripOperation(Operation operation) {
    ExecuteOperationMetadata metadata = expectExecuteOperationMetadata(operation);
    if (metadata == null) {
      metadata = ExecuteOperationMetadata.getDefaultInstance();
    }
    return operation.toBuilder().setMetadata(Any.pack(metadata)).build();
  }

  private static Operation stripQueuedOperation(Operation operation) {
    if (operation.getMetadata().is(QueuedOperationMetadata.class)) {
      operation =
          operation
              .toBuilder()
              .setMetadata(Any.pack(expectExecuteOperationMetadata(operation)))
              .build();
    }
    return operation;
  }

  @Override
  protected Logger getLogger() {
    return log;
  }

  @Override
  public GetClientStartTimeResult getClientStartTime(GetClientStartTimeRequest request) {
    try {
      return backplane.getClientStartTime(request);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  @Override
  public CasIndexResults reindexCas() {
    try {
      return backplane.reindexCas();
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  @Override
  public FindOperationsResults findEnrichedOperations(String filterPredicate) {
    try {
      return backplane.findEnrichedOperations(this, filterPredicate);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  @Override
  public EnrichedOperation findEnrichedOperation(String operationId) {
    try {
      return backplane.findEnrichedOperation(this, operationId);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  @Override
  public List<Operation> findOperations(String filterPredicate) {
    try {
      return backplane.findOperations(filterPredicate);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  public Set<String> findOperationsByInvocationId(String invocationId) {
    try {
      return backplane.findOperationsByInvocationId(invocationId);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  public Iterable<Map.Entry<String, String>> getOperations(Set<String> invocationIds) {
    try {
      return backplane.getOperations(invocationIds);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  @Override
  public void deregisterWorker(String workerName) {
    try {
      backplane.deregisterWorker(workerName);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  @VisibleForTesting
  BuildfarmConfigs getBuildFarmConfigs() {
    return configs;
  }

  private boolean inDenyList(RequestMetadata requestMetadata) throws IOException {
    if (!useDenyList) {
      return false;
    }
    return backplane.isBlacklisted(requestMetadata);
  }

  @Override
  protected CacheCapabilities getCacheCapabilities() {
    SymlinkAbsolutePathStrategy.Value symlinkAbsolutePathStrategy =
        configs.isAllowSymlinkTargetAbsolute()
            ? SymlinkAbsolutePathStrategy.Value.ALLOWED
            : SymlinkAbsolutePathStrategy.Value.DISALLOWED;
    return super.getCacheCapabilities()
        .toBuilder()
        .setSymlinkAbsolutePathStrategy(symlinkAbsolutePathStrategy)
        .build();
  }
}
