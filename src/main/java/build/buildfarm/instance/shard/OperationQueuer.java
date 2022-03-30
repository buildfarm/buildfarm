// Copyright 2022 The Bazel Authors. All rights reserved.
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

import build.bazel.remote.execution.v2.Platform;
import build.buildfarm.common.StringVisitor;
import build.buildfarm.common.redis.BalancedRedisQueue;
import build.buildfarm.common.redis.ProvisionedRedisQueue;
import build.buildfarm.v1test.OperationQueueStatus;
import build.buildfarm.v1test.QueueStatus;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.SetMultimap;
import java.util.ArrayList;
import java.util.List;
import redis.clients.jedis.JedisCluster;
import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
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
import build.buildfarm.backplane.Backplane;
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
import build.buildfarm.common.grpc.UniformDelegateServerCallStreamObserver;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.MatchListener;
import build.buildfarm.instance.server.AbstractServerInstance;
import static build.buildfarm.instance.server.AbstractServerInstance.MISSING_ACTION;
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
import build.buildfarm.v1test.ShardInstanceConfig;
import build.buildfarm.v1test.Tree;
import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
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
import javax.annotation.Nullable;
import javax.naming.ConfigurationException;
import java.util.function.BiFunction;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.String.format;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.catching;
import static com.google.common.util.concurrent.Futures.catchingAsync;
import static build.buildfarm.common.Actions.asExecutionStatus;
import static build.buildfarm.common.Actions.checkPreconditionFailure;
import static build.buildfarm.common.Actions.invalidActionVerboseMessage;
import static build.buildfarm.common.Errors.VIOLATION_TYPE_INVALID;
import static build.buildfarm.common.Errors.VIOLATION_TYPE_MISSING;
import build.buildfarm.v1test.CompletedOperationMetadata;
import build.buildfarm.v1test.ExecutingOperationMetadata;


public class OperationQueuer {
    
    private static final Logger logger = Logger.getLogger(OperationQueuer.class.getName());
    
    private static final BlockingQueue transformTokensQueue = new LinkedBlockingQueue(256);
    
    public static Thread createThread(Backplane backplane, Duration queueTimeout, ListeningExecutorService operationTransformService, ScheduledExecutorService contextDeadlineScheduler, String name, 
                                      BiFunction<ActionKey, RequestMetadata,ListenableFuture<ActionResult>> getActionResult, Cache<RequestMetadata, Boolean> recentCacheServedExecutions,
                                      ReadThroughActionCache readThroughActionCache, Writes writes, ExecutorService operationDeletionService){
        return           new Thread(
              new Runnable() {
                final Stopwatch stopwatch = Stopwatch.createUnstarted();

                ListenableFuture<Void> iterate() throws IOException, InterruptedException {
                  ensureCanQueue(backplane,stopwatch); // wait for transition to canQueue state
                  long canQueueUSecs = stopwatch.elapsed(MICROSECONDS);
                  stopwatch.stop();
                  ExecuteEntry executeEntry = backplane.deprequeueOperation();
                  stopwatch.start();
                  if (executeEntry == null) {
                    logger.log(Level.SEVERE, "OperationQueuer: Got null from deprequeue...");
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
                            logger.log(
                                Level.SEVERE,
                                format("error polling %s for queuing", operationName),
                                e);
                          // mostly ignore, we will be stopped at some point later
                        }
                        return true;
                      },
                      () -> {},
                      Deadline.after(5, MINUTES));
                  try {
                    logger.log(Level.FINE, "queueing " + operationName);
                    ListenableFuture<Void> queueFuture = queue(backplane,executeEntry, poller, queueTimeout, name, operationTransformService, contextDeadlineScheduler, getActionResult, recentCacheServedExecutions, readThroughActionCache, writes, operationDeletionService);
                    addCallback(
                        queueFuture,
                        new FutureCallback<Void>() {
                          @Override
                          public void onSuccess(Void result) {
                            logger.log(Level.FINE, "successfully queued " + operationName);
                            // nothing
                          }

                          @Override
                          public void onFailure(Throwable t) {
                            logger.log(Level.SEVERE, "error queueing " + operationName, t);
                          }
                        },
                        operationTransformService);
                    long operationTransformDispatchUSecs =
                        stopwatch.elapsed(MICROSECONDS) - canQueueUSecs;
                    logger.log(
                        Level.FINE,
                        format(
                            "OperationQueuer: Dispatched To Transform %s: %dus in canQueue, %dus in transform dispatch",
                            operationName, canQueueUSecs, operationTransformDispatchUSecs));
                    return queueFuture;
                  } catch (Throwable t) {
                    poller.pause();
                    logger.log(Level.SEVERE, "error queueing " + operationName, t);
                    return immediateFuture(null);
                  }
                }

                @Override
                public void run() {
                  logger.log(Level.FINE, "OperationQueuer: Running");
                  try {
                    for (; ; ) {
                      transformTokensQueue.put(new Object());
                      stopwatch.start();
                      try {
                        iterate()
                            .addListener(
                                () -> {
                                  try {
                                    transformTokensQueue.take();
                                  } catch (InterruptedException e) {
                                    logger.log(
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
                  } catch (InterruptedException e) {
                    return;
                  } catch (Exception t) {
                    logger.log(
                        Level.SEVERE, "OperationQueuer: fatal exception during iteration", t);
                  } finally {
                    logger.log(Level.FINE, "OperationQueuer: Exiting");
                  }
                  try {
                  } catch (InterruptedException e) {
                    logger.log(Level.SEVERE, "interrupted while stopping instance " + name, e);
                  }
                }
              });
    }
    
    
      private static void ensureCanQueue(Backplane backplane, Stopwatch stopwatch) throws IOException, InterruptedException {
    while (!backplane.canQueue()) {
      stopwatch.stop();
      TimeUnit.MILLISECONDS.sleep(100);
      stopwatch.start();
    }
  }
  
  
  @VisibleForTesting
  public static ListenableFuture<Void> queue(Backplane backplane, ExecuteEntry executeEntry, Poller poller, Duration timeout, String name, ListeningExecutorService operationTransformService, ScheduledExecutorService contextDeadlineScheduler, BiFunction<ActionKey,
                                             RequestMetadata,ListenableFuture<ActionResult>> getActionResult, Cache<RequestMetadata, Boolean> recentCacheServedExecutions, ReadThroughActionCache readThroughActionCache, Writes writes, ExecutorService operationDeletionService) {
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
          checkCacheFuture(backplane,actionKey, operation, executeEntry.getRequestMetadata(), operationTransformService, contextDeadlineScheduler, getActionResult,recentCacheServedExecutions);
    }
    return transformAsync(
        cachedResultFuture,
        (cachedResult) -> {
          if (cachedResult) {
            poller.pause();
            long checkCacheUSecs = stopwatch.elapsed(MICROSECONDS);
            logger.log(
                Level.FINE,
                format(
                    "ShardInstance(%s): checkCache(%s): %sus elapsed",
                    name, operation.getName(), checkCacheUSecs));
            return Futures.immediateFuture(null);
          }
          return transformAndQueue(backplane,name, executeEntry, poller, operation, stopwatch, timeout, operationTransformService, readThroughActionCache,writes, operationDeletionService);
        },
        operationTransformService);
  }
  
  
  private static ListenableFuture<Boolean> checkCacheFuture(Backplane backplane, 
      ActionKey actionKey, Operation operation, RequestMetadata requestMetadata, ListeningExecutorService operationTransformService, ScheduledExecutorService contextDeadlineScheduler,
       BiFunction<ActionKey, RequestMetadata,ListenableFuture<ActionResult>> getActionResult, Cache<RequestMetadata, Boolean> recentCacheServedExecutions) {
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
      return checkCacheFutureCancellable(backplane,actionKey, operation, requestMetadata, withDeadline,operationTransformService,getActionResult,recentCacheServedExecutions);
    } catch (RuntimeException e) {
      withDeadline.cancel(null);
      throw e;
    }
  }

  private static ListenableFuture<Boolean> checkCacheFutureCancellable(Backplane backplane, 
      ActionKey actionKey,
      Operation operation,
      RequestMetadata requestMetadata,
      Context.CancellableContext ctx,
      ListeningExecutorService operationTransformService,
      BiFunction<ActionKey, RequestMetadata,ListenableFuture<ActionResult>> getActionResult,
      Cache<RequestMetadata, Boolean> recentCacheServedExecutions) {
    ListenableFuture<Boolean> checkCacheFuture =
        transformAsync(
            getActionResult.apply(actionKey, requestMetadata),
            actionResult -> {
              try {
                return immediateFuture(
                    ctx.call(
                        () -> {
                          if (actionResult != null) {
                            deliverCachedActionResult(
                                actionResult, actionKey, operation, requestMetadata, recentCacheServedExecutions);
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
          logger.log(Level.SEVERE, "error checking cache for " + operation.getName(), e);
          return false;
        },
        operationTransformService);
  }
  
  private static ListenableFuture<Void> transformAndQueue(Backplane backplane, 
                                                          String name,
      ExecuteEntry executeEntry,
      Poller poller,
      Operation operation,
      Stopwatch stopwatch,
      Duration timeout,
      ListeningExecutorService operationTransformService,
      ReadThroughActionCache readThroughActionCache,
      Writes writes,
      ExecutorService operationDeletionService) {
    long checkCacheUSecs = stopwatch.elapsed(MICROSECONDS);
    ExecuteOperationMetadata metadata;
    try {
      metadata = operation.getMetadata().unpack(ExecuteOperationMetadata.class);
    } catch (InvalidProtocolBufferException e) {
      return immediateFailedFuture(e);
    }
    Digest actionDigest = metadata.getActionDigest();
    SettableFuture<Void> queueFuture = SettableFuture.create();
    logger.log(
        Level.FINE,
        format(
            "ShardInstance(%s): queue(%s): fetching action %s",
            name, operation.getName(), actionDigest.getHash()));
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
                    readThroughActionCache.invalidate(DigestUtil.asActionKey(actionDigest));
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
              logger.log(
                  Level.FINE,
                  format(
                      "ShardInstance(%s): queue(%s): fetched action %s transforming queuedOperation",
                      name, operation.getName(), actionDigest.getHash()));
              Stopwatch transformStopwatch = Stopwatch.createStarted();
              return transform(
                  transformQueuedOperation(
                      backplane,
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
              logger.log(
                  Level.FINE,
                  format(
                      "ShardInstance(%s): queue(%s): queuedOperation %s transformed, validating",
                      name,
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
              logger.log(
                  Level.FINE,
                  format(
                      "ShardInstance(%s): queue(%s): queuedOperation %s validated, uploading",
                      name,
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
                      writes,queuedOperationDigest, queuedOperationBlob, requestMetadata, timeout),
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
              ensureCanQueue(backplane,stopwatch);
              long startQueueUSecs = stopwatch.elapsed(MICROSECONDS);
              poller.pause();
              backplane.queue(queueEntry, queueOperation);
              long elapsedUSecs = stopwatch.elapsed(MICROSECONDS);
              long queueUSecs = elapsedUSecs - startQueueUSecs;
              logger.log(
                  Level.FINE,
                  format(
                      "ShardInstance(%s): queue(%s): %dus checkCache, %dus transform, %dus validate, %dus upload, %dus queue, %dus elapsed",
                      name,
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
              logger.log(
                  Level.SEVERE, "no rpc status from exception for " + operation.getName(), t);
              status = asExecutionStatus(t);
            } else if (com.google.rpc.Code.forNumber(status.getCode())
                == com.google.rpc.Code.DEADLINE_EXCEEDED) {
              logger.log(
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
            AbstractServerInstance.logFailedStatus(actionDigest, status,logger);
            errorOperationFuture(backplane,operation, requestMetadata, status, queueFuture, operationDeletionService);
          }
        },
        operationTransformService);
    return queueFuture;
  }
  
  private static <T> void errorOperationFuture(
      Backplane backplane,
      Operation operation,
      RequestMetadata requestMetadata,
      com.google.rpc.Status status,
      SettableFuture<T> errorFuture,
      ExecutorService operationDeletionService) {
    operationDeletionService.execute(
        new Runnable() {
          // we must make all efforts to delete this thing
          int attempt = 1;

          @Override
          public void run() {
            try {
              errorOperation(backplane,operation, requestMetadata, status);
              errorFuture.setException(StatusProto.toStatusException(status));
            } catch (StatusRuntimeException e) {
              if (attempt % 100 == 0) {
                logger.log(
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
  
  private static void errorOperation(
      Backplane backplane, Operation operation, RequestMetadata requestMetadata, com.google.rpc.Status status)
      throws InterruptedException {
    if (operation.getDone()) {
      throw new IllegalStateException("Trying to error already completed operation [" + operation.getName() + "]");
    }
    ExecuteOperationMetadata metadata = expectExecuteOperationMetadata(operation);
    if (metadata == null) {
      metadata = ExecuteOperationMetadata.getDefaultInstance();
    }
    CompletedOperationMetadata completedMetadata =
        CompletedOperationMetadata.newBuilder()
            .setExecuteOperationMetadata(
                metadata.toBuilder().setStage(ExecutionStage.Value.COMPLETED).build())
            .setRequestMetadata(requestMetadata)
            .build();
    putOperation(
        backplane,
        operation
            .toBuilder()
            .setDone(true)
            .setMetadata(Any.pack(completedMetadata))
            .setResponse(Any.pack(ExecuteResponse.newBuilder().setStatus(status).build()))
            .build());
  }
  
  private static ExecuteOperationMetadata expectExecuteOperationMetadata(Operation operation) {
    String name = operation.getName();
    Any metadata = operation.getMetadata();
    QueuedOperationMetadata queuedOperationMetadata = maybeQueuedOperationMetadata(name, metadata);
    if (queuedOperationMetadata != null) {
      return queuedOperationMetadata.getExecuteOperationMetadata();
    }
    ExecutingOperationMetadata executingOperationMetadata =
        maybeExecutingOperationMetadata(name, metadata);
    if (executingOperationMetadata != null) {
      return executingOperationMetadata.getExecuteOperationMetadata();
    }
    CompletedOperationMetadata completedOperationMetadata =
        maybeCompletedOperationMetadata(name, metadata);
    if (completedOperationMetadata != null) {
      return completedOperationMetadata.getExecuteOperationMetadata();
    }
    try {
      return operation.getMetadata().unpack(ExecuteOperationMetadata.class);
    } catch (InvalidProtocolBufferException e) {
      logger.log(
          Level.SEVERE, format("invalid execute operation metadata %s", operation.getName()), e);
    }
    return null;
  }
  
  private static boolean putOperation(Backplane backplane, Operation operation) {
    if (isErrored(operation)) {
      try {
        return backplane.putOperation(operation, ExecutionStage.Value.COMPLETED);
      } catch (IOException e) {
        throw Status.fromThrowable(e).asRuntimeException();
      }
    }
    throw new UnsupportedOperationException();
  }
  
  private static QueuedOperationMetadata maybeQueuedOperationMetadata(String name, Any metadata) {
    if (metadata.is(QueuedOperationMetadata.class)) {
      try {
        return metadata.unpack(QueuedOperationMetadata.class);
      } catch (InvalidProtocolBufferException e) {
        logger.log(Level.SEVERE, format("invalid executing operation metadata %s", name), e);
      }
    }
    return null;
  }
  
  private static ExecutingOperationMetadata maybeExecutingOperationMetadata(
      String name, Any metadata) {
    if (metadata.is(ExecutingOperationMetadata.class)) {
      try {
        return metadata.unpack(ExecutingOperationMetadata.class);
      } catch (InvalidProtocolBufferException e) {
        logger.log(Level.SEVERE, format("invalid executing operation metadata %s", name), e);
      }
    }
    return null;
  }

  private static CompletedOperationMetadata maybeCompletedOperationMetadata(
      String name, Any metadata) {
    if (metadata.is(CompletedOperationMetadata.class)) {
      try {
        return metadata.unpack(CompletedOperationMetadata.class);
      } catch (InvalidProtocolBufferException e) {
        logger.log(Level.SEVERE, format("invalid completed operation metadata %s", name), e);
      }
    }
    return null;
  }
  
  private static boolean isErrored(Operation operation) {
    return operation.getDone()
        && operation.getResultCase() == Operation.ResultCase.RESPONSE
        && operation.getResponse().is(ExecuteResponse.class)
        && expectExecuteResponse(operation).getStatus().getCode() != Code.OK.getNumber();
  }
  
  
  private static ListenableFuture<Tree> getTreeFuture(
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
  
  private static ExecuteResponse expectExecuteResponse(Operation operation) {
    try {
      return operation.getResponse().unpack(ExecuteResponse.class);
    } catch (InvalidProtocolBufferException e) {
      return null;
    }
  }
  
  private static ListenableFuture<QueuedOperation> transformQueuedOperation(
      Backplane backplane,
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
                  logger.log(
                      Level.FINE,
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
  
  private static ListenableFuture<Long> writeBlobFuture(Writes writes,
      Digest digest, ByteString content, RequestMetadata requestMetadata, Duration timeout)
      throws EntryLimitException {
    checkState(digest.getSizeBytes() == content.size());
    SettableFuture<Long> writtenFuture = SettableFuture.create();
    Write write = getBlobWrite(backplane, writes,digest, UUID.randomUUID(), requestMetadata);
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
  
  private static Write getBlobWrite(Backplane backplane, Writes writes, Digest digest, UUID uuid, RequestMetadata requestMetadata)
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
    return writes.get(digest, uuid, requestMetadata);
  }
  
  
  private static void deliverCachedActionResult(
      ActionResult actionResult,
      ActionKey actionKey,
      Operation operation,
      RequestMetadata requestMetadata,
      Cache<RequestMetadata, Boolean> recentCacheServedExecutions)
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
  
  private static void validateQueuedOperation(Digest actionDigest, QueuedOperation queuedOperation)
      throws StatusException {
    PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
    validateAction(
        queuedOperation.getAction(),
        queuedOperation.hasCommand() ? queuedOperation.getCommand() : null,
        DigestUtil.proxyDirectoriesIndex(queuedOperation.getTree().getDirectoriesMap()),
        digest -> {},
        preconditionFailure);
    checkPreconditionFailure(actionDigest, preconditionFailure.build());
  }
  
  
    private static ListenableFuture<Action> expectAction(Digest actionBlobDigest, RequestMetadata requestMetadata) {
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

  private static QueuedOperationMetadata buildQueuedOperationMetadata(
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
    
    
}