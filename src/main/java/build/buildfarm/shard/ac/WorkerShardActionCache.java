// Copyright 2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.shard.ac;

import static build.buildfarm.shard.Util.SHARD_IS_RETRIABLE;
import static build.buildfarm.shard.Util.removeMalfunctioningWorker;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.catching;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static net.javacrumbs.futureconverter.java8guava.FutureConverter.toCompletableFuture;
import static net.javacrumbs.futureconverter.java8guava.FutureConverter.toListenableFuture;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.ac.ActionCache;
import build.buildfarm.backplane.Backplane;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.grpc.TracingMetadataUtils;
import build.buildfarm.instance.Instance;
import build.buildfarm.shard.Shard;
import build.buildfarm.shard.WorkerStubs;
import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.Status.Code;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

public class WorkerShardActionCache implements ActionCache {
  private final Backplane backplane;
  private final LoadingCache<String, ? extends Instance> workerStubs;
  private final Random rand;
  private final ListeningExecutorService service;
  private final AsyncLoadingCache<ActionKey, ActionResult> actionResultCache;

  public WorkerShardActionCache(
      long maxLocalCacheSize,
      Backplane backplane,
      LoadingCache<String, ? extends Instance> workerStubs,
      Random rand,
      ListeningExecutorService service) {
    this.backplane = backplane;
    this.workerStubs = workerStubs;
    this.rand = rand;
    this.service = service;

    AsyncCacheLoader<ActionKey, ActionResult> loader =
        (actionKey, loaderExecutor) ->
            toCompletableFuture(
                catching(
                    getQueryingEachWorker(actionKey, TracingMetadataUtils.fromCurrentContext()),
                    IOException.class,
                    e -> {
                      throw Status.fromThrowable(e).asRuntimeException();
                    },
                    loaderExecutor));

    actionResultCache =
        Caffeine.newBuilder().maximumSize(maxLocalCacheSize).executor(service).buildAsync(loader);
  }

  @Override
  public ListenableFuture<ActionResult> get(ActionKey actionKey) {
    return catching(
        toListenableFuture(actionResultCache.get(actionKey)),
        InvalidCacheLoadException.class,
        e -> null,
        directExecutor());
  }

  private ListenableFuture<ActionResult> queryBackplaneThrough(ActionKey actionKey) {
    ListenableFuture<ActionResult> resultFuture =
        service.submit(() -> backplane.getActionResult(actionKey));
    addCallback(
        resultFuture,
        new FutureCallback<>() {
          @Override
          public void onSuccess(ActionResult result) {
            if (result != null) {
              try {
                put(actionKey, result);
              } catch (IOException | InterruptedException e) {
                // ignore read through write back issues
              }
            }
          }

          @Override
          public void onFailure(Throwable t) {
            // ignore
          }
        },
        service);
    return resultFuture;
  }

  private ListenableFuture<ActionResult> getQueryingEachWorker(
      ActionKey actionKey, RequestMetadata requestMetadata) {
    Deque<String> workers;
    try {
      List<String> workersList =
          backplane.getStorageWorkers().stream()
              .map(w -> w.getEndpoint())
              .collect(Collectors.toList());
      Collections.shuffle(workersList, rand);
      workers = new ArrayDeque<>(workersList);
    } catch (IOException e) {
      return immediateFailedFuture(Status.fromThrowable(e).asException());
    }
    if (workers.isEmpty()) {
      return queryBackplaneThrough(actionKey);
    }

    SettableFuture<ActionResult> resultFuture = SettableFuture.create();
    getOnWorker(
        actionKey,
        workers,
        resultFuture,
        requestMetadata,
        Context.current().fixedContextExecutor(service));
    return resultFuture;
  }

  private void getOnWorker(
      ActionKey actionKey,
      Deque<String> workers,
      SettableFuture<ActionResult> resultFuture,
      RequestMetadata requestMetadata,
      Executor executor) {
    String worker = workers.removeFirst();
    ListenableFuture<ActionResult> workerGetActionResultFuture =
        WorkerStubs.get(workerStubs, worker).getActionResult(actionKey, requestMetadata);

    addCallback(
        workerGetActionResultFuture,
        new FutureCallback<>() {
          @Override
          public void onSuccess(ActionResult result) {
            resultFuture.set(result);
          }

          @Override
          public void onFailure(Throwable t) {
            Status status = Status.fromThrowable(t);
            // NOT_FOUND is normal, expected as we move through the workers
            if (status.getCode() != Code.NOT_FOUND) {
              if (status.getCode() == Code.UNAVAILABLE || status.getCode() == Code.UNIMPLEMENTED) {
                removeMalfunctioningWorker(
                    backplane,
                    worker,
                    t,
                    "getActionResult(" + DigestUtil.toString(actionKey.getDigest()) + ")",
                    workerStubs);
              } else if (status.getCode() == Code.DEADLINE_EXCEEDED) {
                resultFuture.setException(status.asException());
              } else if (status.getCode() == Code.CANCELLED
                  || Context.current().isCancelled()
                  || !SHARD_IS_RETRIABLE.test(status)) {
                // do nothing further if we're cancelled
                resultFuture.setException(status.asException());
              } else {
                // why not, always
                workers.addLast(worker);
              }
            }

            if (!resultFuture.isDone()) {
              if (workers.isEmpty()) {
                resultFuture.setFuture(queryBackplaneThrough(actionKey));
              } else {
                getOnWorker(actionKey, workers, resultFuture, requestMetadata, executor);
              }
            }
          }
        },
        executor);
  }

  @Override
  public ListenableFuture<ActionResult> put(ActionKey actionKey, ActionResult actionResult)
      throws IOException, InterruptedException {
    WorkerStubs.get(workerStubs, Shard.getRandomWritingWorker(backplane, rand))
        .putActionResult(actionKey, actionResult, TracingMetadataUtils.fromCurrentContext());
    readThrough(actionKey, actionResult);
    return immediateFuture(actionResult);
  }

  @Override
  public void invalidate(ActionKey actionKey) {
    actionResultCache.synchronous().invalidate(actionKey);
  }

  @Override
  public void readThrough(ActionKey actionKey, ActionResult actionResult) {
    actionResultCache.put(actionKey, CompletableFuture.completedFuture(actionResult));
  }
}
