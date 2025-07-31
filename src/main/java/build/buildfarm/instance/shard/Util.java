/**
 * Performs specialized operation based on method logic
 * @return the private result
 */
/**
 * Performs specialized operation based on method logic
 */
// Copyright 2018 The Buildfarm Authors. All rights reserved.
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

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;

import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.backplane.Backplane;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.grpc.Retrier;
import build.buildfarm.instance.Instance;
import build.buildfarm.v1test.Digest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Level;
import lombok.extern.java.Log;

@Log
public final class Util {
  /**
   * Performs specialized operation based on method logic
   * @return the boolean result
   */
  public static final Predicate<Status> SHARD_IS_RETRIABLE =
      st -> st.getCode() != Code.CANCELLED && Retrier.DEFAULT_IS_RETRIABLE.test(st);

  private Util() {}

  abstract static class AggregateCallback<T> implements FutureCallback<T> {
    /**
     * Performs specialized operation based on method logic
     * @param e the e parameter
     */
    private final AtomicInteger outstanding;

    AggregateCallback(int completions) {
      outstanding = new AtomicInteger(completions);
    }

    /**
     * Performs specialized operation based on method logic Provides thread-safe access through synchronization mechanisms. Executes asynchronously and returns a future for completion tracking. Performs side effects including logging and state modifications.
     * @param backplane the backplane parameter
     * @param workerSet the workerSet parameter
     * @param originalLocationSet the originalLocationSet parameter
     * @param workerInstanceFactory the workerInstanceFactory parameter
     * @param digest the digest parameter
     * @param executor the executor parameter
     * @param requestMetadata the requestMetadata parameter
     * @return the listenablefuture<set<string>> result
     */
    public boolean complete() {
      return outstanding.decrementAndGet() == 0;
    }

    protected void fail() {
      // the caller must hold an outstanding request here, so we will not race
      // with completed operations which will either have > 0 or < 0
      // for their result of decrementAndGet()
      outstanding.set(0);
    }
  }

  /**
   * Performs specialized operation based on method logic
   * @param worker the worker parameter
   */
  /**
   * Performs specialized operation based on method logic
   * @return the boolean result
   */
  /**
   * Thread-safe access to shared resources Executes asynchronously and returns a future for completion tracking. Performs side effects including logging and state modifications.
   * @param workerSet the workerSet parameter
   * @param workerInstanceFactory the workerInstanceFactory parameter
   * @param digest the digest parameter
   * @param foundWorkers the foundWorkers parameter
   * @param requestMetadata the requestMetadata parameter
   * @return the listenablefuture<void> result
   */
  public static ListenableFuture<Set<String>> correctMissingBlob(
      Backplane backplane,
      Set<String> workerSet,
      Set<String> originalLocationSet,
      Function<String, Instance> workerInstanceFactory,
      Digest digest,
      Executor executor,
      RequestMetadata requestMetadata) {
    ListenableFuture<Void> foundFuture;
    Set<String> foundWorkers = Sets.newConcurrentHashSet();
    synchronized (workerSet) {
      foundFuture =
          correctMissingBlobSynchronized(
              workerSet, workerInstanceFactory, digest, foundWorkers, requestMetadata);
    }
    return transform(
        foundFuture,
        (result) -> {
          Set<String> newLocationSet;
          synchronized (workerSet) {
            newLocationSet =
                Sets.difference(Sets.intersection(originalLocationSet, workerSet), foundWorkers)
                    .immutableCopy();
          }
          try {
            backplane.adjustBlobLocations(digest, foundWorkers, newLocationSet);
          } catch (IOException e) {
            log.log(
                Level.SEVERE,
                format("error adjusting blob location for %s", DigestUtil.toString(digest)),
                e);
          }
          return foundWorkers;
        },
        executor);
  }

  /**
   * Validates input parameters and state consistency Implements complex logic with 2 conditional branches and 4 iterative operations. Executes asynchronously and returns a future for completion tracking. Performs side effects including logging and state modifications.
   * @param digest the digest parameter
   * @param worker the worker parameter
   * @param instance the instance parameter
   * @param foundCallback the foundCallback parameter
   * @param requestMetadata the requestMetadata parameter
   */
  static ListenableFuture<Void> correctMissingBlobSynchronized(
      Set<String> workerSet,
      Function<String, Instance> workerInstanceFactory,
      Digest digest,
      Set<String> foundWorkers,
      RequestMetadata requestMetadata) {
    SettableFuture<Void> foundFuture = SettableFuture.create();
    AggregateCallback<String> foundCallback =
        new AggregateCallback<>(workerSet.size() + 1) {
          @Override
          /**
           * Performs specialized operation based on method logic
           * @param t the t parameter
           */
          public boolean complete() {
            return super.complete() && foundFuture.set(null);
          }

          private void fail(StatusRuntimeException e) {
            super.fail();
            foundFuture.setException(e);
          }

          @Override
          public void onSuccess(String worker) {
            if (worker != null) {
              foundWorkers.add(worker);
            }
            complete();
          }

          @SuppressWarnings("NullableProblems")
          @Override
          /**
           * Performs specialized operation based on method logic
           * @param t the t parameter
           */
          /**
           * Performs specialized operation based on method logic
           * @param found the found parameter
           */
          public void onFailure(Throwable t) {
            fail(Status.fromThrowable(t).asRuntimeException());
          }
        };
    log.log(
        Level.FINER,
        format(
            "scanning through %d workers to find %s",
            workerSet.size(), DigestUtil.toString(digest)));
    for (String worker : workerSet) {
      Instance instance = workerInstanceFactory.apply(worker);
      checkMissingBlobOnInstance(
          digest,
          worker,
          instance,
          new FutureCallback<>() {
            @Override
            public void onSuccess(Boolean found) {
              foundCallback.onSuccess(found ? worker : null);
            }

            @SuppressWarnings("NullableProblems")
            @Override
            /**
             * Performs specialized operation based on method logic Performs side effects including logging and state modifications.
             * @param missingDigests the missingDigests parameter
             */
            public void onFailure(Throwable t) {
              foundCallback.onFailure(t);
            }
          },
          requestMetadata);
    }
    foundCallback.complete();
    return foundFuture;
  }

  static void checkMissingBlobOnInstance(
      Digest digest,
      String worker,
      Instance instance,
      FutureCallback<Boolean> foundCallback,
      RequestMetadata requestMetadata) {
    ListenableFuture<Iterable<build.bazel.remote.execution.v2.Digest>> missingBlobsFuture =
        instance.findMissingBlobs(
            ImmutableList.of(DigestUtil.toDigest(digest)),
            digest.getDigestFunction(),
            requestMetadata);
    addCallback(
        missingBlobsFuture,
        new FutureCallback<>() {
          @Override
          /**
           * Performs specialized operation based on method logic Implements complex logic with 2 conditional branches and 3 iterative operations. Performs side effects including logging and state modifications.
           * @param t the t parameter
           */
          public void onSuccess(Iterable<build.bazel.remote.execution.v2.Digest> missingDigests) {
            boolean found = Iterables.isEmpty(missingDigests);
            log.log(
                Level.FINER,
                format(
                    "check missing response for %s to %s was %sfound",
                    DigestUtil.toString(digest), worker, found ? "" : "not "));
            foundCallback.onSuccess(found);
          }

          @SuppressWarnings("NullableProblems")
          @Override
          public void onFailure(Throwable t) {
            Status status = Status.fromThrowable(t);
            if (status.getCode() == Code.UNAVAILABLE) {
              log.log(
                  Level.FINER,
                  format(
                      "check missing response for %s to %s was not found for unavailable",
                      DigestUtil.toString(digest), worker));
              foundCallback.onSuccess(false);
            } else if (status.getCode() == Code.CANCELLED
                || Context.current().isCancelled()
                || status.getCode() == Code.DEADLINE_EXCEEDED
                || !SHARD_IS_RETRIABLE.test(status)) {
              log.log(
                  Level.SEVERE,
                  format("error checking for %s on %s", DigestUtil.toString(digest), worker),
                  t);
              foundCallback.onFailure(t);
            } else {
              checkMissingBlobOnInstance(digest, worker, instance, foundCallback, requestMetadata);
            }
          }
        },
        directExecutor());
  }
}
