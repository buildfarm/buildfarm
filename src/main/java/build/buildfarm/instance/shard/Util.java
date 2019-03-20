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

package build.buildfarm.instance.shard;

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.base.Predicates.notNull;
import static java.lang.String.format;
import static java.util.logging.Level.SEVERE;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.ShardBackplane;
import build.buildfarm.common.grpc.Retrier;
import build.buildfarm.instance.Instance;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.ImmutableList;
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
import java.util.logging.Logger;

public class Util {
  private static final Logger logger = Logger.getLogger(Util.class.getName());
  public static final Predicate<Status> SHARD_IS_RETRIABLE =
      st -> st.getCode() != Code.CANCELLED && Retrier.DEFAULT_IS_RETRIABLE.apply(st);

  private Util() { }

  static abstract class AggregateCallback<T> implements FutureCallback<T> {
    private final AtomicInteger outstanding;

    AggregateCallback(AtomicInteger outstanding) {
      this.outstanding = outstanding;
    }

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

  public static ListenableFuture<Set<String>> correctMissingBlob(
      ShardBackplane backplane,
      Set<String> workerSet,
      Set<String> originalLocationSet,
      Function<String, Instance> workerInstanceFactory,
      Digest digest,
      Executor executor) {
    SettableFuture<Void> foundFuture = SettableFuture.create();
    Set<String> foundWorkers = Sets.newConcurrentHashSet();
    AtomicInteger outstanding = new AtomicInteger(1);
    AggregateCallback<String> foundCallback = new AggregateCallback<String>(outstanding) {
      public boolean complete() {
        if (super.complete() && !foundFuture.isDone()) {
          foundFuture.set(null);
          return true;
        }
        return false;
      }

      protected void fail(StatusRuntimeException e) {
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

      @Override
      public void onFailure(Throwable t) {
        fail(Status.fromThrowable(t).asRuntimeException());
      }
    };
    for (String worker : workerSet) {
      outstanding.incrementAndGet();
      Instance instance = workerInstanceFactory.apply(worker);
      checkMissingBlobOnInstance(
          digest,
          instance,
          new FutureCallback<Boolean>() {
            @Override
            public void onSuccess(Boolean found) {
              foundCallback.onSuccess(found ? worker : null);
            }

            @Override
            public void onFailure(Throwable t) {
              foundCallback.onFailure(t);
            }
          },
          executor);
    }
    foundCallback.complete();
    return transform(
        foundFuture,
        (result) -> {
          try {
            backplane.adjustBlobLocations(
                digest,
                foundWorkers,
                Sets.difference(Sets.intersection(originalLocationSet, workerSet), foundWorkers));
          } catch (IOException e) {
            logger.log(SEVERE, format("error adjusting blob location for %s", DigestUtil.toString(digest)), e);
          }
          return foundWorkers;
        },
        executor);
  }

  static void checkMissingBlobOnInstance(
      Digest digest,
      Instance instance,
      FutureCallback<Boolean> foundCallback,
      Executor executor) {
    ListenableFuture<Iterable<Digest>> missingBlobsFuture =
        instance.findMissingBlobs(ImmutableList.of(digest), executor);
    addCallback(
        missingBlobsFuture,
        new FutureCallback<Iterable<Digest>>() {
          @Override
          public void onSuccess(Iterable<Digest> missingDigests) {
            foundCallback.onSuccess(Iterables.isEmpty(missingDigests));
          }

          @Override
          public void onFailure(Throwable t) {
            Status status = Status.fromThrowable(t);
            if (status.getCode() == Code.UNAVAILABLE) {
              foundCallback.onSuccess(false);
            } else if (
                status.getCode() == Code.CANCELLED || Context.current().isCancelled()
                || status.getCode() == Code.DEADLINE_EXCEEDED
                || !SHARD_IS_RETRIABLE.test(status)) {
              foundCallback.onFailure(t);
            } else {
              checkMissingBlobOnInstance(
                  digest,
                  instance,
                  foundCallback,
                  executor);
            }
          }
        },
        executor);
  }
}
