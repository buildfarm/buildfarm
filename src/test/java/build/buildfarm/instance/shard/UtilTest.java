// Copyright 2019 The Bazel Authors. All rights reserved.
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

import static build.buildfarm.instance.shard.Util.correctMissingBlob;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.ShardBackplane;
import build.buildfarm.instance.Instance;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Function;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class UtilTest {
  @Test
  public void correctMissingBlobFailsImmediatelyOnUnretriable() throws InterruptedException {
    String workerName = "worker";
    ShardBackplane backplane = mock(ShardBackplane.class);
    Set<String> workerSet = ImmutableSet.of(workerName);
    Digest digest = Digest.newBuilder()
        .setHash("digest-throws-exception-on-find-missing")
        .setSizeBytes(1)
        .build();
    ImmutableList<Digest> digests = ImmutableList.of(digest);

    Instance instance = mock(Instance.class);
    when(instance.findMissingBlobs(eq(digests), any(Executor.class)))
        .thenReturn(immediateFailedFuture(Status.INVALID_ARGUMENT.asRuntimeException()));

    Function<String, Instance> workerInstanceFactory = new Function<String, Instance>() {
      @Override
      public Instance apply(String worker) {
        if (worker.equals(workerName)) {
          return instance;
        }
        return null;
      }
    };
    ListenableFuture<Set<String>> correctFuture = correctMissingBlob(
        backplane,
        workerSet,
        /* originalLocationSet=*/ ImmutableSet.of(),
        workerInstanceFactory,
        digest,
        directExecutor());
    boolean caughtException = false;
    try {
      correctFuture.get();
    } catch (ExecutionException e) {
      caughtException = true;
      Status status = Status.fromThrowable(e.getCause());
      assertThat(status.getCode()).isEqualTo(Code.INVALID_ARGUMENT);
    }
    verify(instance, times(1)).findMissingBlobs(eq(digests), any(Executor.class));
    assertThat(caughtException).isTrue();
    verifyZeroInteractions(backplane);
  }

  @Test
  public void correctMissingBlobIgnoresUnavailableWorkers() throws Exception {
    String workerName = "worker";
    String unavailableWorkerName = "unavailableWorker";
    ShardBackplane backplane = mock(ShardBackplane.class);
    Set<String> workerSet = ImmutableSet.of(workerName, unavailableWorkerName);

    Digest digest = Digest.newBuilder()
        .setHash("digest")
        .setSizeBytes(1)
        .build();
    ImmutableList<Digest> digests = ImmutableList.of(digest);

    Instance instance = mock(Instance.class);
    when(instance.findMissingBlobs(eq(digests), any(Executor.class)))
        .thenReturn(immediateFuture(ImmutableList.of()));

    Instance unavailableInstance = mock(Instance.class);
    when(unavailableInstance.findMissingBlobs(eq(digests), any(Executor.class)))
        .thenReturn(immediateFailedFuture(Status.UNAVAILABLE.asRuntimeException()));

    Function<String, Instance> workerInstanceFactory = new Function<String, Instance>() {
      @Override
      public Instance apply(String worker) {
        if (worker.equals(workerName)) {
          return instance;
        }
        if (worker.equals(unavailableWorkerName)) {
          return unavailableInstance;
        }
        return null;
      }
    };
    ListenableFuture<Set<String>> correctFuture = correctMissingBlob(
        backplane,
        workerSet,
        /* originalLocationSet=*/ ImmutableSet.of(),
        workerInstanceFactory,
        digest,
        directExecutor());
    assertThat(correctFuture.get()).isEqualTo(ImmutableSet.of(workerName));
    verify(instance, times(1)).findMissingBlobs(eq(digests), any(Executor.class));
    verify(unavailableInstance, times(1)).findMissingBlobs(eq(digests), any(Executor.class));
    verify(backplane, times(1)).adjustBlobLocations(
        eq(digest),
        eq(ImmutableSet.of(workerName)),
        eq(ImmutableSet.of()));
  }

  @Test
  public void correctMissingBlobRetriesRetriable() throws Exception {
    String workerName = "worker";
    ShardBackplane backplane = mock(ShardBackplane.class);
    Set<String> workerSet = ImmutableSet.of(workerName);

    Digest digest = Digest.newBuilder()
        .setHash("digest")
        .setSizeBytes(1)
        .build();
    ImmutableList<Digest> digests = ImmutableList.of(digest);

    Instance instance = mock(Instance.class);
    when(instance.findMissingBlobs(eq(digests), any(Executor.class)))
        .thenReturn(immediateFailedFuture(Status.UNKNOWN.asRuntimeException()))
        .thenReturn(immediateFuture(ImmutableList.of()));

    Function<String, Instance> workerInstanceFactory = new Function<String, Instance>() {
      @Override
      public Instance apply(String worker) {
        if (worker.equals(workerName)) {
          return instance;
        }
        return null;
      }
    };
    ListenableFuture<Set<String>> correctFuture = correctMissingBlob(
        backplane,
        workerSet,
        /* originalLocationSet=*/ ImmutableSet.of(),
        workerInstanceFactory,
        digest,
        directExecutor());
    assertThat(correctFuture.get()).isEqualTo(ImmutableSet.of(workerName));
    verify(instance, times(2)).findMissingBlobs(eq(digests), any(Executor.class));
    verify(backplane, times(1)).adjustBlobLocations(
        eq(digest),
        eq(ImmutableSet.of(workerName)),
        eq(ImmutableSet.of()));
  }

  @Test
  public void correctMissingBlobIgnoresBackplaneException() throws Exception {
    String workerName = "worker";
    Set<String> workerSet = ImmutableSet.of(workerName);

    Digest digest = Digest.newBuilder()
        .setHash("digest")
        .setSizeBytes(1)
        .build();
    ImmutableList<Digest> digests = ImmutableList.of(digest);

    Instance instance = mock(Instance.class);
    when(instance.findMissingBlobs(eq(digests), any(Executor.class)))
        .thenReturn(immediateFailedFuture(Status.UNKNOWN.asRuntimeException()))
        .thenReturn(immediateFuture(ImmutableList.of()));

    ShardBackplane backplane = mock(ShardBackplane.class);
    doThrow(new IOException("failed to adjustBlobLocations"))
        .when(backplane)
        .adjustBlobLocations(
            eq(digest),
            eq(ImmutableSet.of(workerName)),
            eq(ImmutableSet.of()));

    Function<String, Instance> workerInstanceFactory = new Function<String, Instance>() {
      @Override
      public Instance apply(String worker) {
        if (worker.equals(workerName)) {
          return instance;
        }
        return null;
      }
    };
    ListenableFuture<Set<String>> correctFuture = correctMissingBlob(
        backplane,
        workerSet,
        /* originalLocationSet=*/ ImmutableSet.of(),
        workerInstanceFactory,
        digest,
        directExecutor());
    assertThat(correctFuture.get()).isEqualTo(ImmutableSet.of(workerName));
    verify(instance, times(2)).findMissingBlobs(eq(digests), any(Executor.class));
    verify(backplane, times(1)).adjustBlobLocations(
        eq(digest),
        eq(ImmutableSet.of(workerName)),
        eq(ImmutableSet.of()));
  }
}
