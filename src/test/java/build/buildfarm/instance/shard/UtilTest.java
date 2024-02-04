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
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.backplane.Backplane;
import build.buildfarm.instance.Instance;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Status;
import io.grpc.Status.Code;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class UtilTest {
  @Test
  public void correctMissingBlobChecksAllWorkers() throws Exception {
    String worker1Name = "worker1";
    String worker2Name = "worker2";
    String worker3Name = "worker3";
    Set<String> workerSet = ImmutableSet.of(worker1Name, worker2Name, worker3Name);

    Digest digest = Digest.newBuilder().setHash("digest").setSizeBytes(1).build();
    ImmutableList<Digest> digests = ImmutableList.of(digest);

    Instance foundInstance = mock(Instance.class);
    when(foundInstance.findMissingBlobs(eq(digests), any(RequestMetadata.class)))
        .thenReturn(immediateFuture(ImmutableList.of()));
    Instance missingInstance = mock(Instance.class);
    when(missingInstance.findMissingBlobs(eq(digests), any(RequestMetadata.class)))
        .thenReturn(immediateFuture(ImmutableList.of(digest)));

    Backplane backplane = mock(Backplane.class);

    Function<String, Instance> workerInstanceFactory =
        worker -> {
          if (worker.equals(worker1Name)) {
            return missingInstance;
          }
          if (worker.equals(worker2Name) || worker.equals(worker3Name)) {
            return foundInstance;
          }
          return null;
        };
    ListenableFuture<Set<String>> correctFuture =
        correctMissingBlob(
            backplane,
            workerSet,
            /* originalLocationSet=*/ ImmutableSet.of(),
            workerInstanceFactory,
            digest,
            directExecutor(),
            RequestMetadata.getDefaultInstance());
    assertThat(correctFuture.get()).isEqualTo(ImmutableSet.of(worker2Name, worker3Name));
    verify(foundInstance, times(2)).findMissingBlobs(eq(digests), any(RequestMetadata.class));
    verify(missingInstance, times(1)).findMissingBlobs(eq(digests), any(RequestMetadata.class));
    verify(backplane, times(1))
        .adjustBlobLocations(
            eq(digest), eq(ImmutableSet.of(worker2Name, worker3Name)), eq(ImmutableSet.of()));
  }

  @Test
  public void correctMissingBlobFailsImmediatelyOnUnretriable() throws InterruptedException {
    String workerName = "worker";
    Backplane backplane = mock(Backplane.class);
    Set<String> workerSet = ImmutableSet.of(workerName);
    Digest digest =
        Digest.newBuilder()
            .setHash("digest-throws-exception-on-find-missing")
            .setSizeBytes(1)
            .build();
    ImmutableList<Digest> digests = ImmutableList.of(digest);

    Instance instance = mock(Instance.class);
    when(instance.findMissingBlobs(eq(digests), any(RequestMetadata.class)))
        .thenReturn(immediateFailedFuture(Status.INVALID_ARGUMENT.asRuntimeException()));

    Function<String, Instance> workerInstanceFactory =
        worker -> {
          if (worker.equals(workerName)) {
            return instance;
          }
          return null;
        };
    ListenableFuture<Set<String>> correctFuture =
        correctMissingBlob(
            backplane,
            workerSet,
            /* originalLocationSet=*/ ImmutableSet.of(),
            workerInstanceFactory,
            digest,
            directExecutor(),
            RequestMetadata.getDefaultInstance());
    boolean caughtException = false;
    try {
      correctFuture.get();
    } catch (ExecutionException e) {
      caughtException = true;
      Status status = Status.fromThrowable(e.getCause());
      assertThat(status.getCode()).isEqualTo(Code.INVALID_ARGUMENT);
    }
    verify(instance, times(1)).findMissingBlobs(eq(digests), any(RequestMetadata.class));
    assertThat(caughtException).isTrue();
    verifyNoInteractions(backplane);
  }

  @Test
  public void correctMissingBlobIgnoresUnavailableWorkers() throws Exception {
    String workerName = "worker";
    String unavailableWorkerName = "unavailableWorker";
    Backplane backplane = mock(Backplane.class);
    Set<String> workerSet = ImmutableSet.of(workerName, unavailableWorkerName);

    Digest digest = Digest.newBuilder().setHash("digest").setSizeBytes(1).build();
    ImmutableList<Digest> digests = ImmutableList.of(digest);

    Instance instance = mock(Instance.class);
    when(instance.findMissingBlobs(eq(digests), any(RequestMetadata.class)))
        .thenReturn(immediateFuture(ImmutableList.of()));

    Instance unavailableInstance = mock(Instance.class);
    when(unavailableInstance.findMissingBlobs(eq(digests), any(RequestMetadata.class)))
        .thenReturn(immediateFailedFuture(Status.UNAVAILABLE.asRuntimeException()));

    Function<String, Instance> workerInstanceFactory =
        worker -> {
          if (worker.equals(workerName)) {
            return instance;
          }
          if (worker.equals(unavailableWorkerName)) {
            return unavailableInstance;
          }
          return null;
        };
    ListenableFuture<Set<String>> correctFuture =
        correctMissingBlob(
            backplane,
            workerSet,
            /* originalLocationSet=*/ ImmutableSet.of(),
            workerInstanceFactory,
            digest,
            directExecutor(),
            RequestMetadata.getDefaultInstance());
    assertThat(correctFuture.get()).isEqualTo(ImmutableSet.of(workerName));
    verify(instance, times(1)).findMissingBlobs(eq(digests), any(RequestMetadata.class));
    verify(unavailableInstance, times(1)).findMissingBlobs(eq(digests), any(RequestMetadata.class));
    verify(backplane, times(1))
        .adjustBlobLocations(eq(digest), eq(ImmutableSet.of(workerName)), eq(ImmutableSet.of()));
  }

  @Test
  public void correctMissingBlobRetriesRetriable() throws Exception {
    String workerName = "worker";
    Backplane backplane = mock(Backplane.class);
    Set<String> workerSet = ImmutableSet.of(workerName);

    Digest digest = Digest.newBuilder().setHash("digest").setSizeBytes(1).build();
    ImmutableList<Digest> digests = ImmutableList.of(digest);

    Instance instance = mock(Instance.class);
    when(instance.findMissingBlobs(eq(digests), any(RequestMetadata.class)))
        .thenReturn(immediateFailedFuture(Status.UNKNOWN.asRuntimeException()))
        .thenReturn(immediateFuture(ImmutableList.of()));

    Function<String, Instance> workerInstanceFactory =
        worker -> {
          if (worker.equals(workerName)) {
            return instance;
          }
          return null;
        };
    ListenableFuture<Set<String>> correctFuture =
        correctMissingBlob(
            backplane,
            workerSet,
            /* originalLocationSet=*/ ImmutableSet.of(),
            workerInstanceFactory,
            digest,
            directExecutor(),
            RequestMetadata.getDefaultInstance());
    assertThat(correctFuture.get()).isEqualTo(ImmutableSet.of(workerName));
    verify(instance, times(2)).findMissingBlobs(eq(digests), any(RequestMetadata.class));
    verify(backplane, times(1))
        .adjustBlobLocations(eq(digest), eq(ImmutableSet.of(workerName)), eq(ImmutableSet.of()));
  }

  @Test
  public void correctMissingBlobIgnoresBackplaneException() throws Exception {
    String workerName = "worker";
    Set<String> workerSet = ImmutableSet.of(workerName);

    Digest digest = Digest.newBuilder().setHash("digest").setSizeBytes(1).build();
    ImmutableList<Digest> digests = ImmutableList.of(digest);

    Instance instance = mock(Instance.class);
    when(instance.findMissingBlobs(eq(digests), any(RequestMetadata.class)))
        .thenReturn(immediateFailedFuture(Status.UNKNOWN.asRuntimeException()))
        .thenReturn(immediateFuture(ImmutableList.of()));

    Backplane backplane = mock(Backplane.class);
    doThrow(new IOException("failed to adjustBlobLocations"))
        .when(backplane)
        .adjustBlobLocations(eq(digest), eq(ImmutableSet.of(workerName)), eq(ImmutableSet.of()));

    Function<String, Instance> workerInstanceFactory =
        worker -> {
          if (worker.equals(workerName)) {
            return instance;
          }
          return null;
        };
    ListenableFuture<Set<String>> correctFuture =
        correctMissingBlob(
            backplane,
            workerSet,
            /* originalLocationSet=*/ ImmutableSet.of(),
            workerInstanceFactory,
            digest,
            directExecutor(),
            RequestMetadata.getDefaultInstance());
    assertThat(correctFuture.get()).isEqualTo(ImmutableSet.of(workerName));
    verify(instance, times(2)).findMissingBlobs(eq(digests), any(RequestMetadata.class));
    verify(backplane, times(1))
        .adjustBlobLocations(eq(digest), eq(ImmutableSet.of(workerName)), eq(ImmutableSet.of()));
  }
}
