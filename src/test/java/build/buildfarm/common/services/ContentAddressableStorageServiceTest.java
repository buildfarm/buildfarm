// Copyright 2024 The Bazel Authors. All rights reserved.
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

package build.buildfarm.common.services;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.FindMissingBlobsRequest;
import build.bazel.remote.execution.v2.FindMissingBlobsResponse;
import build.bazel.remote.execution.v2.GetTreeRequest;
import build.bazel.remote.execution.v2.GetTreeResponse;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.instance.Instance;
import com.google.common.collect.ImmutableList;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class ContentAddressableStorageServiceTest {
  @Test
  public void conveysInstanceResult() {
    Instance instance = mock(Instance.class);

    ContentAddressableStorageGrpc.AsyncService service =
        new ContentAddressableStorageService(instance);
    StreamObserver<FindMissingBlobsResponse> responseObserver = mock(StreamObserver.class);

    when(instance.getName()).thenReturn("test");
    when(instance.findMissingBlobs(
            any(Iterable.class), any(DigestFunction.Value.class), any(RequestMetadata.class)))
        .thenReturn(immediateFuture(ImmutableList.of()));

    service.findMissingBlobs(FindMissingBlobsRequest.getDefaultInstance(), responseObserver);

    verify(responseObserver, times(1)).onNext(FindMissingBlobsResponse.getDefaultInstance());
    verify(responseObserver, times(1)).onCompleted();
    verifyNoMoreInteractions(responseObserver);
    verify(instance, times(1)).getName();
    verify(instance, times(1))
        .findMissingBlobs(
            any(Iterable.class), any(DigestFunction.Value.class), any(RequestMetadata.class));
    verifyNoMoreInteractions(instance);
  }

  @Test
  public void cancelDoesNotOnError() {
    Instance instance = mock(Instance.class);

    ContentAddressableStorageGrpc.AsyncService service =
        new ContentAddressableStorageService(instance);
    StreamObserver<FindMissingBlobsResponse> responseObserver = mock(StreamObserver.class);

    when(instance.getName()).thenReturn("test");
    when(instance.findMissingBlobs(
            any(Iterable.class), any(DigestFunction.Value.class), any(RequestMetadata.class)))
        .thenReturn(immediateFuture(ImmutableList.of()));
    doThrow(Status.CANCELLED.asRuntimeException())
        .when(responseObserver)
        .onNext(any(FindMissingBlobsResponse.class));

    service.findMissingBlobs(FindMissingBlobsRequest.getDefaultInstance(), responseObserver);

    verify(responseObserver, times(1)).onNext(any(FindMissingBlobsResponse.class));
    verifyNoMoreInteractions(responseObserver);
    verify(instance, times(1))
        .findMissingBlobs(
            any(Iterable.class), any(DigestFunction.Value.class), any(RequestMetadata.class));
    verifyNoMoreInteractions(instance);
  }

  @Test
  public void negativePageSizeInvalid() {
    Instance instance = mock(Instance.class);

    ContentAddressableStorageGrpc.AsyncService service =
        new ContentAddressableStorageService(instance);

    GetTreeRequest invalidRequest = GetTreeRequest.newBuilder().setPageSize(-1).build();
    StreamObserver<GetTreeResponse> responseObserver = mock(StreamObserver.class);
    service.getTree(invalidRequest, responseObserver);

    ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(responseObserver, times(1)).onError(errorCaptor.capture());
    assertThat(Status.fromThrowable(errorCaptor.getValue()).getCode())
        .isEqualTo(Status.Code.INVALID_ARGUMENT);
    verifyNoMoreInteractions(responseObserver);
    verifyNoInteractions(instance);
  }
}
