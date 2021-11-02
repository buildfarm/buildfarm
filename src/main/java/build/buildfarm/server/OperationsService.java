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

package build.buildfarm.server;

import build.buildfarm.instance.Instance;
import com.google.common.collect.ImmutableList;
import com.google.longrunning.CancelOperationRequest;
import com.google.longrunning.DeleteOperationRequest;
import com.google.longrunning.GetOperationRequest;
import com.google.longrunning.ListOperationsRequest;
import com.google.longrunning.ListOperationsResponse;
import com.google.longrunning.Operation;
import com.google.longrunning.OperationsGrpc;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public class OperationsService extends OperationsGrpc.OperationsImplBase {
  private final Instance instance;

  public OperationsService(Instance instance) {
    this.instance = instance;
  }

  @Override
  public void listOperations(
      ListOperationsRequest request, StreamObserver<ListOperationsResponse> responseObserver) {
    int pageSize = request.getPageSize();
    if (pageSize < 0) {
      responseObserver.onError(Status.OUT_OF_RANGE.asException());
      return;
    }

    ImmutableList.Builder<Operation> operations = new ImmutableList.Builder<>();

    String nextPageToken =
        instance.listOperations(pageSize, request.getPageToken(), request.getFilter(), operations);

    responseObserver.onNext(
        ListOperationsResponse.newBuilder()
            .addAllOperations(operations.build())
            .setNextPageToken(nextPageToken)
            .build());
    responseObserver.onCompleted();
  }

  @Override
  public void getOperation(
      GetOperationRequest request, StreamObserver<Operation> responseObserver) {
    responseObserver.onNext(instance.getOperation(request.getName()));
    responseObserver.onCompleted();
  }

  @Override
  public void deleteOperation(
      DeleteOperationRequest request, StreamObserver<Empty> responseObserver) {
    try {
      instance.deleteOperation(request.getName());
      responseObserver.onNext(Empty.newBuilder().build());
      responseObserver.onCompleted();
    } catch (IllegalStateException e) {
      responseObserver.onError(Status.fromThrowable(e).asException());
    }
  }

  @Override
  public void cancelOperation(
      CancelOperationRequest request, StreamObserver<Empty> responseObserver) {
    try {
      instance.cancelOperation(request.getName());
      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
