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

package build.buildfarm.server.services;

import build.buildfarm.instance.Instance;
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
import java.io.IOException;

public class OperationsService extends OperationsGrpc.OperationsImplBase {
  public static final int LIST_OPERATIONS_MAXIMUM_PAGE_SIZE = 100;
  public static final int LIST_OPERATIONS_DEFAULT_PAGE_SIZE = 50;

  private final Instance instance;

  public OperationsService(Instance instance) {
    this.instance = instance;
  }

  @Override
  public void listOperations(
      ListOperationsRequest request, StreamObserver<ListOperationsResponse> responseObserver) {
    int pageSize = request.getPageSize();
    if (pageSize < 0 || pageSize > LIST_OPERATIONS_MAXIMUM_PAGE_SIZE) {
      responseObserver.onError(Status.OUT_OF_RANGE.asException());
      return;
    }
    if (pageSize == 0) {
      pageSize = LIST_OPERATIONS_DEFAULT_PAGE_SIZE;
    }

    // locate index
    String name = request.getName();
    if (name.startsWith(instance.getName() + "/")) {
      name = name.substring(instance.getName().length() + 1);
    } else {
      responseObserver.onError(
          Status.NOT_FOUND.withDescription("instance not found").asException());
      return;
    }

    // TODO make async
    try {
      ListOperationsResponse.Builder response = ListOperationsResponse.newBuilder();
      response.setNextPageToken(
          instance.listOperations(
              name,
              pageSize,
              request.getPageToken(),
              request.getFilter(),
              response::addOperations));
      responseObserver.onNext(response.build());
      responseObserver.onCompleted();
    } catch (IOException e) {
      responseObserver.onError(Status.fromThrowable(e).asException());
    }
  }

  @Override
  public void getOperation(
      GetOperationRequest request, StreamObserver<Operation> responseObserver) {
    // TODO drop instance from name
    responseObserver.onNext(instance.getOperation(request.getName()));
    responseObserver.onCompleted();
  }

  @Override
  public void deleteOperation(
      DeleteOperationRequest request, StreamObserver<Empty> responseObserver) {
    // TODO drop instance from name
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
    // TODO drop instance from name
    try {
      instance.cancelOperation(request.getName());
      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
