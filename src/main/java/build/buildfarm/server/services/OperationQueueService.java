// Copyright 2017 The Buildfarm Authors. All rights reserved.
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
import build.buildfarm.v1test.BackplaneStatus;
import build.buildfarm.v1test.BackplaneStatusRequest;
import build.buildfarm.v1test.OperationQueueGrpc;
import build.buildfarm.v1test.PollOperationRequest;
import com.google.rpc.Code;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public class OperationQueueService extends OperationQueueGrpc.OperationQueueImplBase {
  private final Instance instance;

  public OperationQueueService(Instance instance) {
    this.instance = instance;
  }

  @Override
  public void status(
      BackplaneStatusRequest request, StreamObserver<BackplaneStatus> responseObserver) {
    try {
      responseObserver.onNext(instance.backplaneStatus());
      responseObserver.onCompleted();
    } catch (RuntimeException e) {
      responseObserver.onError(Status.fromThrowable(e).asException());
    }
  }

  @Override
  public void poll(
      PollOperationRequest request, StreamObserver<com.google.rpc.Status> responseObserver) {
    boolean ok = instance.pollOperation(request.getOperationName(), request.getStage());
    Code code = ok ? Code.OK : Code.INVALID_ARGUMENT;
    responseObserver.onNext(com.google.rpc.Status.newBuilder().setCode(code.getNumber()).build());
    responseObserver.onCompleted();
  }
}
