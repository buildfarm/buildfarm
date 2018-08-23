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
import build.bazel.remote.execution.v2.ExecuteRequest;
import build.bazel.remote.execution.v2.ExecutionGrpc;
import build.bazel.remote.execution.v2.WaitExecutionRequest;
import com.google.longrunning.Operation;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.function.Predicate;

public class ExecutionService extends ExecutionGrpc.ExecutionImplBase {
  private final Instances instances;

  public ExecutionService(Instances instances) {
    this.instances = instances;
  }

  private Predicate<Operation> createWatcher(StreamObserver<Operation> responseObserver) {
    return (operation) -> {
      if (Context.current().isCancelled()) {
        return false;
      }
      try {
        if (operation != null) {
          responseObserver.onNext(operation);
        }
        if (operation == null || operation.getDone()) {
          responseObserver.onCompleted();
          return false;
        }
      } catch (StatusRuntimeException e) {
        // no further responses should be necessary
        if (e.getStatus().getCode() != Status.Code.CANCELLED) {
          responseObserver.onError(Status.fromThrowable(e).asException());
        }
        return false;
      } catch (IllegalStateException e) {
        // only indicator for this from ServerCallImpl layer
        // no further responses should be necessary
        if (!e.getMessage().equals("call is closed")) {
          responseObserver.onError(Status.fromThrowable(e).asException());
        }
        return false;
      }
      // still watching
      return true;
    };
  }

  @Override
  public void waitExecution(
      WaitExecutionRequest request, StreamObserver<Operation> responseObserver) {
    String operationName = request.getName();
    Instance instance;
    try {
      instance = instances.getFromOperationName(operationName);
    } catch (InstanceNotFoundException e) {
      responseObserver.onError(BuildFarmInstances.toStatusException(e));
      return;
    }

    boolean watching = instance.watchOperation(
        operationName,
        createWatcher(responseObserver));
    if (!watching) {
      responseObserver.onCompleted();
    }
  }

  @Override
  public void execute(
      ExecuteRequest request, StreamObserver<Operation> responseObserver) {
    Instance instance;
    try {
      instance = instances.get(request.getInstanceName());
    } catch (InstanceNotFoundException ex) {
      responseObserver.onError(BuildFarmInstances.toStatusException(ex));
      return;
    }

    try {
      instance.execute(
          request.getActionDigest(),
          request.getSkipCacheLookup(),
          request.getExecutionPolicy(),
          request.getResultsCachePolicy(),
          createWatcher(responseObserver));
    } catch (IllegalStateException e) {
      responseObserver.onError(Status.FAILED_PRECONDITION
          .withDescription(e.getMessage())
          .asException());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
