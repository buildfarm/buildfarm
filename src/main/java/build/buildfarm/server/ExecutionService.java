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
import com.google.devtools.remoteexecution.v1test.ExecuteRequest;
import com.google.devtools.remoteexecution.v1test.ExecutionGrpc;
import com.google.longrunning.Operation;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

public class ExecutionService extends ExecutionGrpc.ExecutionImplBase {
  private final Instances instances;

  public ExecutionService(Instances instances) {
    this.instances = instances;
  }

  @Override
  public void execute(
      ExecuteRequest request, StreamObserver<Operation> responseObserver) {
    Instance instance;
    try {
      instance = instances.get(request.getInstanceName());
    } catch (InstanceNotFoundException e) {
      responseObserver.onError(BuildFarmInstances.toStatusException(e));
      return;
    }

    try {
      instance.execute(
          request.getAction(),
          request.getSkipCacheLookup(),
          request.getTotalInputFileCount(),
          request.getTotalInputFileBytes(),
          (operation) -> {
            responseObserver.onNext(operation);
            responseObserver.onCompleted();
          });
    } catch (InterruptedException e) {
      responseObserver.onError(Status.fromThrowable(e).asException());
    } catch (IllegalStateException e) {
      e.printStackTrace();
      responseObserver.onError(
          Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asException());
    }
  }
}
