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
import build.buildfarm.v1test.OperationQueueGrpc;
import build.buildfarm.v1test.TakeOperationRequest;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class OperationQueueService extends OperationQueueGrpc.OperationQueueImplBase {
  private final BuildFarmServer server;

  public OperationQueueService(BuildFarmServer server) {
    this.server = server;
  }

  @Override
  public void take(
      TakeOperationRequest request,
      StreamObserver<Operation> responseObserver) {
    Instance instance = server.getInstance(request.getInstanceName());

    instance.match(request.getPlatform(), /*requeueOnFailure=*/ true, (final Operation operation) -> {
      // so this is interesting - the stdout injection belongs here, because
      // we use this criteria to select the format for stream/blob differentiation
      try {
        ExecuteOperationMetadata metadata =
            operation.getMetadata().unpack(ExecuteOperationMetadata.class);
        metadata = metadata.toBuilder()
            .setStdoutStreamName(operation.getName() + "/streams/stdout")
            .setStderrStreamName(operation.getName() + "/streams/stderr")
            .build();
        Operation streamableOperation = operation.toBuilder()
            .setMetadata(Any.pack(metadata))
            .build();

        responseObserver.onNext(streamableOperation);
        responseObserver.onCompleted();
        return true;
      } catch(InvalidProtocolBufferException ex) {
        responseObserver.onError(new StatusException(Status.INTERNAL));
        // should we update operation?
      } catch(StatusRuntimeException ex) {
        if (ex.getStatus().getCode() != Status.Code.CANCELLED) {
          throw ex;
        }
      }
      return false;
    });
  }

  @Override
  public void put(
      Operation operation,
      StreamObserver<com.google.rpc.Status> responseObserver) {
    Instance instance =
        server.getInstanceFromOperationName(operation.getName());

    instance.putOperation(operation);
    responseObserver.onNext(com.google.rpc.Status.newBuilder()
        .setCode(com.google.rpc.Code.OK.getNumber())
        .build());
    responseObserver.onCompleted();
  }
}
