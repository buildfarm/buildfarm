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
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.watcher.v1.Change;
import com.google.watcher.v1.ChangeBatch;
import com.google.watcher.v1.Request;
import com.google.watcher.v1.WatcherGrpc;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class WatcherService extends WatcherGrpc.WatcherImplBase {
  private final Instances instances;

  public WatcherService(Instances instances) {
    this.instances = instances;
  }

  @Override
  public void watch(
      Request request, StreamObserver<ChangeBatch> responseObserver) {
    // FIXME url decode
    String operationName = request.getTarget();
    Instance instance;
    try {
      instance = instances.getFromOperationName(operationName);
    } catch (InstanceNotFoundException e) {
      responseObserver.onError(BuildFarmInstances.toStatusException(e));
      return;
    }

    // FIXME watcher client implementation cannot handle full
    // watcher implementation (specifically with element changes)
    // would error with anything other than full operation data

    ByteString resumeMarker = request.getResumeMarker();
    boolean watchInitialState;
    if (resumeMarker.toStringUtf8().equals("now")) {
      ChangeBatch.Builder changeBatchBuilder = ChangeBatch.newBuilder();
      changeBatchBuilder.addChangesBuilder()
          .setState(Change.State.INITIAL_STATE_SKIPPED);
      responseObserver.onNext(changeBatchBuilder.build());
      watchInitialState = false;
    } if (resumeMarker.isEmpty()) {
      watchInitialState = true;
    } else {
      responseObserver.onError(Status.UNIMPLEMENTED.asException());
      return;
    }

    boolean watching = instance.watchOperation(
        operationName, watchInitialState, operation -> {
          ChangeBatch.Builder changeBatchBuilder = ChangeBatch.newBuilder();

          if (operation == null) {
            changeBatchBuilder.addChangesBuilder()
                .setState(Change.State.DOES_NOT_EXIST);
          } else {
            changeBatchBuilder.addChangesBuilder()
                .setState(Change.State.EXISTS)
                .setData(Any.pack(operation));
          }

          try {
            responseObserver.onNext(changeBatchBuilder.build());

            // we terminate the connection when no further changes
            // would occur, or the operation is unknown
            if (operation == null || operation.getDone()) {
              responseObserver.onCompleted();
            }
          } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() != Status.Code.CANCELLED) {
              throw e;
            }
            return false;
          } catch (IllegalStateException e) {
            e.printStackTrace();
            // check for 'call is closed'?
            return false;
          }
          return true;
        });
    if (!watching) {
      responseObserver.onCompleted();
    }
  }
}
