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

import build.buildfarm.common.function.InterruptingPredicate;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.MatchListener;
import build.buildfarm.v1test.BackplaneStatus;
import build.buildfarm.v1test.BackplaneStatusRequest;
import build.buildfarm.v1test.OperationQueueGrpc;
import build.buildfarm.v1test.PollOperationRequest;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.TakeOperationRequest;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.longrunning.Operation;
import com.google.rpc.Code;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class OperationQueueService extends OperationQueueGrpc.OperationQueueImplBase {
  private final Instances instances;

  public OperationQueueService(Instances instances) {
    this.instances = instances;
  }

  private static <V> V getUnchecked(ListenableFuture<V> future) throws InterruptedException {
    try {
      return future.get();
    } catch (ExecutionException e) {
      return null;
    }
  }

  private static class OperationQueueMatchListener implements MatchListener {
    private final Instance instance;
    private final InterruptingPredicate onMatch;
    private final Consumer<Runnable> setOnCancelHandler;
    private QueueEntry queueEntry = null;

    OperationQueueMatchListener(
        Instance instance, InterruptingPredicate onMatch, Consumer<Runnable> setOnCancelHandler) {
      this.instance = instance;
      this.onMatch = onMatch;
      this.setOnCancelHandler = setOnCancelHandler;
    }

    @Override
    public void onWaitStart() {}

    @Override
    public void onWaitEnd() {}

    @Override
    public boolean onEntry(QueueEntry queueEntry) throws InterruptedException {
      return onMatch.testInterruptibly(queueEntry);
    }

    @Override
    public void onError(Throwable t) {
      Throwables.throwIfUnchecked(t);
      throw new RuntimeException(t);
    }

    @Override
    public void setOnCancelHandler(Runnable onCancelHandler) {
      setOnCancelHandler.accept(onCancelHandler);
    }
  }

  private InterruptingPredicate<QueueEntry> createOnMatch(
      Instance instance, StreamObserver<QueueEntry> responseObserver) {
    return (queueEntry) -> {
      try {
        responseObserver.onNext(queueEntry);
        responseObserver.onCompleted();
        return true;
      } catch (StatusRuntimeException e) {
        Status status = Status.fromThrowable(e);
        if (status.getCode() != Status.Code.CANCELLED) {
          responseObserver.onError(e);
        }
      }
      instance.putOperation(instance.getOperation(queueEntry.getExecuteEntry().getOperationName()));
      return false;
    };
  }

  @Override
  public void take(TakeOperationRequest request, StreamObserver<QueueEntry> responseObserver) {
    Instance instance;
    try {
      instance = instances.get(request.getInstanceName());
    } catch (InstanceNotFoundException e) {
      responseObserver.onError(BuildFarmInstances.toStatusException(e));
      return;
    }

    ServerCallStreamObserver<QueueEntry> callObserver =
        (ServerCallStreamObserver<QueueEntry>) responseObserver;

    try {
      instance.match(
          request.getPlatform(),
          new OperationQueueMatchListener(
              instance,
              createOnMatch(instance, responseObserver),
              callObserver::setOnCancelHandler));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void status(
      BackplaneStatusRequest request, StreamObserver<BackplaneStatus> responseObserver) {
    Instance instance;
    try {
      instance = instances.get(request.getInstanceName());
    } catch (InstanceNotFoundException e) {
      responseObserver.onError(BuildFarmInstances.toStatusException(e));
      return;
    }

    try {
      responseObserver.onNext(instance.backplaneStatus());
      responseObserver.onCompleted();
    } catch (RuntimeException e) {
      responseObserver.onError(Status.fromThrowable(e).asException());
    }
  }

  @Override
  public void put(Operation operation, StreamObserver<com.google.rpc.Status> responseObserver) {
    Instance instance;
    try {
      instance = instances.getFromOperationName(operation.getName());
    } catch (InstanceNotFoundException e) {
      responseObserver.onError(BuildFarmInstances.toStatusException(e));
      return;
    }

    try {
      boolean ok = instance.putAndValidateOperation(operation);
      Code code = ok ? Code.OK : Code.UNAVAILABLE;
      responseObserver.onNext(com.google.rpc.Status.newBuilder().setCode(code.getNumber()).build());
      responseObserver.onCompleted();
    } catch (IllegalStateException e) {
      responseObserver.onNext(
          com.google.rpc.Status.newBuilder().setCode(Code.FAILED_PRECONDITION.getNumber()).build());
      responseObserver.onCompleted();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void poll(
      PollOperationRequest request, StreamObserver<com.google.rpc.Status> responseObserver) {
    Instance instance;
    try {
      instance = instances.getFromOperationName(request.getOperationName());
    } catch (InstanceNotFoundException e) {
      responseObserver.onError(BuildFarmInstances.toStatusException(e));
      return;
    }

    boolean ok = instance.pollOperation(request.getOperationName(), request.getStage());
    Code code = ok ? Code.OK : Code.UNAVAILABLE;
    responseObserver.onNext(com.google.rpc.Status.newBuilder().setCode(code.getNumber()).build());
    responseObserver.onCompleted();
  }
}
