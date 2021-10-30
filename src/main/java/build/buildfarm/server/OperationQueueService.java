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
import com.google.longrunning.Operation;
import com.google.rpc.Code;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.function.Consumer;

public class OperationQueueService extends OperationQueueGrpc.OperationQueueImplBase {
  private final Instance instance;

  public OperationQueueService(Instance instance) {
    this.instance = instance;
  }

  private static class OperationQueueMatchListener implements MatchListener {
    @SuppressWarnings("rawtypes")
    private final InterruptingPredicate onMatch;

    private final Consumer<Runnable> setOnCancelHandler;
    private final QueueEntry queueEntry = null;

    @SuppressWarnings("rawtypes")
    OperationQueueMatchListener(
        InterruptingPredicate onMatch, Consumer<Runnable> setOnCancelHandler) {
      this.onMatch = onMatch;
      this.setOnCancelHandler = setOnCancelHandler;
    }

    @Override
    public void onWaitStart() {}

    @Override
    public void onWaitEnd() {}

    @SuppressWarnings("unchecked")
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
    ServerCallStreamObserver<QueueEntry> callObserver =
        (ServerCallStreamObserver<QueueEntry>) responseObserver;

    try {
      instance.match(
          request.getPlatform(),
          new OperationQueueMatchListener(
              createOnMatch(instance, responseObserver), callObserver::setOnCancelHandler));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
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
  public void put(Operation operation, StreamObserver<com.google.rpc.Status> responseObserver) {
    try {
      boolean ok = instance.putAndValidateOperation(operation);
      Code code = ok ? Code.OK : Code.INVALID_ARGUMENT;
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
    boolean ok = instance.pollOperation(request.getOperationName(), request.getStage());
    Code code = ok ? Code.OK : Code.INVALID_ARGUMENT;
    responseObserver.onNext(com.google.rpc.Status.newBuilder().setCode(code.getNumber()).build());
    responseObserver.onCompleted();
  }
}
