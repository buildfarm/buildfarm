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

import static build.buildfarm.instance.Utils.putBlobFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.Instance.MatchListener;
import build.buildfarm.common.function.InterruptingPredicate;
import build.buildfarm.v1test.OperationQueueGrpc;
import build.buildfarm.v1test.PollOperationRequest;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.QueuedOperation;
import build.buildfarm.v1test.TakeOperationRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ExecutionException;

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
    private QueueEntry queueEntry = null;

    OperationQueueMatchListener(Instance instance, InterruptingPredicate onMatch) {
      this.instance = instance;
      this.onMatch = onMatch;
    }

    @Override
    public void onWaitStart() {
    }

    @Override
    public void onWaitEnd() {
    }

    @Override
    public boolean onEntry(QueueEntry queueEntry) throws InterruptedException {
      return onMatch.testInterruptibly(queueEntry);
    }
  }

  private InterruptingPredicate<QueueEntry> createOnMatch(
      Instance instance, StreamObserver<QueueEntry> responseObserver) {
    return (queueEntry) -> {
      try {
        responseObserver.onNext(queueEntry);
        responseObserver.onCompleted();
        return true;
      } catch(StatusRuntimeException e) {
        Status status = Status.fromThrowable(e);
        if (status.getCode() != Status.Code.CANCELLED) {
          responseObserver.onError(e);
        }
      }
      return false;
    };
  }

  @Override
  public void take(
      TakeOperationRequest request,
      StreamObserver<QueueEntry> responseObserver) {
    Instance instance;
    try {
      instance = instances.get(request.getInstanceName());
    } catch (InstanceNotFoundException e) {
      responseObserver.onError(BuildFarmInstances.toStatusException(e));
      return;
    }

    try {
      instance.match(
          request.getPlatform(),
          new OperationQueueMatchListener(
              instance,
              createOnMatch(instance, responseObserver)));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void put(
      Operation operation,
      StreamObserver<com.google.rpc.Status> responseObserver) {
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
      responseObserver.onNext(com.google.rpc.Status.newBuilder()
          .setCode(code.getNumber())
          .build());
      responseObserver.onCompleted();
    } catch (IllegalStateException e) {
      responseObserver.onNext(com.google.rpc.Status.newBuilder()
          .setCode(Code.FAILED_PRECONDITION.getNumber())
          .build());
      responseObserver.onCompleted();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void poll(
      PollOperationRequest request,
      StreamObserver<com.google.rpc.Status> responseObserver) {
    Instance instance;
    try {
      instance = instances.getFromOperationName(
          request.getOperationName());
    } catch (InstanceNotFoundException e) {
      responseObserver.onError(BuildFarmInstances.toStatusException(e));
      return;
    }

    boolean ok = instance.pollOperation(
        request.getOperationName(),
        request.getStage());
    Code code = ok ? Code.OK : Code.UNAVAILABLE;
    responseObserver.onNext(com.google.rpc.Status.newBuilder()
        .setCode(code.getNumber())
        .build());
    responseObserver.onCompleted();
  }
}
