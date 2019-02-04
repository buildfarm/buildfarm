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

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;

import build.bazel.remote.execution.v2.ExecuteRequest;
import build.bazel.remote.execution.v2.ExecutionGrpc;
import build.bazel.remote.execution.v2.WaitExecutionRequest;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.grpc.TracingMetadataUtils;
import build.buildfarm.instance.Instance;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.longrunning.Operation;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CancellationException;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class ExecutionService extends ExecutionGrpc.ExecutionImplBase {
  public static final Logger logger = Logger.getLogger(ExecutionService.class.getName());

  private final Instances instances;

  public ExecutionService(Instances instances) {
    this.instances = instances;
  }

  private void logExecute(String instanceName, ExecuteRequest request) {
    logger.info(format("ExecutionSuccess: %s: %s", instanceName, DigestUtil.toString(request.getActionDigest())));
  }

  private void withCancellation(StreamObserver<Operation> responseObserver, ListenableFuture<Void> future) {
    addCallback(
        future,
        new FutureCallback<Void>() {
          @Override
          public void onSuccess(Void result) {
            responseObserver.onCompleted();
          }

          @Override
          public void onFailure(Throwable t) {
            if (!(t instanceof CancellationException)) {
              responseObserver.onError(Status.fromThrowable(t).asException());
            }
          }
        });
    Context.current().addListener(
        (context) -> future.cancel(false),
        directExecutor());
  }

  private Consumer<Operation> createWatcher(StreamObserver<Operation> responseObserver) {
    return (operation) -> {
      if (operation == null) {
        throw Status.NOT_FOUND.asRuntimeException();
      }
      responseObserver.onNext(operation);
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

    withCancellation(
        responseObserver,
        instance.watchOperation(
            operationName,
            createWatcher(responseObserver)));
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

    logExecute(instance.getName(), request);

    try {
      withCancellation(
          responseObserver,
          instance.execute(
              request.getActionDigest(),
              request.getSkipCacheLookup(),
              request.getExecutionPolicy(),
              request.getResultsCachePolicy(),
              TracingMetadataUtils.fromCurrentContext(),
              createWatcher(responseObserver)));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
