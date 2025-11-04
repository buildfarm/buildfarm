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

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.scheduleAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;

import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.ExecuteRequest;
import build.bazel.remote.execution.v2.ExecutionGrpc;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.WaitExecutionRequest;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.Watcher;
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.common.grpc.TracingMetadataUtils;
import build.buildfarm.instance.Instance;
import build.buildfarm.metrics.MetricsPublisher;
import build.buildfarm.metrics.log.LogMetricsPublisher;
import build.buildfarm.v1test.Digest;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.longrunning.Operation;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import javax.annotation.Nullable;
import lombok.extern.java.Log;

@Log
public class ExecutionService extends ExecutionGrpc.ExecutionImplBase {
  private final Instance instance;
  private final long keepaliveAfter; /* Seconds */
  private final ScheduledExecutorService keepaliveScheduler;
  private final MetricsPublisher metricsPublisher;

  private static BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  public ExecutionService(Instance instance, ScheduledExecutorService keepaliveScheduler) {
    this.instance = instance;
    this.keepaliveAfter = configs.getServer().getExecuteKeepaliveAfterSeconds();
    this.keepaliveScheduler = keepaliveScheduler;
    this.metricsPublisher = getMetricsPublisher();
  }

  private void withCancellation(
      ServerCallStreamObserver<Operation> serverCallStreamObserver, ListenableFuture<Void> future) {
    Context context = Context.current();
    addCallback(
        future,
        new FutureCallback<Void>() {
          boolean isCancelled() {
            return serverCallStreamObserver.isCancelled()
                || context.isCancelled()
                || future.isCancelled();
          }

          @Override
          public void onSuccess(Void result) {
            if (!isCancelled()) {
              try {
                serverCallStreamObserver.onCompleted();
              } catch (Exception e) {
                onFailure(e);
              }
            }
          }

          @SuppressWarnings("NullableProblems")
          @Override
          public void onFailure(Throwable t) {
            if (!isCancelled() && !(t instanceof CancellationException)) {
              log.log(Level.WARNING, "error occurred during execution", t);
              serverCallStreamObserver.onError(Status.fromThrowable(t).asException());
            }
          }
        },
        context.fixedContextExecutor(directExecutor()));
    serverCallStreamObserver.setOnCancelHandler(() -> future.cancel(false));
  }

  abstract class KeepaliveWatcher implements Watcher {
    private final ServerCallStreamObserver<Operation> serverCallStreamObserver;
    private ListenableFuture<?> keepaliveFuture = null;

    abstract void deliver(Operation operation);

    @SuppressWarnings({"unchecked", "rawtypes"})
    KeepaliveWatcher(ServerCallStreamObserver serverCallStreamObserver) {
      this.serverCallStreamObserver = serverCallStreamObserver;
      serverCallStreamObserver.setOnCancelHandler(this::cancel);
    }

    @Nullable
    ListenableFuture<?> getFuture() {
      return keepaliveFuture;
    }

    private synchronized void cancel() {
      if (keepaliveFuture != null) {
        keepaliveFuture.cancel(false);
        keepaliveFuture = null;
      }
    }

    @Override
    public final synchronized void observe(Operation operation) {
      cancel();
      if (operation == null) {
        throw Status.NOT_FOUND.withDescription("Operation not found.").asRuntimeException();
      }
      deliver(operation);
      keepaliveFuture = scheduleKeepalive(operation.getName());
    }

    private ListenableFuture<?> scheduleKeepalive(String operationName) {
      if (keepaliveAfter <= 0) {
        return null;
      }
      return scheduleAsync(
          () -> {
            deliverKeepalive(operationName);
            return immediateFuture(null);
          },
          keepaliveAfter,
          TimeUnit.SECONDS,
          keepaliveScheduler);
    }

    private synchronized void deliverKeepalive(String operationName) {
      if (!serverCallStreamObserver.isCancelled()) {
        try {
          deliver(Operation.newBuilder().setName(operationName).build());
          keepaliveFuture = scheduleKeepalive(operationName);
        } catch (IllegalStateException e) {
          if (!e.getMessage().equals("call is closed")) {
            throw e;
          }
        }
      }
    }
  }

  KeepaliveWatcher createWatcher(
      ServerCallStreamObserver<Operation> serverCallStreamObserver,
      RequestMetadata requestMetadata) {
    return new KeepaliveWatcher(serverCallStreamObserver) {
      @Override
      void deliver(Operation operation) {
        if (operation != null) {
          metricsPublisher.publishRequestMetadata(operation, requestMetadata);
        }
        serverCallStreamObserver.onNext(operation);
      }
    };
  }

  @Override
  public void waitExecution(
      WaitExecutionRequest request, StreamObserver<Operation> responseObserver) {
    UUID executionId = instance.unbindExecutions(request.getName());

    ServerCallStreamObserver<Operation> serverCallStreamObserver =
        (ServerCallStreamObserver<Operation>) responseObserver;
    withCancellation(
        serverCallStreamObserver,
        instance.watchExecution(
            executionId,
            createWatcher(serverCallStreamObserver, TracingMetadataUtils.fromCurrentContext())));
  }

  @Override
  public void execute(ExecuteRequest request, StreamObserver<Operation> responseObserver) {
    Digest actionDigest =
        DigestUtil.fromDigest(request.getActionDigest(), request.getDigestFunction());
    if (actionDigest.getDigestFunction() == DigestFunction.Value.UNKNOWN) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(
                  format(
                      "digest %s did not match any known types", DigestUtil.toString(actionDigest)))
              .asException());
      return;
    }
    ServerCallStreamObserver<Operation> serverCallStreamObserver =
        (ServerCallStreamObserver<Operation>) responseObserver;
    try {
      RequestMetadata requestMetadata = TracingMetadataUtils.fromCurrentContext();
      withCancellation(
          serverCallStreamObserver,
          instance.execute(
              actionDigest,
              request.getSkipCacheLookup(),
              request.getExecutionPolicy(),
              request.getResultsCachePolicy(),
              requestMetadata,
              createWatcher(serverCallStreamObserver, requestMetadata)));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static MetricsPublisher getMetricsPublisher() {
    return new LogMetricsPublisher();
  }
}
