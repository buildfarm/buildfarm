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
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;

import build.bazel.remote.execution.v2.ActionCacheGrpc;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.GetActionResultRequest;
import build.bazel.remote.execution.v2.UpdateActionResultRequest;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.grpc.TracingMetadataUtils;
import build.buildfarm.instance.Instance;
import build.buildfarm.v1test.Digest;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.prometheus.client.Counter;
import java.util.logging.Level;
import javax.annotation.Nullable;
import lombok.extern.java.Log;

@Log
public class ActionCacheService extends ActionCacheGrpc.ActionCacheImplBase {
  private static final Counter requests =
      Counter.build().name("action_results").help("Action result requests.").register();
  private static final Counter kinds =
      Counter.build()
          .name("action_result_kind")
          .labelNames("kind")
          .help("Action result response kind: hit, miss, or code.")
          .register();
  private static final Counter cancellations =
      Counter.build()
          .name("action_results_cancelled")
          .help("Action result requests cancelled.")
          .register();

  private final Instance instance;
  private final boolean isWritable;

  public ActionCacheService(Instance instance, boolean isWritable) {
    this.instance = instance;
    this.isWritable = isWritable;
  }

  @Override
  public void getActionResult(
      GetActionResultRequest request, StreamObserver<ActionResult> responseObserver) {
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

    ListenableFuture<ActionResult> resultFuture =
        instance.getActionResult(
            DigestUtil.asActionKey(actionDigest), TracingMetadataUtils.fromCurrentContext());

    addCallback(
        resultFuture,
        new FutureCallback<>() {
          final ServerCallStreamObserver<ActionResult> call =
              (ServerCallStreamObserver<ActionResult>) responseObserver;

          @Override
          public void onSuccess(@Nullable ActionResult actionResult) {
            try {
              if (actionResult == null) {
                responseObserver.onError(Status.NOT_FOUND.asException());
                kinds.labels("miss").inc();
              } else {
                responseObserver.onNext(actionResult);
                responseObserver.onCompleted();
                kinds.labels("hit").inc();
              }
            } catch (StatusRuntimeException e) {
              onFailure(e);
            }
          }

          @SuppressWarnings("NullableProblems")
          @Override
          public void onFailure(Throwable t) {
            Status status = Status.fromThrowable(t);
            if (call.isCancelled()) {
              cancellations.inc();
              // no further logging/response required
              return;
            }
            if (status.getCode() != Status.Code.CANCELLED) {
              log.log(
                  Level.WARNING,
                  format(
                      "getActionResult(%s): %s",
                      request.getInstanceName(), DigestUtil.toString(actionDigest)),
                  t);
            }
            try {
              responseObserver.onError(status.asException());
              kinds.labels(status.getCode().toString().toLowerCase()).inc();
            } catch (StatusRuntimeException e) {
              // ignore
            }
          }
        },
        directExecutor());
    requests.inc();
  }

  @Override
  public void updateActionResult(
      UpdateActionResultRequest request, StreamObserver<ActionResult> responseObserver) {
    if (!isWritable) {
      responseObserver.onError(Status.PERMISSION_DENIED.asException());
      return;
    }

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

    ActionResult actionResult = request.getActionResult();
    try {
      instance.putActionResult(DigestUtil.asActionKey(actionDigest), actionResult);

      responseObserver.onNext(actionResult);
      responseObserver.onCompleted();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
