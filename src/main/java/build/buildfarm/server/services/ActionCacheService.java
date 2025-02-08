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
import build.buildfarm.common.config.BuildfarmConfigs;
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
  private static final Counter actionResultsMetric =
      Counter.build().name("action_results").help("Action results.").register();

  private final Instance instance;
  private final boolean isWritable;

  private static BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  public ActionCacheService(Instance instance) {
    this.instance = instance;
    this.isWritable = !configs.getServer().isActionCacheReadOnly();
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
              } else {
                responseObserver.onNext(actionResult);
                responseObserver.onCompleted();
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
            } catch (StatusRuntimeException e) {
              // ignore
            }
          }
        },
        directExecutor());
    actionResultsMetric.inc();
  }

  @Override
  public void updateActionResult(
      UpdateActionResultRequest request, StreamObserver<ActionResult> responseObserver) {
    // A user with write access to the cache can write anything, including malicious code and
    // binaries, which can then be returned to other users on cache lookups.  This is a security
    // concern.  To counteract this, we allow enforcing a policy where clients cannot upload to the
    // action cache.  In this paradigm, it is only the remote execution engine itself that populates
    // the action cache.
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
