// Copyright 2018 The Bazel Authors. All rights reserved.
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

package build.buildfarm.ac;

import static com.google.common.util.concurrent.Futures.catchingAsync;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import build.bazel.remote.execution.v2.ActionCacheGrpc;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.GetActionResultRequest;
import build.bazel.remote.execution.v2.UpdateActionResultRequest;
import build.buildfarm.common.DigestUtil.ActionKey;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.Status.Code;

public class GrpcActionCache implements ActionCache {
  private final String instanceName;
  private final Channel channel;

  public GrpcActionCache(String instanceName, Channel channel) {
    this.instanceName = instanceName;
    this.channel = channel;
  }

  @Override
  public ListenableFuture<ActionResult> get(ActionKey actionKey) {
    return catchingAsync(
        ActionCacheGrpc.newFutureStub(channel)
            .getActionResult(
                GetActionResultRequest.newBuilder()
                    .setInstanceName(instanceName)
                    .setActionDigest(actionKey.getDigest())
                    .build()),
        Exception.class,
        e -> {
          Status status = Status.fromThrowable(e);
          if (status.getCode() == Code.NOT_FOUND) {
            return immediateFuture(null);
          }
          return immediateFailedFuture(e);
        },
        directExecutor());
  }

  @Override
  public void put(ActionKey actionKey, ActionResult actionResult) {
    ActionCacheGrpc.newBlockingStub(channel)
        .updateActionResult(
            UpdateActionResultRequest.newBuilder()
                .setInstanceName(instanceName)
                .setActionDigest(actionKey.getDigest())
                .setActionResult(actionResult)
                .build());
  }
}
