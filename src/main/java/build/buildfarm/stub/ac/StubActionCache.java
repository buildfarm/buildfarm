// Copyright 2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.stub.ac;

import static com.google.common.util.concurrent.Futures.catching;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import build.bazel.remote.execution.v2.ActionCacheGrpc.ActionCacheFutureStub;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.GetActionResultRequest;
import build.bazel.remote.execution.v2.UpdateActionResultRequest;
import build.buildfarm.ac.ActionCache;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.v1test.Digest;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.function.Supplier;

public class StubActionCache implements ActionCache {
  private final Supplier<ActionCacheFutureStub> stubSupplier;
  private final String instanceName;
  private final boolean readOnly;

  public StubActionCache(
      Supplier<ActionCacheFutureStub> stubSupplier, String instanceName, boolean readOnly) {
    this.stubSupplier = stubSupplier;
    this.instanceName = instanceName;
    this.readOnly = readOnly;
  }

  @Override
  public ListenableFuture<ActionResult> get(ActionKey actionKey) {
    Digest actionDigest = actionKey.getDigest();
    return catching(
        stubSupplier
            .get()
            .getActionResult(
                GetActionResultRequest.newBuilder()
                    .setInstanceName(instanceName)
                    .setActionDigest(DigestUtil.toDigest(actionDigest))
                    .setDigestFunction(actionDigest.getDigestFunction())
                    .build()),
        StatusRuntimeException.class,
        (e) -> {
          if (e.getStatus().equals(Status.NOT_FOUND)) {
            return null;
          }
          throw e;
        },
        directExecutor());
  }

  @Override
  public ListenableFuture<ActionResult> put(ActionKey actionKey, ActionResult actionResult)
      throws InterruptedException {
    if (readOnly) {
      return immediateFuture(actionResult);
    }
    Digest actionDigest = actionKey.getDigest();
    return stubSupplier
        .get()
        .updateActionResult(
            UpdateActionResultRequest.newBuilder()
                .setInstanceName(instanceName)
                .setActionDigest(DigestUtil.toDigest(actionDigest))
                .setDigestFunction(actionDigest.getDigestFunction())
                .setActionResult(actionResult)
                .build());
  }

  @Override
  public void invalidate(ActionKey actionKey) {
    // unsupported
  }

  @Override
  public void readThrough(ActionKey actionKey, ActionResult actionResult) {
    throw new UnsupportedOperationException();
  }
}
