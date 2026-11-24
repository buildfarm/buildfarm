// Copyright 2017-2026 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.ac;

import static com.google.common.util.concurrent.Futures.immediateFuture;

import build.bazel.remote.execution.v2.ActionResult;
import build.buildfarm.common.DigestUtil.ActionKey;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.IOException;

public interface ActionCache {
  ListenableFuture<ActionResult> get(ActionKey actionKey);

  ListenableFuture<ActionResult> put(ActionKey actionKey, ActionResult actionResult)
      throws IOException, InterruptedException;

  void invalidate(ActionKey actionKey);

  // should never interact with delegates, might be an internal-only implementation detail
  void readThrough(ActionKey actionKey, ActionResult actionResult);

  default void stop() {}

  // intended as the last cache on a chain of delegation
  final class NullActionCache implements ActionCache {
    @Override
    public ListenableFuture<ActionResult> get(ActionKey actionKey) {
      return immediateFuture(null);
    }

    @Override
    public ListenableFuture<ActionResult> put(ActionKey actionKey, ActionResult actionResult) {
      return immediateFuture(null);
    }

    @Override
    public void invalidate(ActionKey actionKey) {}

    @Override
    public void readThrough(ActionKey actionKey, ActionResult actionResult) {}
  }
}
