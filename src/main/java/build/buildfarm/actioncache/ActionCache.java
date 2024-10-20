// Copyright 2020 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.actioncache;

import build.bazel.remote.execution.v2.ActionResult;
import build.buildfarm.common.DigestUtil.ActionKey;
import com.google.common.util.concurrent.ListenableFuture;

public interface ActionCache {
  ListenableFuture<ActionResult> get(ActionKey actionKey);

  void put(ActionKey actionKey, ActionResult actionResult) throws InterruptedException;

  void invalidate(ActionKey actionKey);

  void readThrough(ActionKey actionKey, ActionResult actionResult);
}
