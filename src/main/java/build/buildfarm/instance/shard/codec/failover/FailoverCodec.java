// Copyright 2025 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.instance.shard.codec.failover;

import build.buildfarm.common.redis.Codec;

public final class FailoverCodec {
  public static Codec create(Codec initial, Codec next) {
    return new Codec(
        new FailoverTranslator<>(initial.actionResult(), next.actionResult()),
        new FailoverTranslator<>(initial.executeEntry(), next.executeEntry()),
        new FailoverTranslator<>(initial.queueEntry(), next.queueEntry()),
        new FailoverTranslator<>(initial.execution(), next.execution()),
        new FailoverTranslator<>(initial.operationChange(), next.operationChange()),
        new FailoverTranslator<>(initial.dispatchedExecution(), next.dispatchedExecution()),
        new FailoverTranslator<>(initial.worker(), next.worker()),
        new FailoverTranslator<>(initial.workerChange(), next.workerChange()));
  }

  private FailoverCodec() {}
}
