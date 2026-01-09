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

package build.buildfarm.instance.shard.codec.json;

import build.buildfarm.common.redis.Codec;
import build.buildfarm.v1test.DispatchedOperation;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.OperationChange;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.ShardWorker;
import build.buildfarm.v1test.WorkerChange;
import com.google.protobuf.Message.Builder;

public final class JsonCodec {
  public static final Codec CODEC =
      new Codec(
          new ActionResultTranslator(),
          new JsonTranslator<>("ExecuteEntry") {
            @Override
            protected Builder builder() {
              return ExecuteEntry.newBuilder();
            }
          },
          new JsonTranslator<>("QueueEntry") {
            @Override
            protected Builder builder() {
              return QueueEntry.newBuilder();
            }
          },
          new ExecutionTranslator(),
          new JsonTranslator<>(
              ExecutionTranslator.TYPE_REGISTRY, "OperationChange") { // as previously implemented
            @Override
            protected Builder builder() {
              return OperationChange.newBuilder();
            }
          },
          new JsonTranslator<>("DispatchedOperation") {
            @Override
            protected Builder builder() {
              return DispatchedOperation.newBuilder();
            }
          },
          new JsonTranslator<>("ShardWorker") {
            @Override
            protected Builder builder() {
              return ShardWorker.newBuilder();
            }
          },
          new JsonTranslator<>("WorkerChange") {
            @Override
            protected Builder builder() {
              return WorkerChange.newBuilder();
            }
          });

  private JsonCodec() {}
}
