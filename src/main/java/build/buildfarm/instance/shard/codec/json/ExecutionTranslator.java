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

import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.buildfarm.v1test.QueuedOperationMetadata;
import com.google.longrunning.Operation;
import com.google.protobuf.Message.Builder;
import com.google.protobuf.util.JsonFormat.TypeRegistry;
import com.google.rpc.PreconditionFailure;
import lombok.extern.java.Log;

@Log
class ExecutionTranslator extends JsonTranslator<Operation> {
  static final TypeRegistry TYPE_REGISTRY =
      TypeRegistry.newBuilder()
          .add(ExecuteOperationMetadata.getDescriptor())
          .add(QueuedOperationMetadata.getDescriptor())
          .add(PreconditionFailure.getDescriptor())
          .build();

  ExecutionTranslator() {
    super(TYPE_REGISTRY, "Execution");
  }

  @Override
  protected Builder builder() {
    return Operation.newBuilder();
  }
}
