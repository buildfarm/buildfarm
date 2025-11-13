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

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.ExecutedActionMetadata;
import build.buildfarm.v1test.WorkerExecutedMetadata;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message.Builder;
import com.google.protobuf.util.JsonFormat.TypeRegistry;
import lombok.extern.java.Log;

@Log
class ActionResultTranslator extends JsonTranslator<ActionResult> {
  private static final TypeRegistry TYPE_REGISTRY =
      TypeRegistry.newBuilder().add(WorkerExecutedMetadata.getDescriptor()).build();

  ActionResultTranslator() {
    super(TYPE_REGISTRY, "ActionResult");
  }

  @Override
  public String print(ActionResult actionResult) {
    String json = super.print(actionResult);
    if (json != null) {
      return json;
    }

    ActionResult.Builder builder = actionResult.toBuilder();
    ExecutedActionMetadata.Builder metadata =
        builder.getExecutionMetadataBuilder().clearAuxiliaryMetadata();
    for (Any auxiliaryMetadata : actionResult.getExecutionMetadata().getAuxiliaryMetadataList()) {
      try {
        // test the serialization capacity of this any
        printer.print(auxiliaryMetadata);
        // serialization passed, re-add it
        metadata.addAuxiliaryMetadata(auxiliaryMetadata);
      } catch (InvalidProtocolBufferException e) {
        // ignore
      }
    }

    json = super.print(builder.build());
    // purge must have succeeded, indicate as much to the server log
    log.warning("error printing auxiliary_metadata for action result, unrecognized content purged");
    return json;
  }

  @Override
  protected Builder builder() {
    return ActionResult.newBuilder();
  }
}
