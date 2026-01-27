// Copyright 2026 The Buildfarm Authors. All rights reserved.
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

import static com.google.common.truth.Truth.assertThat;

import build.bazel.remote.execution.v2.ActionResult;
import build.buildfarm.common.redis.StringTranslator;
import build.buildfarm.v1test.WorkerExecutedMetadata;
import com.google.devtools.build.lib.worker.WorkerProtocol.Input;
import com.google.protobuf.Any;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ActionResultTranslatorTest {
  @Test
  public void putActionResultPurgesUnknownAuxiliaryMetadatas() throws Exception {
    ActionResult.Builder builder = ActionResult.newBuilder();
    // the WorkerExecutedMetadata TYPE_REGISTRY puts a great number of types into the registry
    // action results cannot currently parse WorkerProtocol.Input, ensure this continues to be
    // true to represent a random proto type.
    builder
        .getExecutionMetadataBuilder()
        .addAuxiliaryMetadata(Any.pack(Input.getDefaultInstance()))
        .addAuxiliaryMetadata(Any.pack(WorkerExecutedMetadata.getDefaultInstance()));

    StringTranslator<ActionResult> actionResultTranslator = new ActionResultTranslator();

    String json = actionResultTranslator.print(builder.build());
    ActionResult actionResult = actionResultTranslator.parse(json);

    assertThat(actionResult.getExecutionMetadata().getAuxiliaryMetadataCount()).isEqualTo(1);
  }
}
