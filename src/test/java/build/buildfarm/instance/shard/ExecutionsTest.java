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

package build.buildfarm.instance.shard;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import build.buildfarm.common.redis.StringTranslator;
import com.google.common.collect.ImmutableList;
import com.google.longrunning.Operation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import redis.clients.jedis.PipelineBase;
import redis.clients.jedis.Response;
import redis.clients.jedis.UnifiedJedis;

@RunWith(JUnit4.class)
public class ExecutionsTest {
  @Test
  public void missingOperationsInScanAreNotNull() {
    StringTranslator<Operation> translator = mock(StringTranslator.class);
    when(translator.parse(anyString())).thenReturn(null);

    Executions executions =
        new Executions(
            /* toolInvocations= */ null,
            translator,
            /* name= */ null,
            /* actionsName= */ null,
            /* timeout_s= */ -1,
            /* action_timeout_s= */ -1);

    Response<String> value = mock(Response.class);
    PipelineBase pipeline = mock(PipelineBase.class);
    when(pipeline.get(anyString())).thenReturn(value);

    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(jedis.pipelined()).thenReturn(pipeline);

    Operation operation =
        getOnlyElement(executions.get(jedis, ImmutableList.of("missing-operation")));
    assertThat(operation).isNotNull();
  }
}
