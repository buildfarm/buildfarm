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

package build.buildfarm.worker;

import static com.google.common.truth.Truth.assertThat;

import build.bazel.remote.execution.v2.Platform.Property;
import build.buildfarm.common.config.ExecutionWrapper;
import com.google.common.collect.ImmutableList;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ExecutorTest {
  @Test
  public void cgroupInterpolationUsesOperationId() {
    ExecutionWrapper wrapper = new ExecutionWrapper();
    wrapper.setPath("/bin/wrapper");
    wrapper.setArguments(new String[] {"--cgroup", "<cgroup>"});

    Map<String, Executor.Interpolator> interpolations =
        Executor.createInterpolations(
            /* claim= */ null,
            ImmutableList.of(),
            "projects/example/operations/01J8P3ZJ4C7E8N9Y0K2M");

    assertThat(Executor.transformWrapper(wrapper, interpolations))
        .containsExactly("/bin/wrapper", "--cgroup", "executions/operations/01J8P3ZJ4C7E8N9Y0K2M")
        .inOrder();
  }

  @Test
  public void cgroupInterpolationCannotBeOverriddenByPlatformProperty() {
    ExecutionWrapper wrapper = new ExecutionWrapper();
    wrapper.setPath("/bin/wrapper");
    wrapper.setArguments(new String[] {"<cgroup>"});
    Property cgroupProperty =
        Property.newBuilder().setName("cgroup").setValue("untrusted-cgroup").build();

    Map<String, Executor.Interpolator> interpolations =
        Executor.createInterpolations(
            /* claim= */ null,
            ImmutableList.of(cgroupProperty),
            "instances/default/operations/trusted-operation-id");

    assertThat(Executor.transformWrapper(wrapper, interpolations))
        .containsExactly("/bin/wrapper", "executions/operations/trusted-operation-id")
        .inOrder();
  }
}
