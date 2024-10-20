// Copyright 2024 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.instance;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.truth.Truth.assertThat;

import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class InstanceBaseTest {
  @Test
  public void commutativeBinding() {
    String resource = "shard/executions/eb0b1396-65f3-43fb-a26c-ae0c690b77e7";

    Instance instance = new DummyInstanceBase("shard");

    UUID uuid = checkNotNull(instance.unbindExecutions(resource));
    assertThat(instance.bindExecutions(uuid)).isEqualTo(resource);
  }
}
