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

package build.buildfarm.examples;

import static build.buildfarm.common.base.System.isWindows;

import build.buildfarm.common.config.BuildfarmConfigs;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

// Test that example config files can load properly
@RunWith(JUnit4.class)
@SuppressWarnings({"ProtoBuilderReturnValueIgnored", "ReturnValueIgnored"})
public class ExampleConfigsTest {
  @Before
  public void skipWindows() {
    org.junit.Assume.assumeFalse(isWindows());
  }

  @Test
  public void shardWorkerConfig() throws IOException {
    Path configPath =
        Path.of(System.getenv("TEST_SRCDIR"), "_main", "examples", "config.minimal.yml");
    BuildfarmConfigs configs = BuildfarmConfigs.getInstance();
    configs.loadConfigs(configPath);
  }

  @Test
  public void fullConfig() throws IOException {
    Path configPath = Path.of(System.getenv("TEST_SRCDIR"), "_main", "examples", "config.yml");
    BuildfarmConfigs configs = BuildfarmConfigs.getInstance();
    configs.loadConfigs(configPath);
  }
}
