// Copyright 2023-2024 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package persistent.bazel.processes;

import static com.google.common.truth.Truth.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import persistent.testutil.ProcessUtils;

@RunWith(JUnit4.class)
public class TestDepsTest {
  @SuppressWarnings("CheckReturnValue")
  @Test
  public void canRetrieveAdderBinDeployJar() throws Exception {
    Path workDir = Files.createTempDirectory("test-workdir-");

    String filename = "adder-bin_deploy.jar";

    Path jarPath =
        ProcessUtils.retrieveFileResource(
            getClass().getClassLoader(), filename, workDir.resolve(filename));

    assertThat(Files.exists(jarPath)).isTrue();

    assertThat(Files.size(jarPath)).isAtLeast(11000000L); // at least 11mb
  }
}
