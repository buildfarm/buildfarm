// Copyright 2023 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.common;

import static com.google.common.truth.Truth.assertThat;

import build.bazel.remote.execution.v2.Command;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * @class CommandUtilsTest
 * @brief tests utility functions for working with Commands
 */
@RunWith(JUnit4.class)
public class CommandUtilsTest {
  // Function under test: getResolvedOutputPaths
  // Reason for testing: check path results
  // Failure explanation: paths are not captured or resolved correctly
  @Test
  public void getResolvedOutputPathsCheckOutputPaths() {
    // ARRANGE
    Command command = Command.newBuilder().addOutputPaths("foo0").addOutputPaths("foo1").build();

    // ACT
    List<Path> paths = CommandUtils.getResolvedOutputPaths(command, Paths.get("root"));

    // ASSERT
    assertThat(paths.get(0).toString()).isEqualTo(Paths.get("root/foo0").toString());
    assertThat(paths.get(1).toString()).isEqualTo(Paths.get("root/foo1").toString());
  }

  // Function under test: getResolvedOutputPaths
  // Reason for testing: check path results when output_paths is empty
  // Failure explanation: paths are not captured or resolved correctly
  @Test
  public void getResolvedOutputPathsCheckOutputFiles() {
    // ARRANGE
    Command command = Command.newBuilder().addOutputFiles("foo0").addOutputFiles("foo1").build();

    // ACT
    List<Path> paths = CommandUtils.getResolvedOutputPaths(command, Paths.get("root"));

    // ASSERT
    assertThat(paths.get(0).toString()).isEqualTo(Paths.get("root/foo0").toString());
    assertThat(paths.get(1).toString()).isEqualTo(Paths.get("root/foo1").toString());
  }
}
