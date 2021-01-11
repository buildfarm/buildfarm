// Copyright 2020 The Bazel Authors. All rights reserved.
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

import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Platform;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * @class ResourceDeciderTest
 * @brief tests Decide the resource limitations for a given command.
 * @details Platform properties from specified exec_properties are taken into account as well as
 *     global buildfarm configuration.
 */
@RunWith(JUnit4.class)
public class ResourceDeciderTest {

  // Function under test: decideResourceLimitations
  // Reason for testing: test that cores can be set
  // Failure explanation: cores were not decided as expected
  @Test
  public void decideResourceLimitationsTestCoreSetting() throws Exception {

    // ARRANGE
    Command command =
        Command.newBuilder()
            .setPlatform(
                Platform.newBuilder()
                    .addProperties(
                        Platform.Property.newBuilder().setName("min-cores").setValue("7"))
                    .addProperties(
                        Platform.Property.newBuilder().setName("max-cores").setValue("14")))
            .build();

    // ACT
    ResourceLimits limits = ResourceDecider.decideResourceLimitations(command, false, 100);

    // ASSERT
    assertThat(limits.cpu.min).isEqualTo(7);
    assertThat(limits.cpu.max).isEqualTo(14);
  }

  // Function under test: decideResourceLimitations
  // Reason for testing: test that cores are skipped
  // Failure explanation: cores were not decided as expected
  @Test
  public void decideResourceLimitationsTestCoreSettingSkippedOnNontest() throws Exception {

    // ARRANGE
    Command command =
        Command.newBuilder()
            .setPlatform(
                Platform.newBuilder()
                    .addProperties(
                        Platform.Property.newBuilder().setName("min-cores").setValue("7"))
                    .addProperties(
                        Platform.Property.newBuilder().setName("max-cores").setValue("14")))
            .build();

    // ACT
    ResourceLimits limits = ResourceDecider.decideResourceLimitations(command, true, 100);

    // ASSERT
    assertThat(limits.cpu.min).isEqualTo(1);
    assertThat(limits.cpu.max).isEqualTo(1);
  }

  // Function under test: decideResourceLimitations
  // Reason for testing: if the user does not pass extra environment variables via platform
  // properties they appear empty
  // Failure explanation: the parsing crashed or did not provide an empty map
  @Test
  public void decideResourceLimitationsTestDefaultEnvironmentParse() throws Exception {

    // ARRANGE
    Command command = Command.newBuilder().build();

    // ACT
    ResourceLimits limits = ResourceDecider.decideResourceLimitations(command, true, 100);

    // ASSERT
    assertThat(limits.extraEnvironmentVariables.isEmpty()).isTrue();
  }

  // Function under test: decideResourceLimitations
  // Reason for testing: if the user does not pass extra environment variables via platform
  // properties they appear empty
  // Failure explanation: the parsing crashed or did not provide an empty map
  @Test
  public void decideResourceLimitationsTestEmptyEnvironmentParse() throws Exception {

    // ARRANGE
    Command command =
        Command.newBuilder()
            .setPlatform(
                Platform.newBuilder()
                    .addProperties(Platform.Property.newBuilder().setName("env-vars").setValue("")))
            .build();

    // ACT
    ResourceLimits limits = ResourceDecider.decideResourceLimitations(command, true, 100);

    // ASSERT
    assertThat(limits.extraEnvironmentVariables.isEmpty()).isTrue();
  }

  // Function under test: decideResourceLimitations
  // Reason for testing: if the user passes an extra environment variable via platform properties
  // they should be parsed into the map
  // Failure explanation: the parsing crashed or the map was not correctly populated
  @Test
  public void decideResourceLimitationsTestSingleEnvironmentParse() throws Exception {

    // ARRANGE
    Command command =
        Command.newBuilder()
            .setPlatform(
                Platform.newBuilder()
                    .addProperties(
                        Platform.Property.newBuilder()
                            .setName("env-vars")
                            .setValue("{\"foo\": \"bar\"}")))
            .build();

    // ACT
    ResourceLimits limits = ResourceDecider.decideResourceLimitations(command, true, 100);

    // ASSERT
    assertThat(limits.extraEnvironmentVariables.size()).isEqualTo(1);
    assertThat(limits.extraEnvironmentVariables.containsKey("foo")).isTrue();
    assertThat(limits.extraEnvironmentVariables.get("foo")).isEqualTo("bar");
  }

  // Function under test: decideResourceLimitations
  // Reason for testing: if the user passes an extra environment variable via platform properties
  // they should be parsed into the map
  // Failure explanation: the parsing crashed or the map was not correctly populated
  @Test
  public void decideResourceLimitationsTestDoubleEnvironmentParse() throws Exception {

    // ARRANGE
    Command command =
        Command.newBuilder()
            .setPlatform(
                Platform.newBuilder()
                    .addProperties(
                        Platform.Property.newBuilder()
                            .setName("env-vars")
                            .setValue("{\"foo\": \"bar\", \"baz\": \"qux\"}")))
            .build();

    // ACT
    ResourceLimits limits = ResourceDecider.decideResourceLimitations(command, true, 100);

    // ASSERT
    assertThat(limits.extraEnvironmentVariables.size()).isEqualTo(2);
    assertThat(limits.extraEnvironmentVariables.containsKey("foo")).isTrue();
    assertThat(limits.extraEnvironmentVariables.get("foo")).isEqualTo("bar");
    assertThat(limits.extraEnvironmentVariables.containsKey("baz")).isTrue();
    assertThat(limits.extraEnvironmentVariables.get("baz")).isEqualTo("qux");
  }

  // Function under test: decideResourceLimitations
  // Reason for testing: when a user passes malformed json to the extra environment variables,
  // safely ignore it
  // Failure explanation: the parsing crashed or the map was not correctly populated
  @Test
  public void decideResourceLimitationsTestMalformedEnvironmentParse() throws Exception {

    // ARRANGE
    Command command =
        Command.newBuilder()
            .setPlatform(
                Platform.newBuilder()
                    .addProperties(
                        Platform.Property.newBuilder()
                            .setName("env-vars")
                            .setValue("{\"foo\": \"bar\", \"baz\": \"qux\"} xxxx")))
            .build();

    // ACT
    ResourceLimits limits = ResourceDecider.decideResourceLimitations(command, true, 100);

    // ASSERT
    assertThat(limits.extraEnvironmentVariables.size()).isEqualTo(0);
  }

  // Function under test: decideResourceLimitations
  // Reason for testing: test that environment variables can have their values resolved
  // Failure explanation: values were not resolved as expected
  @Test
  public void decideResourceLimitationsTestEnvironmentMustacheResolution() throws Exception {

    // ARRANGE
    Command command =
        Command.newBuilder()
            .setPlatform(
                Platform.newBuilder()
                    .addProperties(
                        Platform.Property.newBuilder().setName("min-cores").setValue("7"))
                    .addProperties(
                        Platform.Property.newBuilder().setName("max-cores").setValue("14"))
                    .addProperties(
                        Platform.Property.newBuilder()
                            .setName("env-vars")
                            .setValue(
                                "{\"foo\": \"{{limits.cpu.min}}\", \"bar\": \"{{limits.cpu.max}}\"}")))
            .build();

    // ACT
    ResourceLimits limits = ResourceDecider.decideResourceLimitations(command, false, 100);

    // ASSERT
    assertThat(limits.extraEnvironmentVariables.size()).isEqualTo(2);
    assertThat(limits.extraEnvironmentVariables.containsKey("foo")).isTrue();
    assertThat(limits.extraEnvironmentVariables.get("foo")).isEqualTo("7");
    assertThat(limits.extraEnvironmentVariables.containsKey("bar")).isTrue();
    assertThat(limits.extraEnvironmentVariables.get("bar")).isEqualTo("14");
  }

  // Function under test: decideResourceLimitations
  // Reason for testing: if the user passes an extra individual environment variable via platform
  // properties they should be parsed into the map
  // Failure explanation: the parsing was not done correctly and the variable was somehow ignored
  @Test
  public void decideResourceLimitationsTestIndividualEnvironmentVarParse() throws Exception {

    // ARRANGE
    Command command =
        Command.newBuilder()
            .setPlatform(
                Platform.newBuilder()
                    .addProperties(
                        Platform.Property.newBuilder().setName("env-var:foo").setValue("bar")))
            .build();

    // ACT
    ResourceLimits limits = ResourceDecider.decideResourceLimitations(command, true, 100);

    // ASSERT
    assertThat(limits.extraEnvironmentVariables.size()).isEqualTo(1);
    assertThat(limits.extraEnvironmentVariables.containsKey("foo")).isTrue();
    assertThat(limits.extraEnvironmentVariables.get("foo")).isEqualTo("bar");
  }

  // Function under test: decideResourceLimitations
  // Reason for testing: if the user passes two extra individual environment variable via platform
  // properties they should be parsed into the map
  // Failure explanation: the parsing was not done correctly and the variables were ignored for some
  // reason
  @Test
  public void decideResourceLimitationsTestTwoIndividualEnvironmentVarParse() throws Exception {

    // ARRANGE
    Command command =
        Command.newBuilder()
            .setPlatform(
                Platform.newBuilder()
                    .addProperties(
                        Platform.Property.newBuilder().setName("env-var:foo").setValue("bar"))
                    .addProperties(
                        Platform.Property.newBuilder().setName("env-var:baz").setValue("qux")))
            .build();

    // ACT
    ResourceLimits limits = ResourceDecider.decideResourceLimitations(command, true, 100);

    // ASSERT
    assertThat(limits.extraEnvironmentVariables.size()).isEqualTo(2);
    assertThat(limits.extraEnvironmentVariables.containsKey("foo")).isTrue();
    assertThat(limits.extraEnvironmentVariables.get("foo")).isEqualTo("bar");
    assertThat(limits.extraEnvironmentVariables.containsKey("baz")).isTrue();
    assertThat(limits.extraEnvironmentVariables.get("baz")).isEqualTo("qux");
  }

  // Function under test: decideResourceLimitations
  // Reason for testing: if the user passes an extra individual environment var with no value, it
  // should still be parsed into the map
  // Failure explanation: the parsing was not done correctly and the variable was ignored or the
  // value contents are wrong
  @Test
  public void decideResourceLimitationsTestEmptyEnvironmentVarParse() throws Exception {

    // ARRANGE
    Command command =
        Command.newBuilder()
            .setPlatform(
                Platform.newBuilder()
                    .addProperties(
                        Platform.Property.newBuilder().setName("env-var:foo").setValue("")))
            .build();

    // ACT
    ResourceLimits limits = ResourceDecider.decideResourceLimitations(command, true, 100);

    // ASSERT
    assertThat(limits.extraEnvironmentVariables.size()).isEqualTo(1);
    assertThat(limits.extraEnvironmentVariables.containsKey("foo")).isTrue();
    assertThat(limits.extraEnvironmentVariables.get("foo")).isEqualTo("");
  }
}
