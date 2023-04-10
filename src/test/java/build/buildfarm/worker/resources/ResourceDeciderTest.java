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

package build.buildfarm.worker.resources;

import static com.google.common.truth.Truth.assertThat;

import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Platform;
import build.buildfarm.common.ExecutionProperties;
import build.buildfarm.common.config.SandboxSettings;
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
    ResourceLimits limits =
        ResourceDecider.decideResourceLimitations(
            command,
            "worker",
            /* defaultMaxCores=*/ 0,
            /* onlyMulticoreTests=*/ false,
            /* limitGlobalExecution=*/ false,
            /* executeStageWidth=*/ 100,
            /* allowBringYourOwnContainer=*/ false,
            new SandboxSettings());

    // ASSERT
    assertThat(limits.cpu.min).isEqualTo(7);
    assertThat(limits.cpu.max).isEqualTo(14);
  }

  // Function under test: decideResourceLimitations
  // Reason for testing: test that cores are defaulted
  // Failure explanation: cores were not decided as expected
  @Test
  public void decideResourceLimitationsTestCoreSettingDefaultedOnNontest() throws Exception {
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
    int defaultMaxCores = 3;
    ResourceLimits limits =
        ResourceDecider.decideResourceLimitations(
            command,
            "worker",
            defaultMaxCores,
            /* onlyMulticoreTests=*/ true,
            /* limitGlobalExecution=*/ false,
            /* executeStageWidth=*/ 100,
            /* allowBringYourOwnContainer=*/ false,
            new SandboxSettings());

    // ASSERT
    assertThat(limits.cpu.min).isEqualTo(1);
    assertThat(limits.cpu.max).isEqualTo(defaultMaxCores);
  }

  // Function under test: decideResourceLimitations
  // Reason for testing: test that claims are >0 despite min-cores == 0.
  // Failure explanation: claims were not >0 as expected
  @Test
  public void decideResourceLimitationsEnsureClaimsOne() throws Exception {
    // ARRANGE
    Command command =
        Command.newBuilder()
            .setPlatform(
                Platform.newBuilder()
                    .addProperties(
                        Platform.Property.newBuilder().setName("min-cores").setValue("0"))
                    .addProperties(
                        Platform.Property.newBuilder().setName("max-cores").setValue("0")))
            .build();

    // ACT
    ResourceLimits limits =
        ResourceDecider.decideResourceLimitations(
            command,
            "worker",
            /* defaultMaxCores=*/ 0,
            /* onlyMulticoreTests=*/ false,
            /* limitGlobalExecution=*/ false,
            /* executeStageWidth=*/ 100,
            /* allowBringYourOwnContainer=*/ false,
            new SandboxSettings());

    // ASSERT
    assertThat(limits.cpu.claimed).isGreaterThan(0);
  }

  // Function under test: decideResourceLimitations
  // Reason for testing: test that we limit cpu if limitGlobalExecution is given.
  // Failure explanation: expected limit flag set.
  @Test
  public void decideResourceLimitationsEnsureLimitGlobalSet() throws Exception {
    // ARRANGE
    Command command =
        Command.newBuilder()
            .setPlatform(
                Platform.newBuilder()
                    .addProperties(
                        Platform.Property.newBuilder().setName("min-cores").setValue("0"))
                    .addProperties(
                        Platform.Property.newBuilder().setName("max-cores").setValue("0")))
            .build();

    // ACT
    ResourceLimits limits =
        ResourceDecider.decideResourceLimitations(
            command,
            "worker",
            /* defaultMaxCores=*/ 0,
            /* onlyMulticoreTests=*/ false,
            /* limitGlobalExecution=*/ true,
            /* executeStageWidth=*/ 100,
            /* allowBringYourOwnContainer=*/ false,
            new SandboxSettings());

    // ASSERT
    assertThat(limits.cpu.limit).isTrue();
  }

  // Function under test: decideResourceLimitations
  // Reason for testing: test that we do not limit cpu if globalLimitExecution is false.
  // Failure explanation: Did not expect limit flag set.
  @Test
  public void decideResourceLimitationsEnsureNoLimitNoGlobalSet() throws Exception {
    // ARRANGE
    Command command =
        Command.newBuilder()
            .setPlatform(
                Platform.newBuilder()
                    .addProperties(
                        Platform.Property.newBuilder().setName("min-cores").setValue("0"))
                    .addProperties(
                        Platform.Property.newBuilder().setName("max-cores").setValue("0")))
            .build();

    // ACT
    ResourceLimits limits =
        ResourceDecider.decideResourceLimitations(
            command,
            "worker",
            /* defaultMaxCores=*/ 0,
            /* onlyMulticoreTests=*/ false,
            /* limitGlobalExecution=*/ false,
            /* executeStageWidth=*/ 100,
            /* allowBringYourOwnContainer=*/ false,
            new SandboxSettings());

    // ASSERT
    assertThat(limits.cpu.limit).isFalse();
  }

  // Function under test: decideResourceLimitations
  // Reason for testing: test that claims are set to the specified minimum
  // Failure explanation: claims were not the same as minimum
  @Test
  public void decideResourceLimitationsEnsureClaimsAreMin() throws Exception {
    // ARRANGE
    Command command =
        Command.newBuilder()
            .setPlatform(
                Platform.newBuilder()
                    .addProperties(
                        Platform.Property.newBuilder().setName("min-cores").setValue("3"))
                    .addProperties(
                        Platform.Property.newBuilder().setName("max-cores").setValue("6")))
            .build();

    // ACT
    ResourceLimits limits =
        ResourceDecider.decideResourceLimitations(
            command,
            "worker",
            /* defaultMaxCores=*/ 0,
            /* onlyMulticoreTests=*/ false,
            /* limitGlobalExecution=*/ false,
            /* executeStageWidth=*/ 100,
            /* allowBringYourOwnContainer=*/ false,
            new SandboxSettings());

    // ASSERT
    assertThat(limits.cpu.claimed).isEqualTo(3);
  }

  // Function under test: decideResourceLimitations
  // Reason for testing: test that mem constraints can be set
  // Failure explanation: mem limits were not decided as expected
  @Test
  public void decideResourceLimitationsTestMemSetting() throws Exception {
    // ARRANGE
    Command command =
        Command.newBuilder()
            .setPlatform(
                Platform.newBuilder()
                    .addProperties(Platform.Property.newBuilder().setName("min-mem").setValue("5"))
                    .addProperties(
                        Platform.Property.newBuilder().setName("max-mem").setValue("10")))
            .build();

    // ACT
    ResourceLimits limits =
        ResourceDecider.decideResourceLimitations(
            command,
            "worker",
            /* defaultMaxCores=*/ 0,
            /* onlyMulticoreTests=*/ false,
            /* limitGlobalExecution=*/ false,
            /* executeStageWidth=*/ 100,
            /* allowBringYourOwnContainer=*/ false,
            new SandboxSettings());

    // ASSERT
    assertThat(limits.mem.min).isEqualTo(5);
    assertThat(limits.mem.max).isEqualTo(10);
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
    ResourceLimits limits =
        ResourceDecider.decideResourceLimitations(
            command,
            "worker",
            /* defaultMaxCores=*/ 0,
            /* onlyMulticoreTests=*/ false,
            /* limitGlobalExecution=*/ false,
            /* executeStageWidth=*/ 100,
            /* allowBringYourOwnContainer=*/ false,
            new SandboxSettings());

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
    ResourceLimits limits =
        ResourceDecider.decideResourceLimitations(
            command,
            "worker",
            /* defaultMaxCores=*/ 0,
            /* onlyMulticoreTests=*/ false,
            /* limitGlobalExecution=*/ false,
            /* executeStageWidth=*/ 100,
            /* allowBringYourOwnContainer=*/ false,
            new SandboxSettings());

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
    ResourceLimits limits =
        ResourceDecider.decideResourceLimitations(
            command,
            "worker",
            /* defaultMaxCores=*/ 0,
            /* onlyMulticoreTests=*/ false,
            /* limitGlobalExecution=*/ false,
            /* executeStageWidth=*/ 100,
            /* allowBringYourOwnContainer=*/ false,
            new SandboxSettings());

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
    ResourceLimits limits =
        ResourceDecider.decideResourceLimitations(
            command,
            "worker",
            /* defaultMaxCores=*/ 0,
            /* onlyMulticoreTests=*/ false,
            /* limitGlobalExecution=*/ false,
            /* executeStageWidth=*/ 100,
            /* allowBringYourOwnContainer=*/ false,
            new SandboxSettings());

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
    ResourceLimits limits =
        ResourceDecider.decideResourceLimitations(
            command,
            "worker",
            /* defaultMaxCores=*/ 0,
            /* onlyMulticoreTests=*/ false,
            /* limitGlobalExecution=*/ false,
            /* executeStageWidth=*/ 100,
            /* allowBringYourOwnContainer=*/ false,
            new SandboxSettings());

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
    ResourceLimits limits =
        ResourceDecider.decideResourceLimitations(
            command,
            "worker",
            /* defaultMaxCores=*/ 0,
            /* onlyMulticoreTests=*/ false,
            /* limitGlobalExecution=*/ false,
            /* executeStageWidth=*/ 100,
            /* allowBringYourOwnContainer=*/ false,
            new SandboxSettings());

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
    ResourceLimits limits =
        ResourceDecider.decideResourceLimitations(
            command,
            "worker",
            /* defaultMaxCores=*/ 0,
            /* onlyMulticoreTests=*/ false,
            /* limitGlobalExecution=*/ false,
            /* executeStageWidth=*/ 100,
            /* allowBringYourOwnContainer=*/ false,
            new SandboxSettings());

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
    ResourceLimits limits =
        ResourceDecider.decideResourceLimitations(
            command,
            "worker",
            /* defaultMaxCores=*/ 0,
            /* onlyMulticoreTests=*/ false,
            /* limitGlobalExecution=*/ false,
            /* executeStageWidth=*/ 100,
            /* allowBringYourOwnContainer=*/ false,
            new SandboxSettings());

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
    ResourceLimits limits =
        ResourceDecider.decideResourceLimitations(
            command,
            "worker",
            /* defaultMaxCores=*/ 0,
            /* onlyMulticoreTests=*/ false,
            /* limitGlobalExecution=*/ false,
            /* executeStageWidth=*/ 100,
            /* allowBringYourOwnContainer=*/ false,
            new SandboxSettings());

    // ASSERT
    assertThat(limits.extraEnvironmentVariables.size()).isEqualTo(1);
    assertThat(limits.extraEnvironmentVariables.containsKey("foo")).isTrue();
    assertThat(limits.extraEnvironmentVariables.get("foo")).isEqualTo("");
  }

  // Function under test: decideResourceLimitations
  // Reason for testing: we can parse out a positive bool for "before execution debugging"
  // Failure explanation: the bool was not parsed as true like we would have expected
  @Test
  public void decideResourceLimitationsTestDebugBeforeParse() throws Exception {
    // ARRANGE
    Command command =
        Command.newBuilder()
            .addEnvironmentVariables(
                Command.EnvironmentVariable.newBuilder()
                    .setName("XML_OUTPUT_FILE")) // make action look like test
            .setPlatform(
                Platform.newBuilder()
                    .addProperties(
                        Platform.Property.newBuilder()
                            .setName("debug-before-execution")
                            .setValue("true")))
            .build();

    // ACT
    ResourceLimits limits =
        ResourceDecider.decideResourceLimitations(
            command,
            "worker",
            /* defaultMaxCores=*/ 0,
            /* onlyMulticoreTests=*/ false,
            /* limitGlobalExecution=*/ false,
            /* executeStageWidth=*/ 100,
            /* allowBringYourOwnContainer=*/ false,
            new SandboxSettings());

    // ASSERT
    assertThat(limits.debugBeforeExecution).isTrue();
  }

  // Function under test: decideResourceLimitations
  // Reason for testing: we can parse out a positive bool for "after execution debugging"
  // Failure explanation: the bool was not parsed as true like we would have expected
  @Test
  public void decideResourceLimitationsTestDebugAfterParse() throws Exception {
    // ARRANGE
    Command command =
        Command.newBuilder()
            .addEnvironmentVariables(
                Command.EnvironmentVariable.newBuilder()
                    .setName("XML_OUTPUT_FILE")) // make action look like test
            .setPlatform(
                Platform.newBuilder()
                    .addProperties(
                        Platform.Property.newBuilder()
                            .setName("debug-after-execution")
                            .setValue("true")))
            .build();

    // ACT
    ResourceLimits limits =
        ResourceDecider.decideResourceLimitations(
            command,
            "worker",
            /* defaultMaxCores=*/ 0,
            /* onlyMulticoreTests=*/ false,
            /* limitGlobalExecution=*/ false,
            /* executeStageWidth=*/ 100,
            /* allowBringYourOwnContainer=*/ false,
            new SandboxSettings());

    // ASSERT
    assertThat(limits.debugAfterExecution).isTrue();
  }

  // Function under test: decideResourceLimitations
  // Reason for testing: if we provide an invalid boolean value, it is stored as false
  // Failure explanation: the value was not parsed gracefully or was somehow interpreted as true
  @Test
  public void decideResourceLimitationsTestInvalidDebugParse() throws Exception {
    // ARRANGE
    Command command =
        Command.newBuilder()
            .addEnvironmentVariables(
                Command.EnvironmentVariable.newBuilder()
                    .setName("XML_OUTPUT_FILE")) // make action look like test
            .setPlatform(
                Platform.newBuilder()
                    .addProperties(
                        Platform.Property.newBuilder()
                            .setName("debug-before-execution")
                            .setValue("BAD_FORMAT")))
            .build();

    // ACT
    ResourceLimits limits =
        ResourceDecider.decideResourceLimitations(
            command,
            "worker",
            /* defaultMaxCores=*/ 0,
            /* onlyMulticoreTests=*/ false,
            /* limitGlobalExecution=*/ false,
            /* executeStageWidth=*/ 100,
            /* allowBringYourOwnContainer=*/ false,
            new SandboxSettings());

    // ASSERT
    assertThat(limits.debugBeforeExecution).isFalse();
  }

  // Function under test: decideResourceLimitations
  // Reason for testing: The worker name is captured
  // Failure explanation: The worker name is not being captured in the returned value.
  @Test
  public void decideResourceLimitationsTestWorkerName() throws Exception {
    // ARRANGE
    Command command = Command.newBuilder().build();

    // ACT
    ResourceLimits limits =
        ResourceDecider.decideResourceLimitations(
            command,
            "foo",
            /* defaultMaxCores=*/ 0,
            /* onlyMulticoreTests=*/ false,
            /* limitGlobalExecution=*/ false,
            /* executeStageWidth=*/ 100,
            /* allowBringYourOwnContainer=*/ false,
            new SandboxSettings());

    // ASSERT
    assertThat(limits.workerName).isEqualTo("foo");
  }

  // Function under test: decideResourceLimitations
  // Reason for testing: Sandbox is not selected based on sandbox settings.
  // Failure explanation: The sandbox should be off by default.
  @Test
  public void decideResourceLimitationsSanboxOffDefault() throws Exception {
    // ARRANGE
    Command command = Command.newBuilder().build();

    // ACT
    ResourceLimits limits =
        ResourceDecider.decideResourceLimitations(
            command,
            "foo",
            /* defaultMaxCores=*/ 0,
            /* onlyMulticoreTests=*/ false,
            /* limitGlobalExecution=*/ false,
            /* executeStageWidth=*/ 100,
            /* allowBringYourOwnContainer=*/ false,
            new SandboxSettings());

    // ASSERT
    assertThat(limits.useLinuxSandbox).isFalse();
  }

  // Function under test: decideResourceLimitations
  // Reason for testing: Sandbox is selected based on sandbox settings.
  // Failure explanation: The sandbox should have been selected.
  @Test
  public void decideResourceLimitationsAlwaysUseSandbox() throws Exception {
    // ARRANGE
    Command command = Command.newBuilder().build();
    SandboxSettings sandboxSettings = new SandboxSettings();
    sandboxSettings.alwaysUse = true;

    // ACT
    ResourceLimits limits =
        ResourceDecider.decideResourceLimitations(
            command,
            "foo",
            /* defaultMaxCores=*/ 0,
            /* onlyMulticoreTests=*/ false,
            /* limitGlobalExecution=*/ false,
            /* executeStageWidth=*/ 100,
            /* allowBringYourOwnContainer=*/ false,
            sandboxSettings);

    // ASSERT
    assertThat(limits.useLinuxSandbox).isTrue();
  }

  // Function under test: decideResourceLimitations
  // Reason for testing: Sandbox is selected based on sandbox settings.
  // Failure explanation: The sandbox should have been selected.
  @Test
  public void decideResourceLimitationsSandboxChosenViaBlockNetwork() throws Exception {
    // ARRANGE
    Command command =
        Command.newBuilder()
            .setPlatform(
                Platform.newBuilder()
                    .addProperties(
                        Platform.Property.newBuilder()
                            .setName(ExecutionProperties.BLOCK_NETWORK)
                            .setValue("true")))
            .build();
    SandboxSettings sandboxSettings = new SandboxSettings();
    sandboxSettings.selectForBlockNetwork = true;

    // ACT
    ResourceLimits limits =
        ResourceDecider.decideResourceLimitations(
            command,
            "foo",
            /* defaultMaxCores=*/ 0,
            /* onlyMulticoreTests=*/ false,
            /* limitGlobalExecution=*/ false,
            /* executeStageWidth=*/ 100,
            /* allowBringYourOwnContainer=*/ false,
            sandboxSettings);

    // ASSERT
    assertThat(limits.useLinuxSandbox).isTrue();
  }

  // Function under test: decideResourceLimitations
  // Reason for testing: Sandbox is NOT selected based on sandbox settings.
  // Failure explanation: The sandbox should not have been selected.
  @Test
  public void decideResourceLimitationsSandboxNotChosenViaBlockNetwork() throws Exception {
    // ARRANGE
    Command command =
        Command.newBuilder()
            .setPlatform(
                Platform.newBuilder()
                    .addProperties(
                        Platform.Property.newBuilder()
                            .setName(ExecutionProperties.TMPFS)
                            .setValue("true")))
            .build();
    SandboxSettings sandboxSettings = new SandboxSettings();
    sandboxSettings.selectForBlockNetwork = true;

    // ACT
    ResourceLimits limits =
        ResourceDecider.decideResourceLimitations(
            command,
            "foo",
            /* defaultMaxCores=*/ 0,
            /* onlyMulticoreTests=*/ false,
            /* limitGlobalExecution=*/ false,
            /* executeStageWidth=*/ 100,
            /* allowBringYourOwnContainer=*/ false,
            sandboxSettings);

    // ASSERT
    assertThat(limits.useLinuxSandbox).isFalse();
  }

  // Function under test: decideResourceLimitations
  // Reason for testing: Sandbox is selected based on sandbox settings.
  // Failure explanation: The sandbox should have been selected.
  @Test
  public void decideResourceLimitationsSandboxChosenViaTmpFs() throws Exception {
    // ARRANGE
    Command command =
        Command.newBuilder()
            .setPlatform(
                Platform.newBuilder()
                    .addProperties(
                        Platform.Property.newBuilder()
                            .setName(ExecutionProperties.TMPFS)
                            .setValue("true")))
            .build();
    SandboxSettings sandboxSettings = new SandboxSettings();
    sandboxSettings.selectForTmpFs = true;

    // ACT
    ResourceLimits limits =
        ResourceDecider.decideResourceLimitations(
            command,
            "foo",
            /* defaultMaxCores=*/ 0,
            /* onlyMulticoreTests=*/ false,
            /* limitGlobalExecution=*/ false,
            /* executeStageWidth=*/ 100,
            /* allowBringYourOwnContainer=*/ false,
            sandboxSettings);

    // ASSERT
    assertThat(limits.useLinuxSandbox).isTrue();
  }

  // Function under test: decideResourceLimitations
  // Reason for testing: Sandbox is NOT selected based on sandbox settings.
  // Failure explanation: The sandbox should not have been selected.
  @Test
  public void decideResourceLimitationsSandboxNotChosenViaTmpFs() throws Exception {
    // ARRANGE
    Command command =
        Command.newBuilder()
            .setPlatform(
                Platform.newBuilder()
                    .addProperties(
                        Platform.Property.newBuilder()
                            .setName(ExecutionProperties.BLOCK_NETWORK)
                            .setValue("true")))
            .build();
    SandboxSettings sandboxSettings = new SandboxSettings();
    sandboxSettings.selectForTmpFs = true;

    // ACT
    ResourceLimits limits =
        ResourceDecider.decideResourceLimitations(
            command,
            "foo",
            /* defaultMaxCores=*/ 0,
            /* onlyMulticoreTests=*/ false,
            /* limitGlobalExecution=*/ false,
            /* executeStageWidth=*/ 100,
            /* allowBringYourOwnContainer=*/ false,
            sandboxSettings);

    // ASSERT
    assertThat(limits.useLinuxSandbox).isFalse();
  }
}
