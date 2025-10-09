// Copyright 2023-2025 The Buildfarm Authors. All rights reserved.
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
import static persistent.testutil.ProcessUtils.spawnPersistentWorkerProcess;

import com.google.devtools.build.lib.worker.WorkerProtocol;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import persistent.common.processes.JavaProcessWrapper;
import persistent.common.processes.ProcessWrapper;
import persistent.testutil.ProcessUtils;

@RunWith(JUnit4.class)
public class ProtoWorkerRWTest {
  // Similar to AdderTest; but spawns the process via a jar
  @SuppressWarnings("CheckReturnValue")
  @Test
  public void canAddMultipleTimesWithAdder() throws Exception {
    Path workDir = Files.createTempDirectory("test-workdir-");

    String filename = "adder-bin_deploy.jar";

    Path jarPath =
        ProcessUtils.retrieveFileResource(
            getClass().getClassLoader(), filename, workDir.resolve(filename));

    ProcessWrapper process;
    try (JavaProcessWrapper jpw = spawnPersistentWorkerProcess(jarPath, "adder.Adder")) {
      process = jpw;
      ProtoWorkerRW rw = new ProtoWorkerRW(jpw);

      assertThat(jpw.isAlive()).isTrue();

      rw.write(WorkerProtocol.WorkRequest.newBuilder().addArguments("1").addArguments("3").build());
      assertThat(rw.waitAndRead().getOutput()).isEqualTo("4");
      assertThat(jpw.isAlive()).isTrue();

      rw.write(WorkerProtocol.WorkRequest.newBuilder().addArguments("2").addArguments("5").build());
      assertThat(rw.waitAndRead().getOutput()).isEqualTo("7");
      assertThat(jpw.isAlive()).isTrue();
    }

    // try-with-resources done -> close() called, process should have been destroyForicbly()'d
    assertThat(process).isNotNull();
    process.waitFor();

    assertThat(process.isAlive()).isFalse();
    assertThat(process.exitValue()).isNotEqualTo(0);
  }
}
