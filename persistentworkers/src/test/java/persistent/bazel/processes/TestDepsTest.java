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
