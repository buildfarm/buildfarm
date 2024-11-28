package adder;

import static com.google.common.truth.Truth.assertThat;
import static persistent.testutil.ProcessUtils.spawnPersistentWorkerProcess;

import com.google.devtools.build.lib.worker.WorkerProtocol;
import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import persistent.bazel.processes.ProtoWorkerRW;
import persistent.common.processes.JavaProcessWrapper;
import persistent.common.processes.ProcessWrapper;

@RunWith(JUnit4.class)
public class AdderTest {
  private JavaProcessWrapper spawnAdderProcess() throws IOException {
    return spawnPersistentWorkerProcess(System.getProperty("java.class.path"), Adder.class);
  }

  @SuppressWarnings("CheckReturnValue")
  @Test
  public void canInitAndDoWorkWithoutDying() throws Exception {
    ProcessWrapper pw;
    try (JavaProcessWrapper jpw = spawnAdderProcess()) {
      pw = jpw;
      ProtoWorkerRW rw = new ProtoWorkerRW(jpw);
      rw.write(WorkerProtocol.WorkRequest.newBuilder().addArguments("1").addArguments("3").build());

      assertThat(rw.waitAndRead().getOutput()).isEqualTo("4");
      assertThat(jpw.isAlive()).isTrue();
    }
    assertThat(pw).isNotNull();

    pw.waitFor();
    assertThat(pw.isAlive()).isFalse();
    assertThat(pw.exitValue()).isNotEqualTo(0);
  }

  @SuppressWarnings("CheckReturnValue")
  @Test
  public void canShutdownAdder() throws Exception {
    try (JavaProcessWrapper jpw = spawnAdderProcess()) {
      ProtoWorkerRW rw = new ProtoWorkerRW(jpw);
      rw.write(WorkerProtocol.WorkRequest.newBuilder().addArguments("stop!").build());
      jpw.waitFor();

      assertThat(jpw.isAlive()).isFalse();
      assertThat(jpw.exitValue()).isEqualTo(0);
    }
  }

  @SuppressWarnings("CheckReturnValue")
  @Test
  public void adderBadRequest() throws Exception {
    try (JavaProcessWrapper jpw = spawnAdderProcess()) {
      ProtoWorkerRW rw = new ProtoWorkerRW(jpw);
      rw.write(WorkerProtocol.WorkRequest.newBuilder().addArguments("bad request").build());
      jpw.waitFor();

      assertThat(jpw.isAlive()).isFalse();
      assertThat(jpw.exitValue()).isEqualTo(2);
    }
  }
}
