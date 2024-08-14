package persistent.bazel.processes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import persistent.bazel.client.PersistentWorker;
import persistent.bazel.client.WorkerKey;
import persistent.common.processes.JavaProcessWrapper;
import persistent.testutil.ProcessUtils;
import persistent.testutil.WorkerUtils;

@RunWith(JUnit4.class)
public class PersistentWorkerTest {

  static WorkResponse sendAddRequest(PersistentWorker worker, Path stdErrLog, int x, int y) throws IOException {
    ImmutableList<String> arguments = ImmutableList.of(String.valueOf(x), String.valueOf(y));

    WorkRequest request = WorkRequest.newBuilder()
        .addAllArguments(arguments)
        .setRequestId(0)
        .build();

    WorkResponse response;
    try {
      response = worker.doWork(request);
    } catch (Exception e) {
      System.err.println(e.getMessage());
      System.err.println(Files.readAllLines(stdErrLog));
      throw e;
    }
    return response;
  }

  @SuppressWarnings("CheckReturnValue")
  @Test
  public void endToEndAdder() throws Exception {
    Path workDir = Files.createTempDirectory("test-workdir-");

    String filename = "adder-bin_deploy.jar";

    Path jarPath = ProcessUtils.retrieveFileResource(
        getClass().getClassLoader(),
        filename,
        workDir.resolve(filename)
    );

    ImmutableList<String> initCmd = ImmutableList.of(
        JavaProcessWrapper.CURRENT_JVM_COMMAND,
        "-cp",
        jarPath.toString(),
        "adder.Adder",
        "--persistent_worker"
    );

    WorkerKey key = WorkerUtils.emptyWorkerKey(workDir, initCmd);

    Path stdErrLog = workDir.resolve("test-err.log");
    PersistentWorker worker = new PersistentWorker(key, "worker-dir");

    WorkResponse response = sendAddRequest(worker, stdErrLog, 2, 4);

    Assert.assertEquals(response.getOutput(), "6");
    Assert.assertEquals(response.getExitCode(), 0);
    Assert.assertEquals(worker.getExitValue(), Optional.empty()); // Not yet exited

    WorkResponse response2 = sendAddRequest(worker, stdErrLog, 13, 37);

    Assert.assertEquals(response2.getOutput(), "50");
    Assert.assertEquals(response2.getExitCode(), 0);
    Assert.assertEquals(worker.getExitValue(), Optional.empty()); // Not yet exited
  }
}
