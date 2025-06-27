package persistent.bazel.processes;

import build.buildfarm.common.Claim;
import com.google.common.collect.ImmutableList;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse;
import java.io.IOException;
import java.nio.file.FileSystemException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
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
  static class StubClaim implements Claim {
    private final UserPrincipal owner;
    private boolean released = false;

    public StubClaim(UserPrincipal owner) {
      this.owner = owner;
    }

    @Override
    public Iterable<Entry<String, List<Object>>> getPools() {
      return Collections.emptyList();
    }

    public boolean isReleased() {
      return released;
    }

    @Override
    public UserPrincipal owner() {
      return owner;
    }

    @Override
    public void release(Stage stage) {
      released = true;
    }

    @Override
    public void release() {
      released = true;
    }
  }

  static WorkResponse sendAddRequest(PersistentWorker worker, Path stdErrLog, int x, int y)
      throws IOException {
    ImmutableList<String> arguments = ImmutableList.of(String.valueOf(x), String.valueOf(y));

    WorkRequest request =
        WorkRequest.newBuilder().addAllArguments(arguments).setRequestId(0).build();

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

    Path jarPath =
        ProcessUtils.retrieveFileResource(
            getClass().getClassLoader(), filename, workDir.resolve(filename));

    ImmutableList<String> initCmd =
        ImmutableList.of(
            JavaProcessWrapper.CURRENT_JVM_COMMAND,
            "-cp",
            jarPath.toString(),
            "adder.Adder",
            "--persistent_worker");

    WorkerKey key = WorkerUtils.emptyWorkerKey(workDir, initCmd);

    Path stdErrLog = workDir.resolve("test-err.log");
    PersistentWorker worker = new PersistentWorker(key, "worker-dir", null);

    WorkResponse response = sendAddRequest(worker, stdErrLog, 2, 4);

    Assert.assertEquals(response.getOutput(), "6");
    Assert.assertEquals(response.getExitCode(), 0);
    Assert.assertEquals(worker.getExitValue(), Optional.empty()); // Not yet exited

    WorkResponse response2 = sendAddRequest(worker, stdErrLog, 13, 37);

    Assert.assertEquals(response2.getOutput(), "50");
    Assert.assertEquals(response2.getExitCode(), 0);
    Assert.assertEquals(worker.getExitValue(), Optional.empty()); // Not yet exited
  }

  @Test
  public void execOwnerSet() throws Exception {
    Path workDir = Files.createTempDirectory("test-workdir-");

    String filename = "adder-bin_deploy.jar";

    Path jarPath =
        ProcessUtils.retrieveFileResource(
            getClass().getClassLoader(), filename, workDir.resolve(filename));

    ImmutableList<String> initCmd =
        ImmutableList.of(
            JavaProcessWrapper.CURRENT_JVM_COMMAND,
            "-cp",
            jarPath.toString(),
            "adder.Adder",
            "--persistent_worker");

    WorkerKey key = WorkerUtils.emptyWorkerKey(workDir, initCmd);

    UserPrincipalLookupService lookupService =
        FileSystems.getDefault().getUserPrincipalLookupService();

    UserPrincipal nobody = lookupService.lookupPrincipalByName("nobody");

    StubClaim nobodyClaim = new StubClaim(nobody);

    Path execRoot = workDir.resolve("worker-dir");

    try {
      PersistentWorker worker = new PersistentWorker(key, "worker-dir", nobodyClaim);

      Assert.assertEquals(Files.getOwner(execRoot), nobody);

      worker.destroy();

      Assert.assertTrue(nobodyClaim.isReleased());
    } catch (FileSystemException exception) {
      /*
       * It's likely that the current user doesn't have permission to change the owner of `execRoot`, in which case we
       * should ignore the exception and assume `PersistentWorker` attempted to change the owner of the correct directory.
       */
    }

    // Test that the claim is released when the worker is destroyed
    StubClaim meClaim =
        new StubClaim(lookupService.lookupPrincipalByName(System.getProperty("user.name")));

    PersistentWorker worker = new PersistentWorker(key, "worker-dir", meClaim);

    worker.destroy();

    Assert.assertTrue(meClaim.isReleased());
  }
}
