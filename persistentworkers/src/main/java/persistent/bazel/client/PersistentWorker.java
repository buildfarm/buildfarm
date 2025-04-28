package persistent.bazel.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.Getter;
import persistent.bazel.processes.ProtoWorkerRW;
import persistent.common.Worker;
import persistent.common.processes.ProcessWrapper;

/**
 * Wraps a persistent worker process, using ProtoWorkerRW. The process is created with a working
 * directory under the execRoot of the WorkerKey.
 *
 * <p>Maintains the metadata of the underlying process, i.e. its WorkerKey and the command used to
 * run it.
 *
 * <p>Also takes care of the underlying process's environment, i.e. directories and files.
 */
public class PersistentWorker implements Worker<WorkRequest, WorkResponse> {
  /** Services supporting being run as persistent workers need to parse this flag */
  public static final String PERSISTENT_WORKER_FLAG = "--persistent_worker";

  private static final Logger logger = Logger.getLogger(PersistentWorker.class.getName());

  public static final String TOOL_INPUT_SUBDIR = "tool_inputs";

  @Getter private final WorkerKey key;
  @Getter private final ImmutableList<String> initCmd;
  @Getter private final Path execRoot;
  private final ProtoWorkerRW workerRW;

  public PersistentWorker(WorkerKey key, String workerDir) throws IOException {
    this.key = key;
    this.execRoot = key.getExecRoot().resolve(workerDir);
    this.initCmd =
        ImmutableList.<String>builder().addAll(key.getCmd()).addAll(key.getArgs()).build();

    Files.createDirectories(execRoot);

    Set<Path> workerFiles = ImmutableSet.copyOf(key.getWorkerFilesWithHashes().keySet());
    logger.log(
        Level.FINE,
        "Starting Worker["
            + key.getMnemonic()
            + "]<"
            + execRoot
            + ">("
            + initCmd
            + ") with files: \n"
            + workerFiles);

    ProcessWrapper processWrapper = new ProcessWrapper(execRoot, initCmd, key.getEnv());
    this.workerRW = new ProtoWorkerRW(processWrapper);
  }

  @Override
  public WorkResponse doWork(WorkRequest request) {
    WorkResponse response = null;
    try {
      logRequest(request);

      workerRW.write(request);
      response = workerRW.waitAndRead();

      logIfBadResponse(response);
    } catch (IOException e) {
      e.printStackTrace();
      logger.severe("IO Failing with : " + e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
      logger.severe("Failing with : " + e.getMessage());
    }
    return response;
  }

  public Optional<Integer> getExitValue() {
    ProcessWrapper pw = workerRW.getProcessWrapper();
    return pw != null && !pw.isAlive() ? Optional.of(pw.exitValue()) : Optional.empty();
  }

  public String getStdErr() {
    try {
      return this.workerRW.getProcessWrapper().getErrorString();
    } catch (IOException e) {
      e.printStackTrace();
      return "getStdErr Exception: " + e;
    }
  }

  public String flushStdErr() {
    try {
      return this.workerRW.getProcessWrapper().flushErrorString();
    } catch (IOException e) {
      e.printStackTrace();
      return "flushStdErr Exception: " + e;
    }
  }

  private void logRequest(WorkRequest request) {
    logger.log(
        Level.FINE,
        "doWork()------<" + "Got request with args: " + request.getArgumentsList() + "------>");
  }

  private void logIfBadResponse(WorkResponse response) throws IOException {
    int returnCode = response.getExitCode();
    if (returnCode != 0 && logger.isLoggable(Level.FINE)) {
      StringBuilder sb = new StringBuilder();
      sb.append("logBadResponse(err)");
      sb.append("\nResponse non-zero exit_code: ");
      sb.append(returnCode);
      sb.append("\nResponse output: ");
      sb.append(response.getOutput());
      sb.append("\n\tProcess stderr: ");
      String stderr = workerRW.getProcessWrapper().getErrorString();
      sb.append(stderr);
      logger.log(Level.FINE, sb.toString());
    }
  }

  @Override
  public void destroy() {
    this.workerRW.getProcessWrapper().destroy();
  }
}
