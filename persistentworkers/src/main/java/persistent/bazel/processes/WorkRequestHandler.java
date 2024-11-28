package persistent.bazel.processes;

import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.function.BiFunction;

/**
 * Persistent-worker-compatible tools should instantiate this class Reads WorkRequests, handles
 * them, and returns WorkResponses -- forever
 */
public class WorkRequestHandler {
  private final BiFunction<List<String>, PrintWriter, Integer> requestHandler;

  public WorkRequestHandler(BiFunction<List<String>, PrintWriter, Integer> callback) {
    this.requestHandler = callback;
  }

  public void writeToStream(WorkResponse workResponse, PrintStream out) throws IOException {
    synchronized (this) {
      ProtoWorkerRW.writeTo(workResponse, out);
    }
  }

  public int processForever(InputStream in, PrintStream out, PrintStream err) {
    while (true) {
      try {
        WorkRequest request = ProtoWorkerRW.readRequest(in);

        if (request == null) {
          break;
        } else {
          WorkResponse response = respondTo(request);
          writeToStream(response, out);
        }
      } catch (IOException e) {
        e.printStackTrace(err);
        return 1;
      }
    }
    return 0;
  }

  public WorkResponse respondTo(WorkRequest request) throws IOException {
    try (StringWriter outputWriter = new StringWriter();
        PrintWriter outputPrinter = new PrintWriter(outputWriter)) {
      int exitCode;
      try {
        exitCode = requestHandler.apply(request.getArgumentsList(), outputPrinter);
      } catch (RuntimeException e) {
        e.printStackTrace(outputPrinter);
        exitCode = 1;
      }

      outputPrinter.flush();
      String output = outputWriter.toString();

      if (exitCode != 0) {
        System.err.println(output);
      }
      return WorkResponse.newBuilder()
          .setOutput(output)
          .setExitCode(exitCode)
          .setRequestId(request.getRequestId())
          .build();
    }
  }
}
