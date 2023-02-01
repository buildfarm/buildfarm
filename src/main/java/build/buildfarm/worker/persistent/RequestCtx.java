package build.buildfarm.worker.persistent;

import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest;
import com.google.protobuf.Duration;
import persistent.common.CtxAround;

public class RequestCtx implements CtxAround<WorkRequest> {
  public final WorkRequest request;

  public final WorkFilesContext filesContext;

  public final WorkerInputs workerInputs;

  public final Duration timeout;

  public RequestCtx(
      WorkRequest request, WorkFilesContext ctx, WorkerInputs workFiles, Duration timeout) {
    this.request = request;
    this.filesContext = ctx;
    this.workerInputs = workFiles;
    this.timeout = timeout;
  }

  @Override
  public WorkRequest get() {
    return request;
  }
}
