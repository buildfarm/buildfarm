package build.buildfarm.worker.persistent;

import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse;
import persistent.common.CtxAround;

public class ResponseCtx implements CtxAround<WorkResponse> {
  public final WorkResponse response;

  public final String errorString;

  public ResponseCtx(WorkResponse response, String errorString) {
    this.response = response;
    this.errorString = errorString;
  }

  @Override
  public WorkResponse get() {
    return response;
  }
}
