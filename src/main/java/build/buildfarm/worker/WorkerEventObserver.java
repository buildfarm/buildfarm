package build.buildfarm.worker;

import com.google.longrunning.Operation;

public interface WorkerEventObserver {
  void onFetched(long numBytes);
  void onCreatedLinkedDirectory();
  void onCompletedExecution(Operation execution);
}
