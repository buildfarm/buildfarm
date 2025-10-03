package build.buildfarm.server.services;

import build.buildfarm.instance.Instance;
import build.buildfarm.v1test.*;
import io.grpc.stub.StreamObserver;
import lombok.extern.java.Log;

@Log
public class WorkerControlProxyService extends WorkerControlGrpc.WorkerControlImplBase {
  Instance instance;

  public WorkerControlProxyService(Instance instance) {
    this.instance = instance;
  }

  @Override
  public void pipelineChange(
      WorkerPipelineChangeRequest request,
      StreamObserver<WorkerPipelineChangeResponse> responseObserver) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void prepareWorkerForGracefulShutdown(
      PrepareWorkerForGracefulShutDownRequest request,
      StreamObserver<PrepareWorkerForGracefulShutDownRequestResults> responseObserver) {
    instance.shutDownWorkerGracefully(request.getWorkerName());
    responseObserver.onNext(PrepareWorkerForGracefulShutDownRequestResults.newBuilder().build());
    responseObserver.onCompleted();
  }
}
