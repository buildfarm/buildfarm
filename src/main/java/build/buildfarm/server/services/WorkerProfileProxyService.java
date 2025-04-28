package build.buildfarm.server.services;

import build.buildfarm.instance.Instance;
import build.buildfarm.v1test.WorkerListMessage;
import build.buildfarm.v1test.WorkerListRequest;
import build.buildfarm.v1test.WorkerProfileGrpc;
import build.buildfarm.v1test.WorkerProfileMessage;
import build.buildfarm.v1test.WorkerProfileRequest;
import io.grpc.stub.StreamObserver;

/** an implementation of WorkerProfileService that delegates to one of the Workers. */
public class WorkerProfileProxyService extends WorkerProfileGrpc.WorkerProfileImplBase {
  //    private final Backplane backplane = null;
  private final Instance instance;

  public WorkerProfileProxyService(Instance instance) {
    this.instance = instance;
  }

  @Override
  public void getWorkerProfile(
      WorkerProfileRequest request, StreamObserver<WorkerProfileMessage> responseObserver) {
    // get usage of CASFileCache

    responseObserver.onNext(instance.getWorkerProfile(request.getWorker()));
    responseObserver.onCompleted();
  }

  @Override
  public void getWorkerList(
      WorkerListRequest request, StreamObserver<WorkerListMessage> responseObserver) {
    WorkerListMessage m = instance.getWorkerList();
    responseObserver.onNext(m);
    responseObserver.onCompleted();
  }
}
