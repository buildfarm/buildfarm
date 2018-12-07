package build.buildfarm.proxy.http;

import static build.buildfarm.common.UrlPath.detectResourceOperation;
import static com.google.common.base.Preconditions.checkState;

import build.buildfarm.common.UrlPath.InvalidResourceNameException;
import build.buildfarm.common.UrlPath.ResourceOperation;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import io.grpc.stub.StreamObserver;

class WriteStreamObserver implements StreamObserver<WriteRequest> {
  private final WriteObserverSource writeObserverSource;
  private final StreamObserver<WriteResponse> responseObserver;
  private Throwable error = null;
  private WriteObserver write = null;

  WriteStreamObserver(
      StreamObserver<WriteResponse> responseObserver,
      WriteObserverSource writeObserverSource) {
    this.writeObserverSource = writeObserverSource;
    this.responseObserver = responseObserver;
  }

  private void writeOnNext(WriteRequest request) throws InvalidResourceNameException {
    if (write == null) {
      write = writeObserverSource.get(request.getResourceName());
    }
    write.onNext(request);
    if (request.getFinishWrite()) {
      responseObserver.onNext(WriteResponse.newBuilder()
          .setCommittedSize(write.getCommittedSize())
          .build());
    }
  }

  @Override
  public void onNext(WriteRequest request) {
    checkState(
        request.getFinishWrite() || request.getData().size() != 0,
        String.format(
            "write onNext supplied with empty WriteRequest for %s at %d",
            request.getResourceName(),
            request.getWriteOffset()));
    if (request.getData().size() != 0) {
      try {
        writeOnNext(request);
        if (request.getFinishWrite()) {
          writeObserverSource.remove(request.getResourceName());
        }
      } catch (Throwable t) {
        onError(t);
      }
    }
  }

  @Override
  public void onError(Throwable t) {
    error = t;
    responseObserver.onError(t);
  }

  @Override
  public void onCompleted() {
    if (write != null) {
      write.onCompleted();
    }
    responseObserver.onCompleted();
  }
}
