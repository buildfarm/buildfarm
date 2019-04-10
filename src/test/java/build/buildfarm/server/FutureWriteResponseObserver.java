package build.buildfarm.server;

import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ExecutionException;

class FutureWriteResponseObserver implements StreamObserver<WriteResponse> {
  private final SettableFuture<WriteResponse> future = SettableFuture.create();

  @Override
  public void onNext(WriteResponse response) {
    future.set(response);
  }

  @Override
  public void onCompleted() {
  }

  @Override
  public void onError(Throwable t) {
    future.setException(t);
  }

  public boolean isDone() {
    return future.isDone();
  }

  public WriteResponse get() throws ExecutionException, InterruptedException {
    return future.get();
  }
}
