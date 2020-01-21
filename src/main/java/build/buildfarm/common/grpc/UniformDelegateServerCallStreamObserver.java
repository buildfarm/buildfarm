package build.buildfarm.common.grpc;

import io.grpc.stub.ServerCallStreamObserver;

public abstract class UniformDelegateServerCallStreamObserver<T> extends DelegateServerCallStreamObserver<T, T> {
  public UniformDelegateServerCallStreamObserver(ServerCallStreamObserver<T> delegate) {
    super(delegate);
  }
}
