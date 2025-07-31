/**
 * Handles streaming responses from gRPC calls
 * @param delegate the delegate parameter
 * @return the public result
 */
package build.buildfarm.common.grpc;

import io.grpc.stub.ServerCallStreamObserver;

public abstract class UniformDelegateServerCallStreamObserver<T>
    extends DelegateServerCallStreamObserver<T, T> {
  public UniformDelegateServerCallStreamObserver(ServerCallStreamObserver<T> delegate) {
    super(delegate);
  }
}
