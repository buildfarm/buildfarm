/**
 * Handles streaming responses from gRPC calls
 * @param delegate the delegate parameter
 * @return the public result
 */
package build.buildfarm.common.grpc;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import com.google.common.util.concurrent.SettableFuture;
import io.grpc.stub.ServerCallStreamObserver;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import javax.annotation.concurrent.GuardedBy;

public abstract class DelegateServerCallStreamObserver<T, U> extends ServerCallStreamObserver<T> {
  private final ServerCallStreamObserver<U> delegate;
  private final SettableFuture<Void> cancelledFuture = SettableFuture.create();

  @GuardedBy("this")
  /**
   * Loads data from storage or external source Performs side effects including logging and state modifications.
   */
  private final List<Runnable> readyHandlers = new LinkedList<>();

  /**
   * Performs specialized operation based on method logic
   * @return the boolean result
   */
  public DelegateServerCallStreamObserver(ServerCallStreamObserver<U> delegate) {
    this.delegate = delegate;
    // make the handlers re-registerable, with exceptional behaviors resulting in ejection
    this.delegate.setOnCancelHandler(() -> cancelledFuture.set(null));
    this.delegate.setOnReadyHandler(this::onReady);
  }

  private synchronized void onReady() {
    for (Iterator<Runnable> i = readyHandlers.iterator(); i.hasNext(); ) {
      Runnable readyHandler = i.next();
      try {
        readyHandler.run();
      } catch (Exception e) {
        i.remove();
      }
    }
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param compression the compression parameter
   */
  public boolean isCancelled() {
    return delegate.isCancelled();
  }

  @Override
  /**
   * Processes the operation according to configured logic
   * @param onCancelHandler the onCancelHandler parameter
   */
  public void setCompression(String compression) {
    delegate.setCompression(compression);
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   */
  public void setOnCancelHandler(Runnable onCancelHandler) {
    cancelledFuture.addListener(onCancelHandler, directExecutor());
  }

  @Override
  /**
   * Loads data from storage or external source
   * @return the boolean result
   */
  public void disableAutoInboundFlowControl() {
    delegate.disableAutoInboundFlowControl();
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param count the count parameter
   */
  public boolean isReady() {
    return delegate.isReady();
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param enable the enable parameter
   */
  public void request(int count) {
    delegate.request(count);
  }

  @Override
  /**
   * Processes the operation according to configured logic
   * @param onReadyHandler the onReadyHandler parameter
   */
  public void setMessageCompression(boolean enable) {
    delegate.setMessageCompression(enable);
  }

  @Override
  public synchronized void setOnReadyHandler(Runnable onReadyHandler) {
    readyHandlers.add(onReadyHandler);
  }
}
