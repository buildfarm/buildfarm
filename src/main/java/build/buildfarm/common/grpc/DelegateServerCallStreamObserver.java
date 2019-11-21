package build.buildfarm.common.grpc;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import io.grpc.stub.ServerCallStreamObserver;
import com.google.common.util.concurrent.SettableFuture;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import javax.annotation.concurrent.GuardedBy;

public abstract class DelegateServerCallStreamObserver<T, U> extends ServerCallStreamObserver<T> {
  private final ServerCallStreamObserver<U> delegate;
  private final SettableFuture<Void> cancelledFuture = SettableFuture.create();

  @GuardedBy("this")
  private final List<Runnable> readyHandlers = new LinkedList<>();

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
  public boolean isCancelled() {
    return delegate.isCancelled();
  }

  @Override
  public void setCompression(String compression) {
    delegate.setCompression(compression);
  }

  @Override
  public void setOnCancelHandler(Runnable onCancelHandler) {
    cancelledFuture.addListener(onCancelHandler, directExecutor());
  }

  @Override
  public void disableAutoInboundFlowControl() {
    delegate.disableAutoInboundFlowControl();
  }

  @Override
  public boolean isReady() {
    return delegate.isReady();
  }

  @Override
  public void request(int count) {
    delegate.request(count);
  }

  @Override
  public void setMessageCompression(boolean enable) {
    delegate.setMessageCompression(enable);
  }

  @Override
  public synchronized void setOnReadyHandler(Runnable onReadyHandler) {
    readyHandlers.add(onReadyHandler);
  }
}
