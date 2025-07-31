package build.buildfarm.instance.shard;

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import build.buildfarm.common.Write;
import build.buildfarm.common.function.IOSupplier;
import build.buildfarm.common.io.FeedbackOutputStream;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Status;
import io.grpc.Status.Code;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * The response to any write may be FAILED_PRECONDITION, which indicates that the backend is in a
 * readonly state (currently only implementation).
 *
 * <p>Here, and in the resulting output stream, we interpret that status from the future and attempt
 * to retry on different workers. The worker should have a backoff as well before being considered
 * for any writes.
 *
 * <p>If any of the underlying stream operations fail on the new worker, that is conveyed to the
 * caller.
 */
class ReadOnlyAwareWrite implements Write {
  private final IOSupplier<Write> delegateSupplier;
  private final SettableFuture<Long> future = SettableFuture.create();
  private Write delegate;

  ReadOnlyAwareWrite(IOSupplier<Write> delegateSupplier) {
    this.delegateSupplier = delegateSupplier;
  }

  // our undelegated future
  @Override
  public ListenableFuture<Long> getFuture() {
    return future;
  }

  private FutureCallback<Long> delegateCallback() {
    return new FutureCallback<>() {
      @Override
      public void onSuccess(Long committedSize) {
        future.set(committedSize);
      }

      @Override
      public void onFailure(Throwable t) {
        // workers return FAILED_PRECONDITION for read only mode
        if (Status.fromThrowable(t).getCode() == Code.FAILED_PRECONDITION) {
          synchronized (ReadOnlyAwareWrite.this) {
            delegate = null;
          }
        } else {
          future.setException(t);
        }
      }
    };
  }

  private synchronized Write getDelegate() throws IOException {
    if (delegate == null) {
      delegate = delegateSupplier.get();
      addCallback(delegate.getFuture(), delegateCallback(), directExecutor());
    }
    return delegate;
  }

  @Override
  public long getCommittedSize() {
    try {
      return getDelegate().getCommittedSize();
    } catch (IOException e) {
      // unlikely
      return 0;
    }
  }

  @Override
  public boolean isComplete() {
    try {
      return getDelegate().isComplete();
    } catch (IOException e) {
      // unlikely
      return false;
    }
  }

  @Override
  public FeedbackOutputStream getOutput(
      long offset, long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler)
      throws IOException {
    return new ReadOnlyAwareOutputStream(
        () -> getDelegate().getOutput(offset, deadlineAfter, deadlineAfterUnits, onReadyHandler));
  }

  @Override
  public ListenableFuture<FeedbackOutputStream> getOutputFuture(
      long offset, long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) {
    // unused interface call on servers
    throw new UnsupportedOperationException();
  }

  @Override
  public void reset() {
    try {
      getDelegate().reset();
    } catch (IOException e) {
      // unlikely, ignore
    }
  }
}
