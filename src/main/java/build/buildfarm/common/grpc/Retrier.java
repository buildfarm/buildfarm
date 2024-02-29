// Copyright 2016 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.common.grpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AsyncCallable;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Supports execution with retries on particular gRPC Statuses. The retrier is ThreadSafe.
 *
 * <p>Example usage: The simple use-case is to call retrier.execute, e.g:
 *
 * <pre>
 * foo = retrier.execute(
 *     new Callable<Foo>() {
 *       @Override
 *       public Foo call() {
 *         return grpcStub.getFoo(fooRequest);
 *       }
 *     });
 * </pre>
 */
public class Retrier {
  /** Wraps around a StatusRuntimeException to make it pass through a single layer of retries. */
  public static class PassThroughException extends Exception {
    private static final long serialVersionUID = 1;

    public PassThroughException(StatusRuntimeException e) {
      super(e);
    }
  }

  /**
   * Backoff is a stateful object providing a sequence of durations that are used to time delays
   * between retries. It is not ThreadSafe. The reason that Backoff needs to be stateful, rather
   * than a static map of attempt number to delay, is to enable using the retrier via the manual
   * calling isRetriable and nextDelayMillis manually (see ByteStreamUploader example).
   */
  public interface Backoff {
    /** Indicates that no more retries should be made for use in {@link #nextDelayMillis()}. */
    long STOP = -1L;

    /** Returns the next delay in milliseconds, or < 0 if we should not continue retrying. */
    long nextDelayMillis();

    /**
     * Returns the number of calls to {@link #nextDelayMillis()} thus far, not counting any calls
     * that returned STOP.
     */
    int getRetryAttempts();

    /**
     * Creates a Backoff supplier for a Backoff which does not support any retries. Both the
     * Supplier and the Backoff are stateless and thread-safe.
     */
    @SuppressWarnings("Guava")
    Supplier<Backoff> NO_RETRIES =
        () ->
            new Backoff() {
              @Override
              public long nextDelayMillis() {
                return STOP;
              }

              @Override
              public int getRetryAttempts() {
                return 0;
              }
            };

    static Supplier<Backoff> sequential(int maxAttempts) {
      return exponential(
          /* initial= */ Duration.ZERO,
          /* max= */ Duration.ZERO,
          /* multiplier= */ 1.1,
          /* jitter= */ 0.0,
          maxAttempts);
    }

    /**
     * Creates a Backoff supplier for an optionally jittered exponential backoff. The supplier is
     * ThreadSafe (non-synchronized calls to get() are fine), but the returned Backoff is not.
     *
     * @param initial The initial backoff duration.
     * @param max The maximum backoff duration.
     * @param multiplier The amount the backoff should increase in each iteration. Must be >1.
     * @param jitter The amount the backoff should be randomly varied (0-1), with 0 providing no
     *     jitter, and 1 providing a duration that is 0-200% of the non-jittered duration.
     * @param maxAttempts Maximal times to attempt a retry 0 means no retries.
     */
    @SuppressWarnings("Guava")
    static Supplier<Backoff> exponential(
        Duration initial, Duration max, double multiplier, double jitter, int maxAttempts) {
      Preconditions.checkArgument(multiplier > 1, "multipler must be > 1");
      Preconditions.checkArgument(jitter >= 0 && jitter <= 1, "jitter must be in the range (0, 1)");
      Preconditions.checkArgument(maxAttempts >= 0, "maxAttempts must be >= 0");
      return () ->
          new Backoff() {
            private final long maxMillis = max.toMillis();
            private long nextDelayMillis = initial.toMillis();
            private int attempts = 0;

            @Override
            public long nextDelayMillis() {
              if (attempts == maxAttempts) {
                return STOP;
              }
              attempts++;
              double jitterRatio = jitter * (ThreadLocalRandom.current().nextDouble(2.0) - 1);
              long result = (long) (nextDelayMillis * (1 + jitterRatio));
              // Advance current by the non-jittered result.
              nextDelayMillis = (long) (nextDelayMillis * multiplier);
              if (nextDelayMillis > maxMillis) {
                nextDelayMillis = maxMillis;
              }
              return result;
            }

            @Override
            public int getRetryAttempts() {
              return attempts;
            }
          };
    }
  }

  @SuppressWarnings("Guava")
  public static final Predicate<Status> DEFAULT_IS_RETRIABLE =
      st -> {
        switch (st.getCode()) {
          case CANCELLED:
            return !Thread.currentThread().isInterrupted();
          case UNKNOWN:
          case DEADLINE_EXCEEDED:
          case ABORTED:
          case INTERNAL:
          case UNAVAILABLE:
          case UNAUTHENTICATED:
            return true;
          default:
            return false;
        }
      };

  @SuppressWarnings("Guava")
  public static final Predicate<Status> REDIS_IS_RETRIABLE =
      st -> {
        switch (st.getCode()) {
          case CANCELLED:
            return !Thread.currentThread().isInterrupted();
          case DEADLINE_EXCEEDED:
            return true;
          default:
            return false;
        }
      };

  @SuppressWarnings("Guava")
  public static final Predicate<Status> RETRY_ALL = Predicates.alwaysTrue();

  @SuppressWarnings("Guava")
  public static final Predicate<Status> RETRY_NONE = Predicates.alwaysFalse();

  public static final Retrier NO_RETRIES = new Retrier(Backoff.NO_RETRIES, RETRY_NONE);

  @SuppressWarnings("Guava")
  private final Supplier<Backoff> backoffSupplier;

  @SuppressWarnings("Guava")
  private final Predicate<Status> isRetriable;

  private final ListeningScheduledExecutorService retryScheduler;

  @SuppressWarnings("Guava")
  public Retrier(Supplier<Backoff> backoffSupplier, Predicate<Status> isRetriable) {
    this(backoffSupplier, isRetriable, /* retryScheduler= */ null);
  }

  @SuppressWarnings("Guava")
  public Retrier(
      Supplier<Backoff> backoffSupplier,
      Predicate<Status> isRetriable,
      ListeningScheduledExecutorService retryScheduler) {
    this.backoffSupplier = backoffSupplier;
    this.isRetriable = isRetriable;
    this.retryScheduler = retryScheduler;
  }

  /** Returns {@code true} if the {@link Status} is retriable. */
  public boolean isRetriable(Status s) {
    return isRetriable.apply(s);
  }

  /**
   * Executes the given callable in a loop, retrying on retryable errors, as defined by the current
   * backoff/retry policy. Will raise the last encountered retriable error, or the first
   * non-retriable error.
   *
   * <p>This method never throws {@link StatusRuntimeException} even if the passed-in Callable does.
   *
   * @param c The callable to execute.
   */
  public <T> T execute(Callable<T> c) throws IOException, InterruptedException {
    Backoff backoff = backoffSupplier.get();
    while (true) {
      try {
        return c.call();
      } catch (PassThroughException e) {
        throw (StatusRuntimeException) e.getCause();
      } catch (RetryException e) {
        throw e; // Nested retries are always pass-through.
      } catch (StatusException | StatusRuntimeException e) {
        Status st = Status.fromThrowable(e);
        long delay = backoff.nextDelayMillis();
        if (st.getCode() == Status.Code.CANCELLED && Thread.currentThread().isInterrupted()) {
          Thread.currentThread().interrupt();
          throw new InterruptedException();
        }
        if (delay < 0 || !isRetriable.apply(st)) {
          throw new RetryException(st.asRuntimeException(), backoff.getRetryAttempts());
        }
        sleep(delay);
      } catch (Exception e) {
        // Generic catch because Callable is declared to throw Exception, we rethrow any unchecked
        // exception as well as any exception we declared above.
        Throwables.throwIfUnchecked(e);
        Throwables.throwIfInstanceOf(e, IOException.class);
        Throwables.throwIfInstanceOf(e, InterruptedException.class);
        throw new RetryException(e, backoff.getRetryAttempts());
      }
    }
  }

  @VisibleForTesting
  void sleep(long timeMillis) throws InterruptedException {
    Preconditions.checkArgument(
        timeMillis >= 0L, "timeMillis must not be negative: %s", timeMillis);
    TimeUnit.MILLISECONDS.sleep(timeMillis);
  }

  /** Executes an {@link AsyncCallable}, retrying execution in case of failure. */
  public <T> ListenableFuture<T> executeAsync(AsyncCallable<T> call) {
    return executeAsync(call, newBackoff());
  }

  /**
   * Executes an {@link AsyncCallable}, retrying execution in case of failure with the given
   * backoff.
   */
  public <T> ListenableFuture<T> executeAsync(AsyncCallable<T> call, Backoff backoff) {
    try {
      return Futures.catchingAsync(
          call.call(),
          Exception.class,
          t -> onExecuteAsyncFailure(t, call, backoff),
          MoreExecutors.directExecutor());
    } catch (Exception e) {
      return onExecuteAsyncFailure(e, call, backoff);
    }
  }

  private <T> ListenableFuture<T> onExecuteAsyncFailure(
      Exception t, AsyncCallable<T> call, Backoff backoff) {
    long waitMillis = backoff.nextDelayMillis();
    if (waitMillis >= 0 && isRetriable.apply(Status.fromThrowable(t))) {
      try {
        return Futures.scheduleAsync(
            () -> executeAsync(call, backoff), waitMillis, TimeUnit.MILLISECONDS, retryScheduler);
      } catch (RejectedExecutionException e) {
        // May be thrown by .scheduleAsync(...) if i.e. the executor is shutdown.
        return Futures.immediateFailedFuture(new IOException(e));
      }
    } else {
      return Futures.immediateFailedFuture(t);
    }
  }

  public Backoff newBackoff() {
    return backoffSupplier.get();
  }

  public static class ProgressiveBackoff implements Backoff {
    @SuppressWarnings("Guava")
    private final Supplier<Backoff> backoffSupplier;

    private Backoff currentBackoff;
    private int retries = 0;

    /**
     * Creates a resettable Backoff for progressive reads. After a reset, the nextDelay returned
     * indicates an immediate retry. Initially and after indicating an immediate retry, a delegate
     * is generated to provide nextDelay until reset.
     *
     * @param backoffSupplier Delegate Backoff generator
     */
    @SuppressWarnings("Guava")
    public ProgressiveBackoff(Supplier<Backoff> backoffSupplier) {
      this.backoffSupplier = backoffSupplier;
      currentBackoff = backoffSupplier.get();
    }

    public void reset() {
      if (currentBackoff != null) {
        retries += currentBackoff.getRetryAttempts();
      }
      currentBackoff = null;
    }

    @Override
    public long nextDelayMillis() {
      if (currentBackoff == null) {
        currentBackoff = backoffSupplier.get();
        retries++;
        return 0;
      }
      return currentBackoff.nextDelayMillis();
    }

    @Override
    public int getRetryAttempts() {
      int retryAttempts = retries;
      if (currentBackoff != null) {
        retryAttempts += currentBackoff.getRetryAttempts();
      }
      return retryAttempts;
    }
  }
}
