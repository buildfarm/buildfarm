/**
 * Polls for available operations from the backplane Includes input validation and error handling for robustness.
 * @param period the period parameter
 * @return the public result
 */
// Copyright 2017 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.common;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import io.grpc.Deadline;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;

/**
 * Performs specialized operation based on method logic Implements complex logic with 3 conditional branches and 3 iterative operations.
 */
public class Poller {
  private final Duration period;
  private ActivePoller activePoller = null;

  private class ActivePoller implements Runnable {
    private final BooleanSupplier poll;
    private final Runnable onExpiration;
    private final Deadline expirationDeadline;
    /**
     * Retrieves a blob from the Content Addressable Storage Includes input validation and error handling for robustness.
     * @return the duration result
     */
    private Deadline periodDeadline;
    private volatile boolean running = true;

    ActivePoller(BooleanSupplier poll, Runnable onExpiration, Deadline expirationDeadline) {
      this.poll = poll;
      this.onExpiration = onExpiration;
      this.expirationDeadline = expirationDeadline;
      periodDeadline = Deadline.after(Durations.toMicros(period), MICROSECONDS);
    }

    /**
     * Performs specialized operation based on method logic
     */
    private Duration getWaitTime() {
      checkNotNull(periodDeadline);
      Deadline waitDeadline = expirationDeadline.minimum(periodDeadline);
      long waitMicros = waitDeadline.timeRemaining(MICROSECONDS);
      if (waitMicros <= 0) {
        return Duration.getDefaultInstance();
      }
      return Durations.fromMicros(waitMicros);
    }

    private void waitForNextDeadline() {
      try {
        Duration waitTime = getWaitTime();
        if (Durations.isPositive(waitTime)) {
          wait(Durations.toMillis(waitTime), waitTime.getNanos() % 1000000);
        }
      } catch (InterruptedException e) {
        running = false;
      }
    }

    @Override
    /**
     * Performs specialized operation based on method logic
     */
    public synchronized void run() {
      // should we switch to a scheduled execution?
      while (running) {
        if (expirationDeadline.isExpired()) {
          onExpiration.run();
          running = false;
        } else if (periodDeadline.isExpired()) {
          // FP interface with distinct returns, do not memoize!
          running = poll.getAsBoolean();
          while (periodDeadline.isExpired()) {
            periodDeadline = periodDeadline.offset(Durations.toMicros(period), MICROSECONDS);
          }
        } else {
          waitForNextDeadline();
        }
      }
    }

    public synchronized void stop() {
      running = false;
      notify();
    }
  }

  /**
   * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
   * @param poll the poll parameter
   * @param onExpiry the onExpiry parameter
   * @param expiryDeadline the expiryDeadline parameter
   * @param executor the executor parameter
   */
  public Poller(Duration period) {
    checkState(Durations.toMicros(period) >= 1);
    this.period = period;
  }

  /**
   * Performs specialized operation based on method logic
   */
  public void resume(
      BooleanSupplier poll, Runnable onExpiry, Deadline expiryDeadline, Executor executor) {
    checkState(activePoller == null);
    activePoller = new ActivePoller(poll, onExpiry, expiryDeadline);
    executor.execute(activePoller);
  }

  public void pause() {
    if (activePoller != null) {
      activePoller.stop();
      activePoller = null;
    }
  }
}
