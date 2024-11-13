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

public class Poller {
  private final Duration period;
  private ActivePoller activePoller = null;

  private class ActivePoller implements Runnable {
    private final BooleanSupplier poll;
    private final Runnable onExpiration;
    private final Deadline expirationDeadline;
    private Deadline periodDeadline;
    private volatile boolean running = true;

    ActivePoller(BooleanSupplier poll, Runnable onExpiration, Deadline expirationDeadline) {
      this.poll = poll;
      this.onExpiration = onExpiration;
      this.expirationDeadline = expirationDeadline;
      periodDeadline = Deadline.after(Durations.toMicros(period), MICROSECONDS);
    }

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
        if (waitTime.getSeconds() != 0 || waitTime.getNanos() != 0) {
          wait(
              waitTime.getSeconds() * 1000 + waitTime.getNanos() / 1000000,
              waitTime.getNanos() % 1000000);
        }
      } catch (InterruptedException e) {
        running = false;
      }
    }

    @Override
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

  public Poller(Duration period) {
    checkState(period.getSeconds() > 0 || period.getNanos() >= 1000);
    this.period = period;
  }

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
