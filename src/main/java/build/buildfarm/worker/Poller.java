// Copyright 2017 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

import com.google.common.base.Preconditions;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import io.grpc.Deadline;
import java.util.function.BooleanSupplier;

public class Poller implements Runnable {
  private final Duration period;
  private final BooleanSupplier poll;
  private final Runnable onExpiry;
  private final Deadline deadline;
  private boolean running;

  public Poller(Duration period, BooleanSupplier poll, Runnable onExpiry, Deadline deadline) {
    this.period = period;
    this.poll = poll;
    this.onExpiry = onExpiry;
    this.deadline = deadline;
    Preconditions.checkState(period.getSeconds() > 0 || period.getNanos() >= 1000);
    running = true;
  }

  private Deadline getPeriodDeadline() {
    long periodMicros = period.getSeconds() * 1000000 + period.getNanos() / 1000;
    return Deadline.after(periodMicros, MICROSECONDS);
  }

  private Duration getWaitTime() {
    Deadline waitDeadline = deadline.minimum(getPeriodDeadline());
    return Durations.fromMicros(waitDeadline.timeRemaining(MICROSECONDS));
  }

  @Override
  public synchronized void run() {
    while (running) {
      try {
        Duration waitTime = getWaitTime();
        this.wait(
            waitTime.getSeconds() * 1000 + waitTime.getNanos() / 1000000,
            waitTime.getNanos() % 1000000);
        if (deadline.isExpired()) {
          onExpiry.run();
          stop();
        }
        if (running) {
          // FP interface with distinct returns, do not memoize!
          running = poll.getAsBoolean();
        }
      } catch (InterruptedException e) {
        running = false;
      }
    }
  }

  public synchronized void stop() {
    running = false;
    this.notify();
  }
}
