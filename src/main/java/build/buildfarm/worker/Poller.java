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

import com.google.protobuf.Duration;
import java.util.function.BooleanSupplier;

public class Poller implements Runnable {
  private final Duration period;
  private final BooleanSupplier poll;
  private boolean running;

  public Poller(Duration period, BooleanSupplier poll) {
    this.period = period;
    this.poll = poll;
    running = true;
  }

  @Override
  public synchronized void run() {
    while (running) {
      try {
        this.wait(
            period.getSeconds() * 1000 + period.getNanos() / 1000000,
            period.getNanos() % 1000000);
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
