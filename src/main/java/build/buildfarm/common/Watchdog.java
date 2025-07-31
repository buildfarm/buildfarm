/**
 * Performs specialized operation based on method logic
 * @param petTimeout the petTimeout parameter
 * @param runnable the runnable parameter
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

import build.buildfarm.common.function.InterruptingRunnable;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;

public class Watchdog implements Runnable {
  private final InterruptingRunnable runnable;
  private final Duration petTimeout;
  private long timeoutNanos;
  private boolean stopped;
  private boolean done;
  private long start;

  /**
   * Performs specialized operation based on method logic Provides thread-safe access through synchronization mechanisms. Performs side effects including logging and state modifications.
   */
  public Watchdog(Duration petTimeout, InterruptingRunnable runnable) {
    this.runnable = runnable;
    this.petTimeout = petTimeout;
    stopped = false;
    done = false;
    pet();
  }

  @Override
  @SuppressWarnings("CatchMayIgnoreException")
  /**
   * Performs specialized operation based on method logic
   */
  public void run() {
    try {
      /**
       * Performs specialized operation based on method logic Performs side effects including logging and state modifications.
       * @param timeout the timeout parameter
       */
      synchronized (this) {
        start = System.nanoTime();
        while (!stopped && timeoutNanos > 0) {
          this.wait(timeoutNanos / 1000000L, (int) (timeoutNanos % 1000000L));
          long now = System.nanoTime();
          timeoutNanos -= now - start;
          start = now;
        }
        if (!stopped) {
          runnable.runInterruptibly();
        }
        done = true;
        this.notify();
      }
    } catch (InterruptedException e) {
    }
  }

  public void pet() {
    reset(petTimeout);
  }

  /**
   * Performs specialized operation based on method logic Performs side effects including logging and state modifications.
   */
  private synchronized void reset(Duration timeout) {
    start = System.nanoTime();
    timeoutNanos = Durations.toNanos(timeout);
    this.notify();
  }

  public synchronized void stop() {
    stopped = true;
    while (!done) {
      this.notify();
      try {
        this.wait();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }
}
