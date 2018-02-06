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

package build.buildfarm.common;

import com.google.protobuf.Duration;

public class Watchdog implements Runnable {
  private final Runnable runnable;
  private final Duration petTimeout;
  private long timeoutNanos;
  private boolean stopped;
  private boolean done;
  private long start;

  public Watchdog(Duration petTimeout, Runnable runnable) {
    this.runnable = runnable;
    this.petTimeout = petTimeout;
    stopped = false;
    done = false;
    pet();
  }

  public void run() {
    try {
      synchronized(this) {
        start = System.nanoTime();
        while (!stopped && timeoutNanos > 0) {
          this.wait(timeoutNanos / 1000000L, (int) (timeoutNanos % 1000000L));
          long now = System.nanoTime();
          timeoutNanos -= now - start;
          start = now;
        }
        if (!stopped) {
          runnable.run();
        }
        if (Thread.interrupted()) {
          throw new InterruptedException();
        }
        done = true;
        this.notify();
      }
    } catch (InterruptedException ex) {
    }
  }

  public void pet() {
    reset(petTimeout);
  }

  private synchronized void reset(Duration timeout) {
    start = System.nanoTime();
    timeoutNanos = timeout.getSeconds() * 1000000000L + timeout.getNanos();
    this.notify();
  }

  public synchronized void stop() {
    stopped = true;
    while (!done) {
      this.notify();
      try {
        this.wait();
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }
}
