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

package build.buildfarm.instance.memory;

import build.buildfarm.instance.Instance;
import com.google.longrunning.Operation;
import com.google.protobuf.Duration;

public class Requeuer implements Runnable {
  private final Operation operation;
  private final Instance instance;

  private long timeoutNanos;
  private boolean stopped;

  public Requeuer(Operation operation, Duration timeout, Instance instance) {
    this.operation = operation;
    this.instance = instance;
    reset(timeout);
  }

  public void run() {
    try {
      long start = System.nanoTime();
      synchronized(this) {
        while (!stopped && timeoutNanos > 0) {
          this.wait(timeoutNanos / 1000000L, (int) (timeoutNanos % 1000000L));
          long now = System.nanoTime();
          timeoutNanos -= now - start;
          start = now;
        }
        if (!stopped) {
          instance.putOperation(operation);
        }
        stopped = true;
      }
    } catch (InterruptedException ex) {
    }
  }

  public synchronized void reset( Duration timeout ) {
    timeoutNanos = timeout.getSeconds() * 1000000000L + timeout.getNanos();
  }

  public synchronized void stop() {
    stopped = true;
    this.notify();
  }
}

