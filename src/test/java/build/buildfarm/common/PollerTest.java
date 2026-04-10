// Copyright 2026 The Buildfarm Authors. All rights reserved.
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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.protobuf.util.Durations;
import io.grpc.Deadline;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PollerTest {
  @Test
  public void constructor_throwsIfPeriodTooShort() {
    assertThrows(IllegalArgumentException.class, () -> new Poller(Durations.fromNanos(500)));
  }

  @Test
  public void resumeAndPause() throws InterruptedException {
    Executor executor = Executors.newSingleThreadExecutor();
    Poller poller = new Poller(Durations.fromMillis(10));
    AtomicBoolean polled = new AtomicBoolean(false);
    Deadline deadline = Deadline.after(1, TimeUnit.SECONDS);

    poller.resume(
        () -> {
          polled.set(true);
          return false;
        },
        () -> {},
        deadline,
        executor);

    // Wait for the poller to run at least once
    long timeout = System.currentTimeMillis() + 500;
    while (!polled.get() && System.currentTimeMillis() < timeout) {
      Thread.sleep(10);
    }

    assertThat(polled.get()).isTrue();
    poller.pause();
  }

  @Test
  public void pollUntilFalse() throws InterruptedException {
    Executor executor = Executors.newSingleThreadExecutor();
    Poller poller = new Poller(Durations.fromMillis(10));
    AtomicBoolean polledCount = new AtomicBoolean(false);
    int[] count = {0};
    Deadline deadline = Deadline.after(5, TimeUnit.SECONDS);

    poller.resume(
        () -> {
          count[0]++;
          if (count[0] >= 3) {
            return false;
          }
          return true;
        },
        () -> {},
        deadline,
        executor);

    // Wait for the poller to complete its task
    long timeout = System.currentTimeMillis() + 1000;
    while (count[0] < 3 && System.currentTimeMillis() < timeout) {
      Thread.sleep(10);
    }

    assertThat(count[0]).isAtLeast(3);
    poller.pause();
  }

  @Test
  public void pollUntilExpiration() throws InterruptedException {
    Executor executor = Executors.newSingleThreadExecutor();
    Poller poller = new Poller(Durations.fromMillis(10));
    AtomicBoolean expired = new AtomicBoolean(false);
    Deadline deadline = Deadline.after(200, TimeUnit.MILLISECONDS);

    poller.resume(() -> true, () -> expired.set(true), deadline, executor);

    // Wait for the expiration to occur
    long timeout = System.currentTimeMillis() + 1000;
    while (!expired.get() && System.currentTimeMillis() < timeout) {
      Thread.sleep(10);
    }

    assertThat(expired.get()).isTrue();
    poller.pause();
  }
}
