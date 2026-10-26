// Copyright 2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.worker.shard;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import build.buildfarm.backplane.Backplane;
import build.buildfarm.v1test.ShardWorker;
import io.grpc.Status;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WorkerRegistrationRetryTest {
  @Test
  public void registrationRetryRecognizesTransientFailures() {
    assertThat(
            Worker.isRetriableRegistrationException(
                Status.UNAVAILABLE.withDescription("redis unavailable").asRuntimeException()))
        .isTrue();
    assertThat(
            Worker.isRetriableRegistrationException(
                Status.DEADLINE_EXCEEDED
                    .withDescription("redis request timed out")
                    .asRuntimeException()))
        .isTrue();
  }

  @Test
  public void registrationRetryRejectsNonTransientFailures() {
    assertThat(
            Worker.isRetriableRegistrationException(
                Status.INTERNAL.withDescription("bad worker registration").asRuntimeException()))
        .isFalse();
    assertThat(Worker.isRetriableRegistrationException(new IllegalStateException("bad state")))
        .isFalse();
  }

  @Test
  public void registrationRetryBackoffAddsTwentyPercentJitter() {
    assertThat(Worker.registrationRetryBackoffWithJitterMillis(1000, -1.0)).isEqualTo(800);
    assertThat(Worker.registrationRetryBackoffWithJitterMillis(1000, 0.0)).isEqualTo(1000);
    assertThat(Worker.registrationRetryBackoffWithJitterMillis(1000, 1.0)).isEqualTo(1200);
    assertThat(Worker.registrationRetryBackoffWithJitterMillis(30000, 1.0)).isEqualTo(30000);
  }

  @Test
  public void addWorkerRetriesWithFreshRegistrationUntilBackplaneAcceptsWorker()
      throws IOException, InterruptedException {
    Backplane backplane = mock(Backplane.class);
    List<ShardWorker> registrations = new ArrayList<>();
    List<Long> sleeps = new ArrayList<>();
    AtomicLong expireAt = new AtomicLong();
    doAnswer(
            invocation -> {
              registrations.add(invocation.getArgument(0));
              if (registrations.size() <= 2) {
                throw new IOException(
                    Status.UNAVAILABLE.withDescription("redis unavailable").asException());
              }
              return null;
            })
        .when(backplane)
        .addWorker(any(ShardWorker.class));

    Worker.addWorker(
        () ->
            ShardWorker.newBuilder()
                .setEndpoint("worker")
                .setExpireAt(expireAt.addAndGet(10))
                .build(),
        () -> backplane,
        () -> false,
        sleeps::add);

    assertThat(registrations).hasSize(3);
    assertThat(registrations.get(0).getExpireAt()).isEqualTo(10);
    assertThat(registrations.get(1).getExpireAt()).isEqualTo(20);
    assertThat(registrations.get(2).getExpireAt()).isEqualTo(30);
    assertThat(sleeps).hasSize(2);
    assertThat(sleeps.get(0)).isAtLeast(800L);
    assertThat(sleeps.get(0)).isAtMost(1200L);
    assertThat(sleeps.get(1)).isAtLeast(1600L);
    assertThat(sleeps.get(1)).isAtMost(2400L);
  }

  @Test
  public void addWorkerStopsRetryingWhenShutdownStarts() throws IOException, InterruptedException {
    Backplane backplane = mock(Backplane.class);
    AtomicBoolean shuttingDown = new AtomicBoolean(false);
    List<ShardWorker> registrations = new ArrayList<>();
    List<Long> sleeps = new ArrayList<>();
    doAnswer(
            invocation -> {
              registrations.add(invocation.getArgument(0));
              throw new IOException(
                  Status.UNAVAILABLE.withDescription("redis unavailable").asException());
            })
        .when(backplane)
        .addWorker(any(ShardWorker.class));

    Worker.addWorker(
        () -> ShardWorker.newBuilder().setEndpoint("worker").setExpireAt(10).build(),
        () -> backplane,
        shuttingDown::get,
        sleepMillis -> {
          sleeps.add(sleepMillis);
          shuttingDown.set(true);
        });

    assertThat(registrations).hasSize(1);
    assertThat(sleeps).hasSize(1);
  }

  @Test
  public void addWorkerPropagatesNonTransientRuntimeFailures() throws IOException {
    Backplane backplane = mock(Backplane.class);
    IllegalStateException failure = new IllegalStateException("bad state");
    doAnswer(
            invocation -> {
              throw failure;
            })
        .when(backplane)
        .addWorker(any(ShardWorker.class));

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                Worker.addWorker(
                    () -> ShardWorker.newBuilder().setEndpoint("worker").setExpireAt(10).build(),
                    () -> backplane,
                    () -> false,
                    sleepMillis -> {}));
    assertThat(thrown).isSameInstanceAs(failure);
  }
}
