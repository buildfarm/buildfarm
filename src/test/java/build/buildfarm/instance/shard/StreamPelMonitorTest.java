// Copyright 2025 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.instance.shard;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StreamPelMonitorTest {

  @Test
  public void shouldStopStopsMonitor() throws Exception {
    StreamPelMonitor.PelReclaimer reclaimer = mock(StreamPelMonitor.PelReclaimer.class);
    StreamPelMonitor monitor =
        new StreamPelMonitor(
            /* shouldStop= */ () -> true,
            reclaimer,
            /* intervalSeconds= */ 0,
            /* minIdleMillis= */ 60000L);

    monitor.run();
    verifyNoInteractions(reclaimer);
  }

  @Test
  public void iterateShouldCallReclaimer() throws Exception {
    StreamPelMonitor.PelReclaimer reclaimer = mock(StreamPelMonitor.PelReclaimer.class);
    when(reclaimer.reclaimAll(60000L)).thenReturn(3);
    StreamPelMonitor monitor =
        new StreamPelMonitor(
            /* shouldStop= */ () -> false,
            reclaimer,
            /* intervalSeconds= */ 0,
            /* minIdleMillis= */ 60000L);

    monitor.iterate();
    verify(reclaimer, times(1)).reclaimAll(60000L);
  }

  @Test
  public void iterateShouldHandleExceptions() throws Exception {
    StreamPelMonitor.PelReclaimer reclaimer = mock(StreamPelMonitor.PelReclaimer.class);
    when(reclaimer.reclaimAll(60000L)).thenThrow(new IOException("transient error"));
    StreamPelMonitor monitor =
        new StreamPelMonitor(
            /* shouldStop= */ () -> false,
            reclaimer,
            /* intervalSeconds= */ 0,
            /* minIdleMillis= */ 60000L);

    // Should not throw
    monitor.iterate();
    verify(reclaimer, times(1)).reclaimAll(60000L);
  }

  @Test
  public void shouldStopOnInterrupt() throws Exception {
    AtomicBoolean readyForInterrupt = new AtomicBoolean(false);
    StreamPelMonitor.PelReclaimer reclaimer = mock(StreamPelMonitor.PelReclaimer.class);
    when(reclaimer.reclaimAll(60000L))
        .thenAnswer(
            invocation -> {
              readyForInterrupt.set(true);
              return 0;
            });

    StreamPelMonitor monitor =
        new StreamPelMonitor(
            /* shouldStop= */ () -> false,
            reclaimer,
            /* intervalSeconds= */ 0,
            /* minIdleMillis= */ 60000L);
    Thread thread = new Thread(monitor);
    thread.start();
    while (!readyForInterrupt.get()) {
      TimeUnit.MICROSECONDS.sleep(1);
    }
    thread.interrupt();
    thread.join();
  }

  @Test
  public void shouldIterateUntilShouldStop() throws Exception {
    StreamPelMonitor.PelReclaimer reclaimer = mock(StreamPelMonitor.PelReclaimer.class);
    when(reclaimer.reclaimAll(60000L)).thenReturn(0);
    BooleanSupplier shouldStop = mock(BooleanSupplier.class);
    when(shouldStop.getAsBoolean()).thenReturn(false).thenReturn(true);
    StreamPelMonitor monitor =
        new StreamPelMonitor(shouldStop, reclaimer, /* intervalSeconds= */ 0, 60000L);
    monitor.run();
    verify(reclaimer, atLeastOnce()).reclaimAll(60000L);
    verify(shouldStop, times(2)).getAsBoolean();
  }
}
