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

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.logging.Level;
import lombok.extern.java.Log;

/**
 * Monitors Redis Stream Pending Entries Lists (PELs) and reclaims stale entries using XAUTOCLAIM.
 *
 * <p>When stream queues are enabled, messages dequeued via XREADGROUP enter the PEL. If a server
 * crashes between XREADGROUP and XACK, the message is orphaned. This monitor periodically scans for
 * such entries and re-enqueues them.
 *
 * <p>This is complementary to {@link DispatchedMonitor} which handles operations that made it into
 * the dispatched hash but whose workers became unresponsive.
 */
@Log
class StreamPelMonitor implements Runnable {

  @FunctionalInterface
  interface PelReclaimer {
    int reclaimAll(long minIdleMillis) throws Exception;
  }

  private final BooleanSupplier shouldStop;
  private final PelReclaimer reclaimer;
  private final int intervalSeconds;
  private final long minIdleMillis;

  StreamPelMonitor(
      BooleanSupplier shouldStop, PelReclaimer reclaimer, int intervalSeconds, long minIdleMillis) {
    this.shouldStop = shouldStop;
    this.reclaimer = reclaimer;
    this.intervalSeconds = intervalSeconds;
    this.minIdleMillis = minIdleMillis;
  }

  void iterate() throws InterruptedException {
    try {
      int count = reclaimer.reclaimAll(minIdleMillis);
      if (count > 0) {
        log.log(Level.INFO, "StreamPelMonitor: reclaimed " + count + " stale PEL entries");
      }
    } catch (InterruptedException e) {
      throw e;
    } catch (Exception e) {
      log.log(Level.SEVERE, "StreamPelMonitor: error during PEL reclaim", e);
    }
  }

  private void runInterruptibly() throws InterruptedException {
    while (!shouldStop.getAsBoolean()) {
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException();
      }
      TimeUnit.SECONDS.sleep(intervalSeconds);
      iterate();
    }
  }

  @Override
  public synchronized void run() {
    log.log(Level.INFO, "StreamPelMonitor: Running");
    try {
      runInterruptibly();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      log.log(Level.INFO, "StreamPelMonitor: Exiting");
    }
  }
}
