// Copyright 2019 The Bazel Authors. All rights reserved.
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

package build.buildfarm.server;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.scheduleAsync;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.common.util.concurrent.ListenableFuture;
import io.prometheus.client.Summary;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

class ActionCacheRequestCounter {
  private static final Summary actionResults =
      Summary.build().name("action_results").help("Action results.").register();

  private final Logger logger;
  private final Duration delay;
  private final AtomicLong counter = new AtomicLong(0l);
  private final ScheduledExecutorService service = newSingleThreadScheduledExecutor();

  ActionCacheRequestCounter(Logger logger, Duration delay) {
    this.logger = logger;
    this.delay = delay;
  }

  public void start() {
    schedule();
  }

  public boolean stop() {
    return shutdownAndAwaitTermination(service, 1, SECONDS);
  }

  public void increment() {
    counter.incrementAndGet();
  }

  private void logRequests() {
    long requestCount = counter.getAndSet(0l);
    if (requestCount > 0) {
      actionResults.observe(requestCount);
      logger.log(Level.FINE, String.format("GetActionResult %d Requests", requestCount));
    }
    schedule();
  }

  private void schedule() {
    ListenableFuture<Void> logFuture =
        scheduleAsync(
            () -> {
              logRequests();
              return immediateFuture(null);
            },
            delay.toMillis(),
            MILLISECONDS,
            service);
  }
}
