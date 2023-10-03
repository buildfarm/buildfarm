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

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

import com.google.common.base.Stopwatch;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

public abstract class PipelineStage implements Runnable {
  protected final String name;
  protected final WorkerContext workerContext;
  protected final PipelineStage output;
  protected final PipelineStage error;

  protected boolean claimed = false;
  private volatile boolean closed = false;
  private Thread tickThread = null;
  private boolean tickCancelledFlag = false;
  private String operationName = null;

  PipelineStage(
      String name, WorkerContext workerContext, PipelineStage output, PipelineStage error) {
    this.name = name;
    this.workerContext = workerContext;
    this.output = output;
    this.error = error;
  }

  public String getName() {
    return name;
  }

  private void runInterruptible() throws InterruptedException {
    while (!output.isClosed() || isClaimed()) {
      iterate();
    }
  }

  public @Nullable String getOperationName() {
    return operationName;
  }

  @Override
  public void run() {
    try {
      runInterruptible();
    } catch (InterruptedException e) {
      // ignore
    } catch (Exception e) {
      getLogger()
          .log(Level.SEVERE, format("%s::run(): stage terminated due to exception", name), e);
    } finally {
      boolean wasInterrupted = Thread.interrupted();
      try {
        close();
      } finally {
        if (wasInterrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  public String name() {
    return name;
  }

  protected void cancelTick() {
    // if we are not in a tick, this has no effect
    // if we are in a tick, set the cancel flag, interrupt the tick thread
    Thread cancelTickThread = tickThread;
    if (cancelTickThread != null) {
      tickCancelledFlag = true;
      cancelTickThread.interrupt();
    }
  }

  private boolean tickCancelled() {
    boolean isTickCancelled = tickCancelledFlag;
    tickCancelledFlag = false;
    return isTickCancelled;
  }

  protected void iterate() throws InterruptedException {
    OperationContext operationContext;
    OperationContext nextOperationContext = null;
    long stallUSecs = 0;
    Stopwatch stopwatch = Stopwatch.createUnstarted();
    try {
      operationContext = take();
      start(operationContext.operation.getName());
      stopwatch.start();
      boolean valid = false;
      tickThread = Thread.currentThread();
      try {
        nextOperationContext = tick(operationContext);
        long tickUSecs = stopwatch.elapsed(MICROSECONDS);
        valid = nextOperationContext != null && output.claim(nextOperationContext);
        stallUSecs = stopwatch.elapsed(MICROSECONDS) - tickUSecs;
        // ensure that we clear interrupted if we were supposed to cancel tick
        if (Thread.interrupted() && !tickCancelled()) {
          throw new InterruptedException();
        }
        tickThread = null;
      } catch (InterruptedException e) {
        boolean isTickCancelled = tickCancelled();
        tickThread = null;
        if (valid) {
          output.release();
        }
        if (!isTickCancelled) {
          throw e;
        }
      }
      if (valid) {
        output.put(nextOperationContext);
      } else {
        error.put(operationContext);
      }
    } finally {
      release();
    }
    after(operationContext);
    long usecs = stopwatch.elapsed(MICROSECONDS);
    complete(operationName, usecs, stallUSecs, nextOperationContext != null);
    operationName = null;
  }

  private String logIterateId(String operationName) {
    return format("%s::iterate(%s)", name, operationName);
  }

  protected void start() {
    start("");
  }

  protected void start(String operationName) {
    start(operationName, "Starting");
  }

  protected void start(String operationName, String message) {
    // TODO to unary stage
    this.operationName = operationName;
    getLogger().log(Level.FINER, format("%s: %s", logIterateId(operationName), message));
  }

  protected void complete(String operationName, long usecs, long stallUSecs, boolean success) {
    complete(operationName, usecs, stallUSecs, success ? "Success" : "Failed");
  }

  protected void complete(String operationName, long usecs, long stallUSecs, String status) {
    this.operationName = operationName;
    getLogger()
        .log(
            Level.FINER,
            format(
                "%s: %g ms (%g ms stalled) %s",
                logIterateId(operationName), usecs / 1000.0f, stallUSecs / 1000.0f, status));
  }

  protected OperationContext tick(OperationContext operationContext) throws InterruptedException {
    return operationContext;
  }

  protected void after(OperationContext operationContext) {}

  public synchronized boolean claim(OperationContext operationContext) throws InterruptedException {
    while (!closed && claimed) {
      wait();
    }
    if (closed) {
      notify(); // no further notify would be called, we must signal
      return false;
    }
    claimed = true;
    return true;
  }

  public synchronized void release() {
    claimed = false;
    notify();
  }

  public void close() {
    closed = true;
  }

  public boolean isClosed() {
    return closed;
  }

  protected boolean isClaimed() {
    return claimed;
  }

  public PipelineStage output() {
    return this.output;
  }

  public PipelineStage error() {
    return this.error;
  }

  abstract Logger getLogger();

  abstract OperationContext take() throws InterruptedException;

  abstract void put(OperationContext operationContext) throws InterruptedException;

  public static class NullStage extends PipelineStage {
    public NullStage() {
      this(/* workerContext=*/ null, /* output=*/ null);
    }

    public NullStage(WorkerContext workerContext, PipelineStage output) {
      super("NullStage", workerContext, output, null);
    }

    @Override
    public Logger getLogger() {
      return null;
    }

    @Override
    public boolean claim(OperationContext operationContext) {
      return true;
    }

    @Override
    public void release() {}

    @Override
    public OperationContext take() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void put(OperationContext operationContext) throws InterruptedException {}

    @Override
    public void run() {}

    @Override
    public void close() {}

    @Override
    public boolean isClosed() {
      return false;
    }
  }
}
