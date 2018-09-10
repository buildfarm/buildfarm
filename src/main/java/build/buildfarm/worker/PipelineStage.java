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

public abstract class PipelineStage implements Runnable {
  private final String name;
  protected final WorkerContext workerContext;
  protected final PipelineStage output;
  private final PipelineStage error;

  private PipelineStage input;
  protected boolean claimed;
  private boolean closed;
  private Thread tickThread = null;
  private boolean tickCancelled = false;

  PipelineStage(String name, WorkerContext workerContext, PipelineStage output, PipelineStage error) {
    this.name = name;
    this.workerContext = workerContext;
    this.output = output;
    this.error = error;

    input = null;
    claimed = false;
    closed = false;
  }

  public void setInput(PipelineStage input) {
    this.input = input;
  }

  private void runInterruptible() throws InterruptedException {
    while (!output.isClosed() || isClaimed()) {
      iterate();
    }
  }

  @Override
  public void run() {
    try {
      runInterruptible();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      close();
    }
  }

  protected void cancelTick() {
    // if we are not in a tick, this has no effect
    // if we are in a tick, set the cancel flag, interrupt the tick thread
    Thread cancelTickThread = tickThread;
    if (cancelTickThread != null) {
      tickCancelled = true;
      cancelTickThread.interrupt();
    }
  }

  private boolean tickCancelled() {
    boolean isTickCancelled = tickCancelled;
    tickCancelled = false;
    return isTickCancelled;
  }

  protected void iterate() throws InterruptedException {
    long startTime, waitTime = 0;
    OperationContext operationContext, nextOperationContext = null;
    try {
      operationContext = take();
      startTime = System.nanoTime();
      boolean valid = false;
      try {
        tickThread = Thread.currentThread();
        nextOperationContext = tick(operationContext);
        long waitStartTime = System.nanoTime();
        valid = nextOperationContext != null && output.claim();
        waitTime = System.nanoTime() - waitStartTime;
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

    long endTime = System.nanoTime();
    workerContext.logInfo(String.format(
        "%s::iterate(%s): %gms (%gms wait) %s",
        name,
        operationContext.operation.getName(),
        (endTime - startTime) / 1000000.0f,
        waitTime / 1000000.0f,
        nextOperationContext == null ? "Failed" : "Success"));
  }

  protected OperationContext tick(OperationContext operationContext) throws InterruptedException {
    return operationContext;
  }

  protected void after(OperationContext operationContext) { }

  public synchronized boolean claim() throws InterruptedException {
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

  abstract OperationContext take() throws InterruptedException;
  abstract void put(OperationContext operationContext) throws InterruptedException;

  public static class NullStage extends PipelineStage {
    public NullStage() {
      super(null, null, null, null);
    }

    @Override
    public boolean claim() { return true; }
    @Override
    public void release() { }
    @Override
    public OperationContext take() { throw new UnsupportedOperationException(); }
    @Override
    public void put(OperationContext operation) throws InterruptedException { }
    @Override
    public void setInput(PipelineStage input) { }
    @Override
    public void run() { }
    @Override
    public void close() { }
    @Override
    public boolean isClosed() { return false; }
  }
}
