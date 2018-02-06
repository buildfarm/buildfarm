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

  protected void iterate() throws InterruptedException {
    OperationContext operationContext, nextOperationContext;
    try {
      operationContext = take();
      nextOperationContext = tick(operationContext);
      if (nextOperationContext != null && output.claim()) {
        output.put(nextOperationContext);
      } else {
        error.put(operationContext);
      }
    } finally {
      release();
    }
    after(operationContext);
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
}
