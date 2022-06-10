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

package build.buildfarm.worker;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

abstract class SuperscalarPipelineStage extends PipelineStage {
  @SuppressWarnings("rawtypes")
  protected final Slots slots;

  private volatile boolean catastrophic = false;

  @SuppressWarnings("rawtypes")
  public SuperscalarPipelineStage(
      String name,
      WorkerContext workerContext,
      PipelineStage output,
      PipelineStage error,
      int width) {
    super(name, workerContext, output, error);

    slots = new Slots(width);
  }

  protected abstract void interruptAll();

  protected abstract int claimsRequired(OperationContext operationContext);

  synchronized void waitForReleaseOrCatastrophe(BlockingQueue<OperationContext> queue) {
    boolean interrupted = false;
    while (!catastrophic && isClaimed()) {
      if (output.isClosed()) {
        // interrupt the currently running threads, because they have nowhere to go
        interruptAll();
      }
      OperationContext operationContext = queue.poll();
      if (operationContext != null) {
        releaseClaim(operationContext.operation.getName(), claimsRequired(operationContext));
      } else {
        try {
          wait(/* timeout=*/ 10);
        } catch (InterruptedException e) {
          interrupted = Thread.interrupted() || interrupted;
          // ignore, we will throw it eventually
        }
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  protected OperationContext takeOrDrain(BlockingQueue<OperationContext> queue)
      throws InterruptedException {
    boolean interrupted = false;
    InterruptedException exception;
    try {
      while (!isClosed() && !output.isClosed()) {
        OperationContext context = queue.poll(10, TimeUnit.MILLISECONDS);
        if (context != null) {
          return context;
        }
      }
      exception = new InterruptedException();
    } catch (InterruptedException e) {
      // only possible way to be terminated
      exception = e;
      // clear interrupted flag
      interrupted = Thread.interrupted();
    }
    waitForReleaseOrCatastrophe(queue);
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    throw exception;
  }

  protected synchronized void releaseClaim(String operationName, int count) {
    slots.claims.release(count);
  }

  protected String getUsage(int size) {
    return String.format("%s/%d", size, slots.width);
  }

  @SuppressWarnings("unchecked")
  public boolean claim(int count) throws InterruptedException {
    // Can't claim if stage is closed
    if (isClosed()) {
      return false;
    }

    // Wait until there is enough room to claim
    slots.claims.acquire(count);
    return true;
  }

  public int getSlotUsage() {
    return slots.width - slots.claims.availablePermits();
  }

  @Override
  public boolean claim(OperationContext operationContext) throws InterruptedException {
    return claim(claimsRequired(operationContext));
  }

  @Override
  public void release() {
    releaseClaim("unidentified operation", 1);
  }

  @Override
  protected boolean isClaimed() {
    return getSlotUsage() > 0;
  }
}
