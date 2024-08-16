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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import lombok.Getter;

public abstract class SuperscalarPipelineStage extends PipelineStage {
  @Getter protected final int width;

  protected final BlockingQueue<Object> claims;

  protected Set<String> operationNames = new HashSet<>();

  private volatile boolean catastrophic = false;

  // ensure that only a single claim waits for available slots for core count
  private final Object claimLock = new Object();

  public SuperscalarPipelineStage(
      String name,
      WorkerContext workerContext,
      PipelineStage output,
      PipelineStage error,
      int width) {
    super(name, workerContext, output, error);
    this.width = width;
    claims = new ArrayBlockingQueue<>(width);
  }

  protected abstract void interruptAll();

  protected abstract int claimsRequired(ExecutionContext executionContext);

  @Override
  public String getOperationName() {
    throw new UnsupportedOperationException("use getOperationNames on superscalar stages");
  }

  public abstract int getSlotUsage();

  public Iterable<String> getOperationNames() {
    synchronized (operationNames) {
      return new HashSet<>(operationNames);
    }
  }

  @Override
  protected void start(String operationName, String message) {
    synchronized (operationNames) {
      operationNames.add(operationName);
    }
    super.start(operationName, message);
  }

  @Override
  protected void complete(String operationName, long usecs, long stallUSecs, String status) {
    super.complete(operationName, usecs, stallUSecs, status);
    synchronized (operationNames) {
      operationNames.remove(operationName);
    }
  }

  synchronized void waitForReleaseOrCatastrophe(BlockingQueue<ExecutionContext> queue) {
    boolean interrupted = false;
    while (!catastrophic && isClaimed()) {
      if (output.isClosed()) {
        // interrupt the currently running threads, because they have nowhere to go
        interruptAll();
      }
      ExecutionContext executionContext = queue.poll();
      if (executionContext != null) {
        releaseClaim(executionContext.operation.getName(), claimsRequired(executionContext));
      } else {
        try {
          wait(/* timeout= */ 10);
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

  protected ExecutionContext takeOrDrain(BlockingQueue<ExecutionContext> queue)
      throws InterruptedException {
    boolean interrupted = false;
    InterruptedException exception;
    try {
      while (!isClosed() && !output.isClosed()) {
        ExecutionContext context = queue.poll(10, TimeUnit.MILLISECONDS);
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

  protected synchronized void releaseClaim(String operationName, int slots) {
    // clear interrupted flag for take
    boolean interrupted = Thread.interrupted();
    try {
      for (int i = 0; i < slots; i++) {
        claims.take();
      }
    } catch (InterruptedException e) {
      catastrophic = true;
      getLogger()
          .log(
              Level.SEVERE,
              name
                  + ": could not release claim on "
                  + operationName
                  + ", aborting drain to avoid deadlock");
      close();
    } finally {
      notify();
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  protected String getUsage(int size) {
    return String.format("%s/%d", size, width);
  }

  @SuppressWarnings("unchecked")
  private boolean claim(int count) throws InterruptedException {
    Object handle = new Object();
    int claimed = 0;
    synchronized (claimLock) {
      while (count > 0 && !isClosed()) {
        try {
          if (claims.offer(handle, 10, TimeUnit.MILLISECONDS)) {
            claimed++;
            count--;
          }
        } catch (InterruptedException e) {
          boolean interrupted = Thread.interrupted();
          while (claimed != 0) {
            interrupted = Thread.interrupted() || interrupted;
            try {
              claims.take();
              claimed--;
            } catch (InterruptedException intEx) {
              // ignore, we must release our claims
              e.addSuppressed(intEx);
            }
          }
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
          throw e;
        }
      }
    }
    return count == 0;
  }

  @Override
  public boolean claim(ExecutionContext executionContext) throws InterruptedException {
    return claim(claimsRequired(executionContext));
  }

  @Override
  public void release() {
    releaseClaim("unidentified operation", 1);
  }

  @Override
  protected boolean isClaimed() {
    return !claims.isEmpty();
  }
}
