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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

abstract class SuperscalarPipelineStage extends PipelineStage {

  private final int width;
  private final BlockingQueue claims;
  private boolean catastrophic = false;

  public SuperscalarPipelineStage(
      String name,
      WorkerContext workerContext,
      PipelineStage output,
      PipelineStage error,
      int width) {
    super(name, workerContext, output, error);
    this.width = width;
    claims = new ArrayBlockingQueue(width);
  }

  protected abstract void interruptAll();

  synchronized void waitForReleaseOrCatastrophe() {
    boolean interrupted = false;
    while (!catastrophic && isClaimed()) {
      if (output.isClosed()) {
        // interrupt the currently running threads, because they have nowhere to go
        interruptAll();
      }
      try {
        wait();
      } catch (InterruptedException e) {
        interrupted = Thread.interrupted() || interrupted;
        // ignore, we will throw it eventually
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
    waitForReleaseOrCatastrophe();
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    throw exception;
  }

  protected synchronized void releaseClaim(String operationName) {
    try {
      claims.take();
    } catch (InterruptedException e) {
      catastrophic = true;
      getLogger().severe(name + ": could not release claim on " + operationName + ", aborting drain to avoid deadlock");
      close();
    } finally {
      notify();
    }
  }

  protected String getUsage(int size) {
    return String.format("%s/%d", size, width);
  }

  @Override
  public boolean claim() throws InterruptedException {
    Object handle = new Object();
    boolean offered = false;
    while (!offered && !isClosed()) {
      offered = claims.offer(handle, 10, TimeUnit.MILLISECONDS);
    }
    return offered;
  }

  @Override
  public void release() {
    releaseClaim("unidentified operation");
  }

  @Override
  protected boolean isClaimed() {
    return claims.size() > 0;
  }
}
