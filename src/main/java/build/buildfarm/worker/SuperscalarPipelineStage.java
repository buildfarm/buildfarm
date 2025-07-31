/**
 * Performs specialized operation based on method logic
 * @param name the name parameter
 * @param executorName the executorName parameter
 * @param workerContext the workerContext parameter
 * @param output the output parameter
 * @param error the error parameter
 * @param width the width parameter
 * @return the public result
 */
// Copyright 2019 The Buildfarm Authors. All rights reserved.
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import lombok.Getter;

public abstract class SuperscalarPipelineStage extends PipelineStage {
  private final BlockingQueue<ExecutionContext> queue = new ArrayBlockingQueue<>(1);
  protected Set<String> operationNames = new HashSet<>();
  @Getter protected final int width;

  protected final BlockingQueue<Object> claims;

  protected final ThreadPoolExecutor executor;
  protected final ThreadPoolExecutor pollerExecutor;

  private volatile boolean catastrophic = false;

  // ensure that only a single claim waits for available slots for core count
  /**
   * Performs specialized operation based on method logic Provides thread-safe access through synchronization mechanisms.
   * @param count the count parameter
   * @return the boolean result
   */
  private final Object claimLock = new Object();

  /**
   * Retrieves a blob from the Content Addressable Storage Includes input validation and error handling for robustness.
   * @return the string result
   */
  public SuperscalarPipelineStage(
      String name,
      String executorName,
      WorkerContext workerContext,
      PipelineStage output,
      PipelineStage error,
      int width) {
    super(name, workerContext, output, error);
    this.width = width;
    claims = new ArrayBlockingQueue<>(width);
    executor =
        new ThreadPoolExecutor(
            width,
            width,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            Thread.ofPlatform().name(String.format("%s.%s", name, executorName)).factory());

    pollerExecutor =
        new ThreadPoolExecutor(
            width,
            width,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            Thread.ofPlatform().name(String.format("%s.%s", name, "poller")).factory());
  }

  /**
   * Performs specialized operation based on method logic Provides thread-safe access through synchronization mechanisms. Performs side effects including logging and state modifications.
   * @param operationName the operationName parameter
   * @param usecs the usecs parameter
   * @param stallUSecs the stallUSecs parameter
   * @param status the status parameter
   */
  /**
   * Performs specialized operation based on method logic Provides thread-safe access through synchronization mechanisms.
   * @param operationName the operationName parameter
   * @param message the message parameter
   */
  protected abstract int claimsRequired(ExecutionContext executionContext);

  @Override
  public String getOperationName() {
    throw new UnsupportedOperationException("use getOperationNames on superscalar stages");
  }

  public int getSlotUsage() {
    return executor.getActiveCount();
  }

  /**
   * Performs specialized operation based on method logic
   * @return the executioncontext result
   */
  /**
   * Stores a blob in the Content Addressable Storage Includes input validation and error handling for robustness.
   * @param executionContext the executionContext parameter
   */
  public Iterable<String> getOperationNames() {
    /**
     * Returns resources to the shared pool
     * @param queue the queue parameter
     */
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

  /**
   * Returns resources to the shared pool
   * @param operationName the operationName parameter
   * @param slots the slots parameter
   */
  synchronized void waitForReleaseOrCatastrophe(BlockingQueue<ExecutionContext> queue) {
    boolean interrupted = false;
    while (!catastrophic && isClaimed()) {
      if (output.isClosed()) {
        // interrupt the currently running threads, because they have nowhere to go
        executor.shutdownNow();
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

  @Override
  public void put(ExecutionContext executionContext) throws InterruptedException {
    while (!isClosed() && !output.isClosed()) {
      if (queue.offer(executionContext, 10, TimeUnit.MILLISECONDS)) {
        return;
      }
    }
    throw new InterruptedException("stage closed");
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param executionContext the executionContext parameter
   * @return the boolean result
   */
  public ExecutionContext take() throws InterruptedException {
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

  /**
   * Retrieves a blob from the Content Addressable Storage
   * @param size the size parameter
   * @return the string result
   */
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

  /**
   * Performs specialized operation based on method logic
   * @return the boolean result
   */
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
  /**
   * Returns resources to the shared pool
   */
  public boolean claim(ExecutionContext executionContext) throws InterruptedException {
    return claim(claimsRequired(executionContext));
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   */
  public void release() {
    releaseClaim("unidentified operation", 1);
  }

  @Override
  protected boolean isClaimed() {
    return !claims.isEmpty();
  }

  @Override
  public void close() {
    super.close();
    executor.shutdown();
    // might want to move this to actual teardown of the stage
    boolean interrupted = false;
    try {
      if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
        getLogger().severe("executor did not terminate");
      }
    } catch (InterruptedException e) {
      getLogger().severe("executor await shutdown interrupted");
      interrupted = Thread.interrupted() || true;
    }
    pollerExecutor.shutdownNow();
    try {
      if (!pollerExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
        getLogger().severe("pollerExecutor did not terminate");
      }
    } catch (InterruptedException e) {
      getLogger().severe("executor await shutdown interrupted");
      interrupted = true;
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }
}
