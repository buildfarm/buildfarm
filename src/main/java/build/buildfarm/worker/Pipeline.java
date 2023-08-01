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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import lombok.extern.java.Log;

@Log
public class Pipeline {
  private final Map<PipelineStage, Thread> stageThreads;
  private final Map<PipelineStage, Integer> stageClosePriorities;
  private Thread joiningThread = null;
  private boolean closing = false;
  // FIXME ThreadGroup?

  public Pipeline() {
    stageThreads = new HashMap<>();
    stageClosePriorities = new HashMap<>();
  }

  public void add(PipelineStage stage, int closePriority) {
    stageThreads.put(stage, new Thread(stage));
    if (closePriority < 0) {
      throw new IllegalArgumentException("closePriority cannot be negative");
    }
    stageClosePriorities.put(stage, closePriority);
  }

  public void start() {
    for (Thread stageThread : stageThreads.values()) {
      stageThread.start();
    }
  }

  public void close() throws InterruptedException {
    synchronized (this) {
      closing = true;
      if (joiningThread != null) {
        joiningThread.interrupt();
        joiningThread = null;
      }
    }
    join(true);
  }

  /** Inform MatchStage to stop matching or picking up new Operations from queue. */
  public void stopMatchingOperations() {
    for (PipelineStage stage : stageClosePriorities.keySet()) {
      if (stage instanceof MatchStage) {
        ((MatchStage) stage).prepareForGracefulShutdown();
        return;
      }
    }
  }

  /**
   * Checking if there is any ongoing actions in any stages of the pipeline.
   *
   * @return true of all PipelineStages are empty.
   */
  public boolean isEmpty() {
    for (PipelineStage stage : stageClosePriorities.keySet()) {
      // InputFetchStage
      if (stage instanceof InputFetchStage) {
        int slotUsage = ((InputFetchStage) stage).getSlotUsage();
        if (slotUsage != 0) {
          log.log(
              Level.INFO,
              String.format("InputFetchStage is not empty with slot usage: %d!", slotUsage));
          return false;
        }
      } else if (stage instanceof ExecuteActionStage) { // ExecuteActionStage
        int slotUsage = ((ExecuteActionStage) stage).getSlotUsage();
        if (slotUsage != 0) {
          log.log(Level.INFO, String.format("ExecuteActionStage slot usage: %d!", slotUsage));
          return false;
        }
      } else { // not SuperScalar stage
        if (stage.claimed) {
          log.log(Level.INFO, "NonSuperScalarPipelineStage is not empty yet!");
          return false;
        }
      }
    }
    return true;
  }

  public boolean hasStages() {
    return !stageThreads.isEmpty();
  }

  public void join() throws InterruptedException {
    synchronized (this) {
      joiningThread = Thread.currentThread();
    }
    join(false);
    synchronized (this) {
      joiningThread = null;
    }
  }

  private void join(boolean closeStage) throws InterruptedException {
    List<PipelineStage> inactiveStages = new ArrayList<>();
    InterruptedException intEx = null;
    try {
      while (!stageThreads.isEmpty()) {
        if (closeStage) {
          PipelineStage stageToClose = null;
          int maxPriority = -1;
          for (PipelineStage stage : stageThreads.keySet()) {
            if (stage.isClosed()) {
              stageToClose = null;
              break;
            }
            if (stageClosePriorities.get(stage) > maxPriority) {
              maxPriority = stageClosePriorities.get(stage);
              stageToClose = stage;
            }
          }
          if (stageToClose != null && !stageToClose.isClosed()) {
            log.log(Level.FINER, "Closing stage at priority " + maxPriority);
            stageToClose.close();
          }
        }
        for (Map.Entry<PipelineStage, Thread> stageThread : stageThreads.entrySet()) {
          PipelineStage stage = stageThread.getKey();
          Thread thread = stageThread.getValue();
          try {
            thread.join(closeStage ? 1 : 1000);
          } catch (InterruptedException e) {
            if (!closeStage) {
              synchronized (this) {
                while (closing && !stageThreads.isEmpty()) {
                  wait();
                }
              }
              throw e;
            }
            intEx = e;
          }

          if (!thread.isAlive()) {
            log.log(
                Level.FINER,
                "Stage "
                    + stage.name()
                    + " has exited at priority "
                    + stageClosePriorities.get(stage));
            inactiveStages.add(stage);
          } else if (stage.isClosed()) {
            log.log(
                Level.INFO,
                "Interrupting unterminated closed thread in stage "
                    + stage.name()
                    + " at priority "
                    + stageClosePriorities.get(stage));
            thread.interrupt();
          }
        }
        closeStage = false;
        for (PipelineStage stage : inactiveStages) {
          synchronized (this) {
            stageThreads.remove(stage);
            closeStage = true;
            notify();
          }
        }
        inactiveStages.clear();
      }
    } finally {
      if (intEx != null) {
        throw intEx;
      }
    }
  }
}
