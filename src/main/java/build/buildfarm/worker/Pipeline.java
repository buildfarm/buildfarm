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

public class Pipeline {
  private final PipelineStage initial;
  private final Map<PipelineStage, Thread> stageThreads;
  private final Map<PipelineStage, Integer> stageClosePriorities;
  // FIXME ThreadGroup?

  public Pipeline() {
    this(/* initial= */ null);
  }

  public Pipeline(PipelineStage initial) {
    this.initial = initial;
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

  public void put(OperationContext operationContext) throws InterruptedException {
    initial.put(operationContext);
  }

  public void close() {
    join(true);
  }

  public void join() {
    join(false);
  }

  private void join(boolean closeStage) {
    List<PipelineStage> inactiveStages = new ArrayList<>();
    boolean wasInterrupted = false;
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
            System.out.println("Closing stage at priority " + maxPriority);
            stageToClose.close();
          }
        }
        for (Map.Entry<PipelineStage, Thread> stageThread : stageThreads.entrySet()) {
          PipelineStage stage = stageThread.getKey();
          Thread thread = stageThread.getValue();
          try {
            thread.join(closeStage ? 1 : 1000);
          } catch (InterruptedException ex) {
            wasInterrupted = true;
          }

          if (!thread.isAlive()) {
            System.out.println("Stage has exited at priority " + stageClosePriorities.get(stage));
            inactiveStages.add(stage);
          } else if (stage.isClosed()) {
            System.out.println("Interrupting unterminated closed thread at priority " + stageClosePriorities.get(stage));
            thread.interrupt();
          }
        }
        closeStage = false;
        for (PipelineStage stage : inactiveStages) {
          stageThreads.remove(stage);
          closeStage = true;
        }
        inactiveStages.clear();
      }
    } finally {
      if (wasInterrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
