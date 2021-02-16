// Copyright 2020 The Bazel Authors. All rights reserved.
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

package build.buildfarm.instance.queues;

import java.util.ArrayList;
import java.util.List;

/**
 * This holds various pre-configured worker queues. They can be used directly, or referenced as test
 * examples.
 */
public class WorkerQueueConfigurations {

  public static WorkerQueues gpuAndFallback() {
    WorkerQueues queues = new WorkerQueues();
    queues.AddQueues(WorkerQueueConfigurations.gpuAndFallbackQueues());
    return queues;
  }

  public static WorkerQueues gpu() {
    WorkerQueues queues = new WorkerQueues();
    queues.AddQueues(WorkerQueueConfigurations.gpuQueues());
    return queues;
  }

  /* This is a queue paradigm for a GPU and non-GPU queue */
  private static List<WorkerQueue> gpuAndFallbackQueues() {

    List<WorkerQueue> queues = new ArrayList<WorkerQueue>();

    // add a gpu queue
    queues.add(gpuQueue());

    // add a fallback queue with no required provisions
    WorkerQueue fallbackQueue = new WorkerQueue();
    fallbackQueue.name = "Other";
    queues.add(fallbackQueue);

    return queues;
  }

  /* This is a queue paradigm of only one gpu queue */
  private static List<WorkerQueue> gpuQueues() {

    List<WorkerQueue> queues = new ArrayList<WorkerQueue>();
    queues.add(gpuQueue());
    return queues;
  }

  /* This is an example of a gpu configured queue */
  private static WorkerQueue gpuQueue() {

    // add a gpu queue
    WorkerQueue gpuQueue = new WorkerQueue();
    gpuQueue.name = "GPU";
    gpuQueue.requiredProvisions.put("gpu_required", "");

    return gpuQueue;
  }
}
