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

import build.buildfarm.instance.MatchListener;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.google.longrunning.Operation;
import java.util.Iterator;
import java.util.List;

/*
    This holds all of the operation queues for a particular instance.
    Each queue is tagged with the required provisions which affects the operations and workers involved with the queue.
*/
public class WorkerQueues implements Iterable<WorkerQueue> {

  /*
    The specific queues whose provision requirements are considered when queuing an operation.
    The queues are evaluated in the order that they are listed.
    I you would like to add a fallback queue with no requirements,
    you must ensure that the queue is listed last, and contains no required provisions.
    That way, all of the provision-requiring queues are considered first,
    before choosing the fallback queue last.
  */
  public List<WorkerQueue> specificQueues = Lists.newArrayList();

  @Override
  public Iterator<WorkerQueue> iterator() {
    return specificQueues.iterator();
  }

  public int queueSize(String queueName) {
    for (WorkerQueue queue : specificQueues) {
      if (queue.name == queueName) {
        return queue.operations.size();
      }
    }
    return 0;
  }

  public void AddQueues(List<WorkerQueue> queues) {
    specificQueues.addAll(queues);
  }

  /* Based on the provided provision information,
     return the first queue whose provision requirements are met.
  */
  public WorkerQueue MatchEligibleQueue(SetMultimap<String, String> provisions)
      throws InterruptedException {

    for (WorkerQueue queue : specificQueues) {

      /*
        Accept the queue if all of its provision requirements are met by the given provisions.
        If no queue is eligible based on the provisions return the last queue for now.
      */
      if (QueueIsEligible(queue, provisions)) {
        return queue;
      }
    }

    throw new InterruptedException("can not find matching queue based on provided provisions");
  }

  public boolean enqueueOperation(Operation operation, SetMultimap<String, String> provisions) {

    // if the user gives null provisions, assume an empty set
    if (provisions == null) {
      provisions = HashMultimap.create();
    }

    try {
      WorkerQueue queue = MatchEligibleQueue(provisions);
      enqueueOperation(queue.operations, operation);
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  public boolean AddWorker(SetMultimap<String, String> provisions, MatchListener listener) {

    try {
      WorkerQueue queue = MatchEligibleQueue(provisions);
      queue.workers.add(new Worker(provisions, listener));
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  public boolean AddWorkers(String queueName, List<Worker> workers) {
    for (WorkerQueue queue : specificQueues) {
      if (queue.name == queueName) {
        queue.workers.addAll(workers);
        return true;
      }
    }
    return false;
  }

  /*
    Removes all instances of an existing listener from any of the queues
  */
  public void removeWorker(MatchListener listener) {

    for (WorkerQueue queue : specificQueues) {
      removeWorkerFromQueue(queue, listener);
    }
  }

  private void enqueueOperation(List<Operation> operations, Operation operation) {
    synchronized (operations) {
      Preconditions.checkState(
          !Iterables.any(
              operations,
              (queuedOperation) -> queuedOperation.getName().equals(operation.getName())));
      operations.add(operation);
    }
  }

  /*
     Check for any unmet required provisions.
     Otherwise, the required provisions of the queue are met and the queue is eligible.
  */
  private Boolean QueueIsEligible(WorkerQueue queue, SetMultimap<String, String> provisions) {
    for (String checkedProvision : queue.requiredProvisions.asMap().keySet()) {
      if (!provisions.asMap().containsKey(checkedProvision)) {
        return false;
      }
    }
    return true;
  }

  private void removeWorkerFromQueue(WorkerQueue queue, MatchListener listener) {
    synchronized (queue.workers) {
      Iterator<Worker> iter = queue.workers.iterator();
      while (iter.hasNext()) {
        if (iter.next().getListener() == listener) {
          iter.remove();
        }
      }
    }
  }
}
