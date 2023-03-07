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

package build.buildfarm.worker;

import build.bazel.remote.execution.v2.Platform;
import build.buildfarm.common.ExecutionProperties;
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.worker.resources.LocalResourceSet;
import build.buildfarm.worker.resources.LocalResourceSetUtils;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import org.jetbrains.annotations.NotNull;

/**
 * @class DequeueMatchEvaluator
 * @brief Algorithm for deciding whether a worker should keep a dequeued operation.
 * @details When a worker takes an entry off of the queue, should it decide to keep that entry or
 *     reject and requeue it? In some sense, it should keep all entries because they have already
 *     been vetted for that particular worker. This is because the scheduler matches operations to
 *     particular queues, and workers match themselves to which queues they want to read from. But
 *     should the worker always blindly take what it pops off? And can they trust the scheduler?
 *     There may be situations where the worker chooses to give operations back based on particular
 *     contexts not known to the scheduler. For example, you might have a variety of workers with
 *     different amounts of cpu cores all sharing the same queue. The queue may accept N-core
 *     operations, because N-core workers exist in the pool, but there are additionally some lower
 *     core workers that would need to forfeit running the operation. All the reasons a worker may
 *     decide it can't take on the operation and should give it back are implemented here. The
 *     settings provided allow varying amount of leniency when evaluating the platform properties.
 */
public class DequeueMatchEvaluator {
  private static BuildfarmConfigs configs = BuildfarmConfigs.getInstance();
  /**
   * @brief Decide whether the worker should keep the operation or put it back on the queue.
   * @details Compares the platform properties of the worker to the operation's platform properties.
   * @param workerProvisions The provisions of the worker.
   * @param resourceSet The limited resources that the worker has available.
   * @param queueEntry An entry recently removed from the queue.
   * @return Whether or not the worker should accept or reject the queue entry.
   * @note Overloaded.
   * @note Suggested return identifier: shouldKeepOperation.
   */
  @SuppressWarnings("NullableProblems")
  @NotNull
  public static boolean shouldKeepOperation(
      SetMultimap<String, String> workerProvisions,
      LocalResourceSet resourceSet,
      QueueEntry queueEntry) {
    return shouldKeepViaPlatform(workerProvisions, resourceSet, queueEntry.getPlatform());
  }

  /**
   * @brief Decide whether the worker should keep the operation via platform or put it back on the
   *     queue.
   * @details Compares the platform properties of the worker to the platform properties of the
   *     operation.
   * @param workerProvisions The provisions of the worker.
   * @param resourceSet The limited resources that the worker has available.
   * @param platform The platforms of operation.
   * @return Whether or not the worker should accept or reject the operation.
   * @note Suggested return identifier: shouldKeepOperation.
   */
  @SuppressWarnings("NullableProblems")
  @NotNull
  private static boolean shouldKeepViaPlatform(
      SetMultimap<String, String> workerProvisions,
      LocalResourceSet resourceSet,
      Platform platform) {
    // attempt to execute everything the worker gets off the queue,
    // provided there is enough resources to do so.
    // this is a recommended configuration.
    if (!LocalResourceSetUtils.claimResources(platform, resourceSet)) {
      return false;
    }
    if (configs.getWorker().getDequeueMatchSettings().isAcceptEverything()) {
      return true;
    }

    return satisfiesProperties(workerProvisions, platform);
  }

  /**
   * @brief Decide whether the worker should keep the operation by comparing its platform properties
   *     with the queue entry.
   * @details Compares the platform properties of the worker to the platform properties.
   * @param workerProvisions The provisions of the worker.
   * @param platform The platforms of operation.
   * @return Whether or not the worker should accept or reject the queue entry.
   * @note Suggested return identifier: shouldKeepOperation.
   */
  @SuppressWarnings("NullableProblems")
  @NotNull
  private static boolean satisfiesProperties(
      SetMultimap<String, String> workerProvisions, Platform platform) {
    for (Platform.Property property : platform.getPropertiesList()) {
      if (!satisfiesProperty(workerProvisions, property)) {
        return false;
      }
    }
    return true;
  }

  /**
   * @brief Decide whether the worker should keep the operation by comparing its platform properties
   *     with a queue entry property.
   * @details Checks for certain exact matches on key/values.
   * @param workerProvisions The provisions of the worker.
   * @param property A property of the queued entry.
   * @return Whether or not the worker should accept or reject the queue entry.
   * @note Suggested return identifier: shouldKeepOperation.
   */
  @SuppressWarnings("NullableProblems")
  @NotNull
  private static boolean satisfiesProperty(
      SetMultimap<String, String> workerProvisions, Platform.Property property) {
    // validate min cores
    if (property.getName().equals(ExecutionProperties.MIN_CORES)) {
      if (!workerProvisions.containsKey(ExecutionProperties.CORES)) {
        return false;
      }

      int coresRequested = Integer.parseInt(property.getValue());
      int possibleCores =
          Integer.parseInt(
              Iterables.getOnlyElement(workerProvisions.get(ExecutionProperties.CORES)));
      return possibleCores >= coresRequested;
    }

    // validate max cores
    if (property.getName().equals(ExecutionProperties.MAX_CORES)) {
      return true;
    }

    // validate min or max amount of memory
    if (property.getName().equals(ExecutionProperties.MIN_MEM)
        || property.getName().equals(ExecutionProperties.MAX_MEM)) {
      // Consider unlimited memory if the worker have not set the MAX_CORES property.
      if (!workerProvisions.containsKey(ExecutionProperties.MAX_MEM)) {
        return true;
      }

      long memBytesRequested = Long.parseLong(property.getValue());
      long possibleMemories =
          Long.parseLong(
              Iterables.getOnlyElement(workerProvisions.get(ExecutionProperties.MAX_MEM)));
      return possibleMemories >= memBytesRequested;
    }

    // accept other properties not specified on the worker
    if (configs.getWorker().getDequeueMatchSettings().isAllowUnmatched()) {
      return true;
    }

    // ensure exact matches
    return workerProvisions.containsEntry(property.getName(), property.getValue())
        || workerProvisions.containsEntry(property.getName(), "*");
  }
}
