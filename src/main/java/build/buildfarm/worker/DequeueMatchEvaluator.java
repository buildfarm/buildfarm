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

import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Platform;
import build.buildfarm.v1test.QueueEntry;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;

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

  /**
   * @field EXEC_PROPERTY_MIN_CORES
   * @brief The exec_property and platform property name for setting min cores.
   * @details This is decided between client and server.
   */
  private static final String EXEC_PROPERTY_MIN_CORES = "min-cores";

  /**
   * @field EXEC_PROPERTY_MAX_CORES
   * @brief The exec_property and platform property name for setting max cores.
   * @details This is decided between client and server.
   */
  private static final String EXEC_PROPERTY_MAX_CORES = "max-cores";

  /**
   * @field WORKER_PLATFORM_CORES_PROPERTY
   * @brief A platform property decided and assigned to a worker.
   * @details This often correlates to execute width of the workers (the max amount of cores it is
   *     willing to give an operation).
   */
  private static final String WORKER_PLATFORM_CORES_PROPERTY = "cores";

  /**
   * @brief Decide whether the worker should keep the operation or put it back on the queue.
   * @details Compares the platform properties of the worker to the operation's platform properties.
   * @param matchSettings The provisions of the worker.
   * @param workerProvisions The provisions of the worker.
   * @param queueEntry An entry recently removed from the queue.
   * @return Whether or not the worker should accept or reject the queue entry.
   * @note Overloaded.
   * @note Suggested return identifier: shouldKeepOperation.
   */
  public static boolean shouldKeepOperation(
      DequeueMatchSettings matchSettings,
      SetMultimap<String, String> workerProvisions,
      QueueEntry queueEntry) {
    // if the queue entry was not actually dequeued, should we still accept it?
    // TODO(luxe): find out why its currently like this, and if that still makes sense.
    if (queueEntry == null) {
      return true;
    }

    return shouldKeepViaPlatform(matchSettings, workerProvisions, queueEntry.getPlatform());
  }

  /**
   * @brief Decide whether the worker should keep the operation or put it back on the queue.
   * @details Compares the platform properties of the worker to the operation's platform properties.
   * @param matchSettings The provisions of the worker.
   * @param workerProvisions The provisions of the worker.
   * @param command A command to evaluate.
   * @return Whether or not the worker should accept or reject the queue entry.
   * @note Overloaded.
   * @note Suggested return identifier: shouldKeepOperation.
   */
  public static boolean shouldKeepOperation(
      DequeueMatchSettings matchSettings,
      SetMultimap<String, String> workerProvisions,
      Command command) {
    return shouldKeepViaPlatform(matchSettings, workerProvisions, command.getPlatform());
  }

  /**
   * @brief Decide whether the worker should keep the operation via platform or put it back on the
   *     queue.
   * @details Compares the platform properties of the worker to the platform properties of the
   *     operation.
   * @param matchSettings The provisions of the worker.
   * @param workerProvisions The provisions of the worker.
   * @param platform The platforms of operation.
   * @return Whether or not the worker should accept or reject the operation.
   * @note Suggested return identifier: shouldKeepOperation.
   */
  private static boolean shouldKeepViaPlatform(
      DequeueMatchSettings matchSettings,
      SetMultimap<String, String> workerProvisions,
      Platform platform) {
    // attempt to execute everything it gets off the queue.
    if (matchSettings.acceptEverything) {
      return true;
    }

    return satisfiesProperties(matchSettings, workerProvisions, platform);
  }

  /**
   * @brief Decide whether the worker should keep the operation by comparing its platform properties
   *     with the queue entry.
   * @details Compares the platform properties of the worker to the platform properties.
   * @param matchSettings The provisions of the worker.
   * @param workerProvisions The provisions of the worker.
   * @param platform The platforms of operation.
   * @return Whether or not the worker should accept or reject the queue entry.
   * @note Suggested return identifier: shouldKeepOperation.
   */
  private static boolean satisfiesProperties(
      DequeueMatchSettings matchSettings,
      SetMultimap<String, String> workerProvisions,
      Platform platform) {
    for (Platform.Property property : platform.getPropertiesList()) {
      if (!satisfiesProperty(matchSettings, workerProvisions, property)) {
        return false;
      }
    }
    return true;
  }

  /**
   * @brief Decide whether the worker should keep the operation by comparing its platform properties
   *     with a queue entry property.
   * @details Checks for certain exact matches on key/values.
   * @param matchSettings The provisions of the worker.
   * @param workerProvisions The provisions of the worker.
   * @param property A property of the queued entry.
   * @return Whether or not the worker should accept or reject the queue entry.
   * @note Suggested return identifier: shouldKeepOperation.
   */
  private static boolean satisfiesProperty(
      DequeueMatchSettings matchSettings,
      SetMultimap<String, String> workerProvisions,
      Platform.Property property) {
    // validate min cores
    if (property.getName().equals(EXEC_PROPERTY_MIN_CORES)) {
      if (!workerProvisions.containsKey(WORKER_PLATFORM_CORES_PROPERTY)) {
        return false;
      }

      int coresRequested = Integer.parseInt(property.getValue());
      int possibleCores =
          Integer.parseInt(
              Iterables.getOnlyElement(workerProvisions.get(WORKER_PLATFORM_CORES_PROPERTY)));
      return possibleCores >= coresRequested;
    }

    // validate max cores
    if (property.getName().equals(EXEC_PROPERTY_MAX_CORES)) {
      return true;
    }

    // accept other properties not specified on the worker
    if (matchSettings.allowUnmatched) {
      return true;
    }

    // ensure exact matches
    return workerProvisions.containsEntry(property.getName(), property.getValue())
        || workerProvisions.containsEntry(property.getName(), "*");
  }
}
