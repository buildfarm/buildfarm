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

package build.buildfarm.common.redis;

import com.google.common.collect.SetMultimap;
import java.util.List;

///
/// @class   ProvisionedRedisQueue
/// @brief   A queue that is designed to hold particularly provisioned
///          elements.
/// @details A provisioned redis queue is an implementation of a queue data
///          structure which internally uses a redis cluster to distribute the
///          data across shards. Its important to know that the lifetime of
///          the queue persists before and after the queue data structure is
///          created (since it exists in redis). Therefore, two redis queues
///          with the same name, would in fact be the same underlying redis
///          queue. This redis queue comes with a list of required provisions.
///          If the queue element does not meet the required provisions, it
///          should not be stored in the queue. Provision queues are intended
///          to represent particular operations that should only be processed
///          by particular workers. An example use case for this would be to
///          have two dedicated provision queues for CPU and GPU operations.
///          CPU/GPU requirements would be determined through the remote api's
///          command platform properties. We designate provision queues to
///          have a set of "required provisions" (which match the platform
///          properties). This allows the scheduler to distribute operations
///          by their properties and allows workers to dequeue from particular
///          queues.
///
public class ProvisionedRedisQueue {

  ///
  /// @field   requiredProvisions
  /// @brief   The required provisions of the queue.
  /// @details The required provisions to allow workers and operations to be
  ///          added to the queue. These often match the remote api's command
  ///          platform properties.
  ///
  private SetMultimap<String, String> requiredProvisions;

  ///
  /// @field   queue
  /// @brief   The queue itself.
  /// @details A balanced redis queue designed to hold particularly provisioned
  ///          elements.
  ///
  private BalancedRedisQueue queue;

  ///
  /// @brief   Constructor.
  /// @details Construct the provision queue.
  /// @param   name               The global name of the queue.
  /// @param   hashtags           Hashtags to distribute queue data.
  /// @param   requiredProvisions The required provisions of the queue.
  ///
  public void redisProvisionQueue(
      String name, List<String> hashtags, SetMultimap<String, String> requiredProvisions) {
    this.queue = new BalancedRedisQueue(name, hashtags);
    this.requiredProvisions = requiredProvisions;
  }
  ///
  /// @brief   Checks required provisions.
  /// @details Checks whether the provisions given fulfill all of the required
  ///          provisions of the queue.
  /// @param   provisions Provisions to check that requirements are met.
  /// @return  Whether the queue is eligible based on the provisions given.
  /// @note    Suggested return identifier: isEligible.
  ///
  public boolean isEligible(SetMultimap<String, String> provisions) {
    for (String checkedProvision : requiredProvisions.asMap().keySet()) {
      if (!provisions.asMap().containsKey(checkedProvision)) {
        return false;
      }
    }
    return true;
  }
}
