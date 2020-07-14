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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
  /// @field   WILDCARD_VALUE
  /// @brief   Wildcard value.
  /// @details Symbol for identifying wildcard in both key/value of provisions.
  ///
  public static final String WILDCARD_VALUE = "*";

  ///
  /// @field   isFullyWildcard
  /// @brief   If the queue will deem any set of properties eligible.
  /// @details If any of the provision keys has a wildcard, we consider
  ///          anything for the queue to be eligible.
  ///
  private final boolean isFullyWildcard;

  ///
  /// @field   wildcardProvisions
  /// @brief   The wildcard provisions of the queue.
  /// @details A filtered set of all provisions that use wildcards.
  ///
  private Set<String> wildcardProvisions;

  ///
  /// @field   requiredProvisions
  /// @brief   The required provisions of the queue.
  /// @details The required provisions to allow workers and operations to be
  ///          added to the queue. These often match the remote api's command
  ///          platform properties.
  ///
  private Set<Map.Entry<String, String>> requiredProvisions;

  ///
  /// @field   queue
  /// @brief   The queue itself.
  /// @details A balanced redis queue designed to hold particularly provisioned
  ///          elements.
  ///
  private final BalancedRedisQueue queue;

  ///
  /// @brief   Constructor.
  /// @details Construct the provision queue.
  /// @param   name             The global name of the queue.
  /// @param   hashtags         Hashtags to distribute queue data.
  /// @param   filterProvisions The filtered provisions of the queue.
  ///
  public ProvisionedRedisQueue(
      String name, List<String> hashtags, SetMultimap<String, String> filterProvisions) {
    this.queue = new BalancedRedisQueue(name, hashtags);
    isFullyWildcard = filterProvisions.containsKey(WILDCARD_VALUE);
    filterProvisionsByWildcard(filterProvisions);
  }
  ///
  /// @brief   Checks required properties.
  /// @details Checks whether the properties given fulfill all of the required
  ///          provisions of the queue.
  /// @param   properties Properties to check that requirements are met.
  /// @return  Whether the queue is eligible based on the properties given.
  /// @note    Suggested return identifier: isEligible.
  ///
  public boolean isEligible(SetMultimap<String, String> properties) {
    // set intersection of requirements and properties with wildcarding
    if (isFullyWildcard) {
      return true;
    }
    // all required non-wildcard provisions must be matched
    Set<Map.Entry<String, String>> requirements = new HashSet<>(requiredProvisions);
    for (Map.Entry<String, String> property : properties.entries()) {
      // for each of the properties specified, we must match requirements
      if (!wildcardProvisions.contains(property.getKey()) && !requirements.remove(property)) {
        return false;
      }
    }
    return requirements.isEmpty();
  }
  ///
  /// @brief   Explain eligibility.
  /// @details Returns an explanation as to why the properties provided are
  ///          eligible / ineligible to be placed on the queue.
  /// @param   properties Properties to get an eligibility explanation of.
  /// @return  An explanation on the eligibility of the provided properties.
  /// @note    Suggested return identifier: explanation.
  ///
  public String explainEligibility(SetMultimap<String, String> properties) {
    // is it eligible?
    String explanation = new String();
    if (isEligible(properties)) {
      explanation += "The properties are eligible for the queue.\n";
    } else {
      explanation += "The properties are not eligible for the queue.\n";
    }

    // is it fully wildcard?
    if (isFullyWildcard) {
      explanation += "The queue is fully wildcard.\n";
      return explanation;
    }

    // gather matched, unmatched, and still required properties
    ImmutableSetMultimap.Builder<String, String> matched = ImmutableSetMultimap.builder();
    ImmutableSetMultimap.Builder<String, String> unmatched = ImmutableSetMultimap.builder();
    ImmutableSetMultimap.Builder<String, String> stillRequired = ImmutableSetMultimap.builder();
    Set<Map.Entry<String, String>> requirements = new HashSet<>(requiredProvisions);
    for (Map.Entry<String, String> property : properties.entries()) {
      if (!wildcardProvisions.contains(property.getKey()) && !requirements.remove(property)) {
        unmatched.put(property);
      } else {
        matched.put(property);
      }
    }
    stillRequired.putAll(requirements);

    // provide further explanation on individual properties
    explanation += "matched: " + matched + "\n";
    explanation += "unmatched: " + unmatched + "\n";
    explanation += "still required: " + stillRequired + "\n";
    return explanation;
  }
  ///
  /// @brief   Get queue.
  /// @details Obtain the internal queue.
  /// @return  The internal queue.
  /// @note    Suggested return identifier: queue.
  ///
  public BalancedRedisQueue queue() {
    return queue;
  }
  ///
  /// @brief   Filter the provisions into separate sets by checking for the
  ///          existence of wildcards.
  /// @details This will organize the incoming provisions into separate sets.
  /// @param   filterProvisions The filtered provisions of the queue.
  ///
  private void filterProvisionsByWildcard(SetMultimap<String, String> filterProvisions) {
    wildcardProvisions =
        isFullyWildcard
            ? ImmutableSet.of()
            : filterProvisions.asMap().entrySet().stream()
                .filter(e -> e.getValue().contains(WILDCARD_VALUE))
                .map(e -> e.getKey())
                .collect(ImmutableSet.toImmutableSet());
    requiredProvisions =
        isFullyWildcard
            ? ImmutableSet.of()
            : filterProvisions.entries().stream()
                .filter(e -> !wildcardProvisions.contains(e.getKey()))
                .collect(ImmutableSet.toImmutableSet());
  }
}
