// Copyright 2020 The Buildfarm Authors. All rights reserved.
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

import build.buildfarm.common.ExecutionProperties;
import build.buildfarm.common.MapUtils;
import build.buildfarm.worker.resources.LocalResourceSetUtils;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @class ProvisionedRedisQueue
 * @brief A queue that is designed to hold particularly provisioned elements.
 * @details A provisioned redis queue is an implementation of a queue data structure which
 *     internally uses a redis cluster to distribute the data across shards. Its important to know
 *     that the lifetime of the queue persists before and after the queue data structure is created
 *     (since it exists in redis). Therefore, two redis queues with the same name, would in fact be
 *     the same underlying redis queue. This redis queue comes with a list of required provisions.
 *     If the queue element does not meet the required provisions, it should not be stored in the
 *     queue. Provision queues are intended to represent particular operations that should only be
 *     processed by particular workers. An example use case for this would be to have two dedicated
 *     provision queues for CPU and GPU operations. CPU/GPU requirements would be determined through
 *     the remote api's command platform properties. We designate provision queues to have a set of
 *     "required provisions" (which match the platform properties). This allows the scheduler to
 *     distribute operations by their properties and allows workers to dequeue from particular
 *     queues.
 */
public class ProvisionedRedisQueue {
  /**
   * @field WILDCARD_VALUE
   * @brief Wildcard value.
   * @details Symbol for identifying wildcard in both key/value of provisions.
   */
  public static final String WILDCARD_VALUE = "*";

  /**
   * @field isFullyWildcard
   * @brief If the queue will deem any set of properties eligible.
   * @details If any of the provision keys has a wildcard, we consider anything for the queue to be
   *     eligible.
   */
  private final boolean isFullyWildcard;

  /**
   * @field allowUserUnmatched
   * @brief Can the user provide extra platform properties that are not a part of the queue and
   *     still be matched with it?
   * @details If true, the user can provide a superset of platform properties and still be matched
   *     with the queue.
   */
  private final boolean allowUserUnmatched;

  /**
   * @field provisions
   * @brief Provisions enforced by the queue.
   * @details The provisions are filtered by wildcard.
   */
  private final FilteredProvisions provisions;

  /**
   * @field queue
   * @brief The queue itself.
   * @details A balanced redis queue designed to hold particularly provisioned elements.
   */
  private final BalancedRedisQueue queue;

  /**
   * @brief Constructor.
   * @details Construct the provision queue.
   * @param name The global name of the queue.
   * @param hashtags Hashtags to distribute queue data.
   * @param filterProvisions The filtered provisions of the queue.
   * @note Overloaded.
   */
  public ProvisionedRedisQueue(
      String name, List<String> hashtags, SetMultimap<String, String> filterProvisions) {
    this(name, RedisQueue::decorate, hashtags, filterProvisions, false);
  }

  /**
   * @brief Constructor.
   * @details Construct the provision queue.
   * @param name The global name of the queue.
   * @param hashtags Hashtags to distribute queue data.
   * @param filterProvisions The filtered provisions of the queue.
   * @param allowUserUnmatched Whether the user can provide extra platform properties and still
   *     match the queue.
   * @note Overloaded.
   */
  public ProvisionedRedisQueue(
      String name,
      List<String> hashtags,
      SetMultimap<String, String> filterProvisions,
      boolean allowUserUnmatched) {
    this(name, RedisQueue::decorate, hashtags, filterProvisions, allowUserUnmatched);
  }

  /**
   * @brief Constructor.
   * @details Construct the provision queue.
   * @param name The global name of the queue.
   * @param type The type of the redis queue (regular or priority).
   * @param hashtags Hashtags to distribute queue data.
   * @param filterProvisions The filtered provisions of the queue.
   * @note Overloaded.
   */
  public ProvisionedRedisQueue(
      String name,
      QueueDecorator queueDecorator,
      List<String> hashtags,
      SetMultimap<String, String> filterProvisions) {
    this(name, queueDecorator, hashtags, filterProvisions, false);
  }

  /**
   * @brief Constructor.
   * @details Construct the provision queue.
   * @param name The global name of the queue.
   * @param type The type of the redis queue (regular or priority).
   * @param hashtags Hashtags to distribute queue data.
   * @param filterProvisions The filtered provisions of the queue.
   * @param allowUserUnmatched Whether the user can provide extra platform properties and still
   *     match the queue.
   * @note Overloaded.
   */
  public ProvisionedRedisQueue(
      String name,
      QueueDecorator queueDecorator,
      List<String> hashtags,
      SetMultimap<String, String> filterProvisions,
      boolean allowUserUnmatched) {
    this.queue = new BalancedRedisQueue(name, hashtags, queueDecorator);
    isFullyWildcard = filterProvisions.containsKey(WILDCARD_VALUE);
    provisions = filterProvisionsByWildcard(filterProvisions, isFullyWildcard);
    this.allowUserUnmatched = allowUserUnmatched;
  }

  public boolean isExhausted(Set<String> exhausted) {
    if (isFullyWildcard) {
      return false;
    }

    // if there is any intersection with the exhausted list for requirements
    // we will consider this queue exhausted
    for (Map.Entry<String, String> requirement : provisions.required) {
      String resourceName = LocalResourceSetUtils.getResourceName(requirement.getKey());
      if (exhausted.contains(resourceName)) {
        return true;
      }
    }
    return false;
  }

  /**
   * @brief Checks required properties.
   * @details Checks whether the properties given fulfill all of the required provisions of the
   *     queue.
   * @param properties Properties to check that requirements are met.
   * @return Whether the queue is eligible based on the properties given.
   * @note Suggested return identifier: isEligible.
   */
  public boolean isEligible(SetMultimap<String, String> properties) {
    // check if a property is specifically requesting to match with the queue
    // any attempt to specifically match will not evaluate other properties
    Set<String> selected = properties.get(ExecutionProperties.CHOOSE_QUEUE);
    if (!selected.isEmpty()) {
      return selected.contains(queue.getName());
    }

    // fully wildcarded queues are always eligible
    if (isFullyWildcard) {
      return true;
    }
    // all required non-wildcard provisions must be matched
    Set<Map.Entry<String, String>> requirements = new HashSet<>(provisions.required);
    for (Map.Entry<String, String> property : properties.entries()) {
      // for each of the properties specified, we must match requirements
      if (!provisions.wildcard.contains(property.getKey())
          && !requirements.remove(property)
          && !allowUserUnmatched) {
        return false;
      }
    }
    return requirements.isEmpty();
  }

  /**
   * @brief Explain eligibility.
   * @details Returns an explanation as to why the properties provided are eligible / ineligible to
   *     be placed on the queue.
   * @param properties Properties to get an eligibility explanation of.
   * @return An explanation on the eligibility of the provided properties.
   * @note Suggested return identifier: explanation.
   */
  public String explainEligibility(SetMultimap<String, String> properties) {
    EligibilityResult result = getEligibilityResult(properties);
    return toString(result);
  }

  /**
   * @brief Get queue.
   * @details Obtain the internal queue.
   * @return The internal queue.
   * @note Suggested return identifier: queue.
   */
  public BalancedRedisQueue queue() {
    return queue;
  }

  /**
   * @brief Filter the provisions into separate sets by checking for the existence of wildcards.
   * @details This will organize the incoming provisions into separate sets.
   * @param filterProvisions The filtered provisions of the queue.
   * @param isFullyWildcard If the queue will deem any set of properties eligible.
   * @return Provisions filtered by wildcard.
   * @note Suggested return identifier: filteredProvisions.
   */
  private static FilteredProvisions filterProvisionsByWildcard(
      SetMultimap<String, String> filterProvisions, boolean isFullyWildcard) {
    FilteredProvisions provisions = new FilteredProvisions();
    provisions.wildcard =
        isFullyWildcard
            ? ImmutableSet.of()
            : filterProvisions.asMap().entrySet().stream()
                .filter(e -> e.getValue().contains(ProvisionedRedisQueue.WILDCARD_VALUE))
                .map(Map.Entry::getKey)
                .collect(ImmutableSet.toImmutableSet());
    provisions.required =
        isFullyWildcard
            ? ImmutableSet.of()
            : filterProvisions.entries().stream()
                .filter(e -> !provisions.wildcard.contains(e.getKey()))
                .collect(ImmutableSet.toImmutableSet());
    return provisions;
  }

  /**
   * @brief Get eligibility result.
   * @details Perform eligibility check with detailed information on evaluation.
   * @param properties Properties to get an eligibility explanation of.
   * @return Detailed results on the evaluation of an eligibility check.
   * @note Suggested return identifier: eligibilityResult.
   */
  private EligibilityResult getEligibilityResult(SetMultimap<String, String> properties) {
    EligibilityResult result = new EligibilityResult();
    result.queueName = queue.getName();
    result.isEligible = isEligible(properties);
    result.isFullyWildcard = isFullyWildcard;
    result.isSpecificallyChosen = false;
    result.allowsUnmatched = allowUserUnmatched;

    // check if a property is specifically requesting to match with the queue
    // any attempt to specifically match will not evaluate other properties
    Set<String> selected = properties.get(ExecutionProperties.CHOOSE_QUEUE);
    if (!selected.isEmpty()) {
      result.isSpecificallyChosen = selected.contains(queue.getName());
    }

    // gather matched, unmatched, and still required properties
    ImmutableSetMultimap.Builder<String, String> matched = ImmutableSetMultimap.builder();
    ImmutableSetMultimap.Builder<String, String> unmatched = ImmutableSetMultimap.builder();
    ImmutableSetMultimap.Builder<String, String> stillRequired = ImmutableSetMultimap.builder();
    Set<Map.Entry<String, String>> requirements = new HashSet<>(provisions.required);
    for (Map.Entry<String, String> property : properties.entries()) {
      if (!provisions.wildcard.contains(property.getKey()) && !requirements.remove(property)) {
        unmatched.put(property);
      } else {
        matched.put(property);
      }
    }
    stillRequired.putAll(requirements);

    result.matched = matched.build();
    result.unmatched = unmatched.build();
    result.stillRequired = stillRequired.build();

    return result;
  }

  /**
   * @brief Convert eligibility result to printable string.
   * @details Used for visibility / debugging.
   * @param result Detailed results on the evaluation of an eligibility check.
   * @return An explanation on the eligibility of the provided properties.
   * @note Suggested return identifier: explanation.
   */
  private static String toString(EligibilityResult result) {
    String explanation = "";
    if (result.isEligible) {
      explanation += "The properties are eligible for the " + result.queueName + " queue.\n";
    } else {
      explanation += "The properties are not eligible for the " + result.queueName + " queue.\n";
    }

    if (result.isSpecificallyChosen) {
      explanation += "The queue was specifically chosen.\n";
      return explanation;
    }

    if (result.isFullyWildcard) {
      explanation += "The queue is fully wildcard.\n";
      return explanation;
    }

    explanation += "matched: " + MapUtils.toString(result.matched.asMap()) + "\n";
    explanation += "unmatched: " + MapUtils.toString(result.unmatched.asMap()) + "\n";
    explanation += "still required: " + MapUtils.toString(result.stillRequired.asMap()) + "\n";
    return explanation;
  }
}
