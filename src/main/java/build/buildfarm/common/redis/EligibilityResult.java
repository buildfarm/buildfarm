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

import com.google.common.collect.SetMultimap;

/**
 * @class EligibilityResult
 * @brief Detailed results from checking eligibility of properties on a provisioned redis queue.
 * @details Useful for visibility and debugging into the queue selection algorithm.
 */
public class EligibilityResult {
  /**
   * @field queueName
   * @brief The name of the queue the eligibility was tested on.
   * @details The name of the provisioned redis queue.
   */
  public String queueName;

  /**
   * @field allowsUnmatched
   * @brief Determines how unmatched properties effect eligibility.
   * @details If true, properties can remain unmatched yet still eligible.
   */
  public boolean allowsUnmatched;

  /**
   * @field isEligible
   * @brief Whether the properties were eligible for the queue.
   * @details Determined the same way queues are selected.
   */
  public boolean isEligible;

  /**
   * @field isFullyWildcard
   * @brief Whether the queue is fully wildcard.
   * @details Fully wildcard queues accept all properties.
   */
  public boolean isFullyWildcard;

  /**
   * @field isSpecificallyChosen
   * @brief Whether the queue was specifically chosen.
   * @details A special property was used to specifically match to the queue. This automatically
   *     makes it eligible.
   */
  public boolean isSpecificallyChosen;

  /**
   * @field matched
   * @brief Properties that were correctly matched for the queue.
   * @details Contribute to successful eligibility.
   */
  public SetMultimap<String, String> matched;

  /**
   * @field unmatched
   * @brief Properties that were not correctly matched for the queue.
   * @details Contribute to failure of eligibility.
   */
  public SetMultimap<String, String> unmatched;

  /**
   * @field stillRequired
   * @brief Properties that are still required for the queue.
   * @details Contribute to failure of eligibility.
   */
  public SetMultimap<String, String> stillRequired;
}
