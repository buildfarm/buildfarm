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

package build.buildfarm.operations;

import java.util.HashMap;
import java.util.Map;

/**
 * @class FindOperationsResults
 * @brief The results from searching for an operation.
 * @details This will only observe operations; not affect their state.
 */
public class FindOperationsResults {

  /**
   * @field operations
   * @brief All of the operations found based on the search query.
   * @details The key is the operation keu, and the value is all the collected information about the
   *     operation.
   */
  public Map<String, EnrichedOperation> operations = new HashMap<String, EnrichedOperation>();

  /**
   * @brief Get a string message for the results.
   * @details This message is useful for logging.
   * @return A message representation of the FindOperationsResults.
   * @note Suggested return identifier: message.
   */
  public String toMessage() {
    StringBuilder message = new StringBuilder();
    message
        .append(String.format("results: %d\n", operations.size()))
        .append(String.join("\n", operations.keySet()));
    return message.toString();
  }
}
