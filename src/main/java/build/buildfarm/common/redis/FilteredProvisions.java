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

import java.util.Map;
import java.util.Set;

/**
 * @class FilteredProvisions
 * @brief Provisions of a provisioned redis queue.
 * @details These provisions are filtered by wildcards and used by the provisioned redis queue.
 */
public class FilteredProvisions {
  /**
   * @field wildcard
   * @brief The wildcard provisions of the queue.
   * @details A filtered set of all provisions that use wildcards.
   */
  public Set<String> wildcard;

  /**
   * @field required
   * @brief The required provisions of the queue.
   * @details The required provisions to allow workers and operations to be added to the queue.
   *     These often match the remote api's command platform properties.
   */
  public Set<Map.Entry<String, String>> required;
}
