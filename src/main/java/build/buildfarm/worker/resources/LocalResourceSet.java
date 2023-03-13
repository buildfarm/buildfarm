// Copyright 2023 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker.resources;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * @class Local Resource Set
 * @brief A fixed amount of a specific resource.
 * @details We define limited resources as a counting semaphores whose configuration contains a name
 *     and a count representing a physical or logical group of units obtained by executors as a
 *     precondition to fulfill a long running operation. These units are released upon the
 *     operation's completion. The resource is requested by the action's platform properties. These
 *     resources are specific to the individual worker.
 */
public class LocalResourceSet {
  /**
   * @field resources
   * @brief A set containing resource semaphores organized by name.
   * @details Key is name, and value is current usage amount.
   */
  public Map<String, Semaphore> resources = new HashMap<>();
}
