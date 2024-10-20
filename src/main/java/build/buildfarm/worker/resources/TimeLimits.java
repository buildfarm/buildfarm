// Copyright 2021 The Buildfarm Authors. All rights reserved.
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

import java.util.ArrayList;

/**
 * @class TimeLimits
 * @brief Time resource limitations imposed on specific actions.
 * @details These resource limitations are often specified by the client (via: exec_properties), but
 *     ultimately validated and decided by the server. All actions should be restricted to a max
 *     timeout. Other properties are available to affect time from the perspective of the action.
 */
public class TimeLimits {
  /**
   * @field skipSleep
   * @brief Avoid any delay when calling time related functions.
   * @details This affects the following syscalls: nanosleep, clock_nanosleep, select, poll,
   *     gettimeofday, clock_gettime, time. This useful for finding artificially slow actions.
   */
  public boolean skipSleep = false;

  /**
   * @field timeShift
   * @brief Start the action after a specified amount of shifted time
   * @details This is useful for seeing how an action will behave in the future.
   */
  public int timeShift = 0;

  /**
   * @field description
   * @brief Description explaining why settings were chosen.
   * @details This can be used to debug execution behavior.
   */
  public final ArrayList<String> description = new ArrayList<>();
}
