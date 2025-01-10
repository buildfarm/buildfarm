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

package build.buildfarm.worker;

import com.google.protobuf.Duration;

/**
 * @class TimeoutSettings
 * @brief Settings used to determine the enforced timeout on the action.
 * @details The REAPI action usually provides its own timeout to be used. Buildfarm must decide if
 *     this timeout is appropriate and adjust the timeout as needed based on configuration.
 */
public class TimeoutSettings {
  /**
   * @field defaultTimeout
   * @brief A default timeout to use if the action did not provide their own.
   * @details This is used if the client does not give a timeout for the action.
   */
  public Duration defaultTimeout;

  /**
   * @field maxTimeout
   * @brief The maximum allowed timeout to be applied to the action.
   * @details And action's timeout is capped to this limit to ensure actions don't run too long.
   */
  public Duration maxTimeout;

  /**
   * @field applyTimeoutPadding
   * @brief Whether or not to adjust the selected timeout with buildfarm specific padding.
   * @details For example, an additional padding time may be added to guarantee resource cleanup
   *     around the action's execution.
   */
  public boolean applyTimeoutPadding = true;

  /**
   * @field timeoutPaddingSeconds
   * @brief The amount of padding to apply to the timeout.
   * @details Only applied if timeout padding is enabled.
   */
  public long timeoutPaddingSeconds = 10;
}
