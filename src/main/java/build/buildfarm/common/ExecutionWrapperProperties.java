// Copyright 2022 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @class ExecutionWrapperProperties
 * @brief The correlation of buildfarm features to underlying execution wrappers.
 * @details Buildfarm has the ability to execute actions differently through execution wrappers.
 *     These wrappers could be chosen dynamically from the client or configured via buildfarm. This
 *     data shows the dependency of a feature to the underlying wrapper. It can be used to verify
 *     whether a wrapper is needed for the particular feature. The original intention of this data
 *     is to verify the existence of wrappers on startup to warn about unavailable features.
 */
public class ExecutionWrapperProperties {
  /**
   * @field mapping
   * @brief A mapping from execution wrappers to features they enable.
   * @details If an execution wrapper is missing the corresponding features would not be possible.
   */
  public final Map<ArrayList<String>, ArrayList<String>> mapping = new HashMap<>();
}
