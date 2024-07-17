// Copyright 2021 The Bazel Authors. All rights reserved.
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

/**
 * @class TestSizeResourceOverride
 * @brief Contains configuration to override resource usage based on a particular test size.
 * @details Users may have certain resource expectations based on the test size. Instead of
 *     overloading users with various exec_properties for each resource, they could instead rely on
 *     the test size buckets which will assign good defaults.
 */
@SuppressWarnings("PMD.TestClassWithoutTestCases")
public class TestSizeResourceOverride {
  /**
   * @field coreMin
   * @brief What to override the minimum core amount to.
   * @details Override will take precedence over other settings.
   */
  public int coreMin = 1;

  /**
   * @field coreMax
   * @brief What to override the maximum core amount to.
   * @details Override will take precedence over other settings.
   */
  public int coreMax = 1;
}
