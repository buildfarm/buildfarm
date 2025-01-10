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

/**
 * @class TestSizeResourceOverrides
 * @brief Contains configurations to override resource usage based on test sizes.
 * @details Users may have certain resource expectations based on the test size. Instead of
 *     overloading users with various exec_properties for each resource, they could instead rely on
 *     the test size buckets which will assign good defaults. This is an optional core setting
 *     strategy and it can be enabled / disabled.
 */
@SuppressWarnings("PMD.TestClassWithoutTestCases")
public class TestSizeResourceOverrides {
  /**
   * @field enabled
   * @brief Whether to perform resource overrides based on test size.
   * @details If false, this data is not considered and resources won't be changed based on test
   *     size.
   */
  public boolean enabled = false;

  /**
   * @field small
   * @brief Resource overrides for small tests.
   * @details Applied to test actions.
   */
  public TestSizeResourceOverride small = new TestSizeResourceOverride();

  /**
   * @field medium
   * @brief Resource overrides for medium tests.
   * @details Applied to test actions.
   */
  public TestSizeResourceOverride medium = new TestSizeResourceOverride();

  /**
   * @field large
   * @brief Resource overrides for large tests.
   * @details Applied to test actions.
   */
  public TestSizeResourceOverride large = new TestSizeResourceOverride();

  /**
   * @field enormous
   * @brief Resource overrides for enormous tests.
   * @details Applied to test actions.
   */
  public TestSizeResourceOverride enormous = new TestSizeResourceOverride();

  /**
   * @field unknown
   * @brief Resource overrides for unknown test sizes.
   * @details Applied to test actions.
   */
  public TestSizeResourceOverride unknown = new TestSizeResourceOverride();
}
