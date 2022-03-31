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

package build.buildfarm.common;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * @class StringUtilsTest
 * @brief tests utility functions for working with Java Strings.
 */
@RunWith(JUnit4.class)
public class StringUtilsTest {
  // Function under test: removePrefix
  // Reason for testing: no prefix to remove.
  // Failure explanation: string should not have changed.
  @Test
  public void removePrefixNoRemove() throws Exception {
    // ARRANGE
    String str = "hello world";

    // ACT
    String newStr = StringUtils.removePrefix(str, "world");

    // ASSERT
    assertThat(newStr).isEqualTo("hello world");
  }

  // Function under test: removePrefix
  // Reason for testing: prefix removed
  // Failure explanation: string should have its prefix removed.
  @Test
  public void removePrefixSimpleExample() throws Exception {
    // ARRANGE
    String str = "hello world";

    // ACT
    String newStr = StringUtils.removePrefix(str, "hello ");

    // ASSERT
    assertThat(str).isEqualTo("hello world");
    assertThat(newStr).isEqualTo("world");
  }
}
