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

package build.buildfarm.common;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * @class SystemProcessorsTest
 * @brief Tests utility functions for deriving the system's total logical processors.
 */
@RunWith(JUnit4.class)
public class SystemProcessorsTest {
  // Function under test: get
  // Reason for testing: functions do not throw and return nonzero results.
  // Failure explanation: The implementation can not correctly derive.
  @Test
  public void getCheckImplValuesAreNotZero() {
    // ASSERT
    assertThat(SystemProcessors.get(SystemProcessors.PROCESSOR_DERIVE.JAVA_RUNTIME))
        .isGreaterThan(0);
    assertThat(SystemProcessors.get(SystemProcessors.PROCESSOR_DERIVE.OSHI)).isGreaterThan(0);
  }

  // Function under test: get
  // Reason for testing: functions chooses highest value.
  // Failure explanation: The implementation does not choose highest value.
  @Test
  public void getCheckBestValueIsChosen() {
    // ASSERT
    assertThat(SystemProcessors.get())
        .isAtLeast(SystemProcessors.get(SystemProcessors.PROCESSOR_DERIVE.JAVA_RUNTIME));
    assertThat(SystemProcessors.get())
        .isAtLeast(SystemProcessors.get(SystemProcessors.PROCESSOR_DERIVE.OSHI));
  }
}
