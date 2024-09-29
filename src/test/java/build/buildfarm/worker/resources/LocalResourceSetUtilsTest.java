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

import static com.google.common.truth.Truth.assertThat;

import build.bazel.remote.execution.v2.Platform;
import java.util.concurrent.Semaphore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * @class LocalResourceSetUtilsTest
 * @brief Tests how local resources are claimed and released.
 * @details Shows behaviour of local resource claims and releases.
 */
@RunWith(JUnit4.class)
public class LocalResourceSetUtilsTest {
  // Function under test: releaseClaims
  // Reason for testing: Show its okay to return claims that were never taken.
  // Failure explanation: can't return claims that were never taken.
  @Test
  public void multipleClaimReleaseHasNoEffect() throws Exception {
    // ARRANGE
    LocalResourceSet resourceSet = new LocalResourceSet();
    Semaphore foo = new Semaphore(2);
    foo.acquire(); // create room to put two resources back on if malfunctioning
    resourceSet.resources.put("FOO", foo);

    Platform platform =
        Platform.newBuilder()
            .addProperties(Platform.Property.newBuilder().setName("resource:FOO").setValue("1"))
            .build();

    // ACT
    Claim claim = LocalResourceSetUtils.claimResources(platform, resourceSet);
    assertThat(foo.availablePermits()).isEqualTo(0);
    claim.release();
    assertThat(foo.availablePermits()).isEqualTo(1);
    claim.release();
    assertThat(foo.availablePermits()).isEqualTo(1);
  }
}
