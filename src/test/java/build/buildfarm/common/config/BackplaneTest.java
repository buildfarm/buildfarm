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

package build.buildfarm.common.config;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * @class BackplaneTest
 * @brief Tests utility functions for Backplane configuration overrides
 */
@RunWith(JUnit4.class)
public class BackplaneTest {
  @Before
  public void assertNoEnvVariable() {
    // If a REDIS_PASSWORD env variable is set, it wins. We're not mocking env vars.
    assertThat(System.getenv("REDIS_PASSWORD")).isNull();
  }

  @Test
  public void testRedisPasswordFromUri() {
    Backplane b = new Backplane();
    String testRedisUri = "redis://user:pass1@redisHost.redisDomain";
    b.setRedisUri(testRedisUri);
    assertThat(b.getRedisPassword()).isEqualTo("pass1");
  }

  /**
   * Validate that the redis URI password is higher priority than the `redisPassword` in the config
   */
  @Test
  public void testRedisPasswordPriorities() {
    Backplane b = new Backplane();
    b.setRedisUri("redis://user:pass1@redisHost.redisDomain");
    b.setRedisPassword("pass2");
    assertThat(b.getRedisPassword()).isEqualTo("pass1");
  }
}
