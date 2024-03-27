// Copyright 2020 The Bazel Authors. All rights reserved.
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

package build.buildfarm.common.redis;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * @class RedisHashtagsTest
 * @brief tests String utilities when dealing with key names that involve hashtags.
 * @details Simple parsers for extracting out / adding hashtags to redis keys.
 */
@RunWith(JUnit4.class)
public class RedisHashtagsTest {
  // Function under test: hashedName
  // Reason for testing: a hashtag can be added to a name to get a hashed key name
  // Failure explanation: the format is not as expected when adding a hashtag
  @Test
  public void hashedNameCanRetrieveDistributedHashes() throws Exception {
    // ASSERT
    assertThat(RedisHashtags.hashedName("x", "y")).isEqualTo("{y}x");
    assertThat(RedisHashtags.hashedName("QueuedOperations", "Execution"))
        .isEqualTo("{Execution}QueuedOperations");
  }

  // Function under test: unhashedName
  // Reason for testing: the hashtag can be removed from the name
  // Failure explanation: the format is not as expected when removing a hashtag
  @Test
  public void unhashedNameGetTheUnhashedName() throws Exception {
    // ASSERT
    assertThat(RedisHashtags.unhashedName("{y}x")).isEqualTo("x");
    assertThat(RedisHashtags.unhashedName("x{y}")).isEqualTo("x");
    assertThat(RedisHashtags.unhashedName("x{y}x")).isEqualTo("xx");
    assertThat(RedisHashtags.unhashedName("{Execution}QueuedOperations"))
        .isEqualTo("QueuedOperations");
  }

  // Function under test: existingHash
  // Reason for testing: the hashtag can be extracted from the full key name
  // Failure explanation: the format is not as expected when extracting the hashtag
  @Test
  public void existingHashGetTheExistingHashName() throws Exception {
    // ASSERT
    assertThat(RedisHashtags.existingHash("{y}x")).isEqualTo("y");
    assertThat(RedisHashtags.existingHash("x{y}")).isEqualTo("y");
    assertThat(RedisHashtags.existingHash("x{y}x")).isEqualTo("y");
    assertThat(RedisHashtags.existingHash("{Execution}QueuedOperations")).isEqualTo("Execution");
  }

  // Function under test: hashedName, unhashedName
  // Reason for testing: the hashtag can be empty
  // Failure explanation: the format is not as expected when extracting the hashtag
  @Test
  public void emptyTestCases() throws Exception {
    // ASSERT
    assertThat(RedisHashtags.hashedName("", "")).isEqualTo("");
    assertThat(RedisHashtags.hashedName("x", "")).isEqualTo("x");
    assertThat(RedisHashtags.hashedName("x", null)).isEqualTo("x");

    assertThat(RedisHashtags.unhashedName("{}x")).isEqualTo("x");
    assertThat(RedisHashtags.unhashedName("z{}")).isEqualTo("z");
    assertThat(RedisHashtags.unhashedName("")).isEqualTo("");
  }

  // Function under test: hashedName, unhashedName
  // Reason for testing: the hashtag can be null or empty
  @Test
  public void testNulls() throws Exception {
    // ASSERT
    assertThrows(NullPointerException.class, () -> RedisHashtags.hashedName(null, null));
    assertThrows(NullPointerException.class, () -> RedisHashtags.unhashedName(null));
  }
}
