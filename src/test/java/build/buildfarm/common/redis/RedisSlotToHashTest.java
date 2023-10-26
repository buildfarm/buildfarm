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
import static redis.clients.jedis.Protocol.CLUSTER_HASHSLOTS;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import redis.clients.jedis.util.JedisClusterCRC16;

/**
 * @class RedisSlotToHashTest
 * @brief tests Get a redis hash tag for the provided slot number.
 * @details Sometimes in redis you want to hash a particular key to a particular node in the redis
 *     cluster. You might decide to do this in order to distribute data evenly across nodes and
 *     balance cpu utilization. Since redis nodes are decided based on the slot number that a value
 *     hashes to, you can force. A key to land on a particular node by choosing a redis hashtag
 *     which you know corresponds to the appropriate slot number. The class is used to convert a
 *     slot number to a corresponding string (which when hashed by redis's crc16 algorithm will
 *     return that slot number.
 */
@RunWith(JUnit4.class)
public class RedisSlotToHashTest {
  // Function under test: correlate
  // Reason for testing: correlating every slot number to a string hashes back to the correct slot
  // Failure explanation: either the underlying lookup table is wrong or the algorithm that converts
  // slot numbers to strings is incorrect
  @Test
  public void correlateCorrectForEverySlot() throws Exception {
    for (int i = 0; i < CLUSTER_HASHSLOTS; ++i) {
      // convert to hashtag
      String hashtag = RedisSlotToHash.correlate(i);

      // convert hashtag back to slot
      int slotNumber = JedisClusterCRC16.getSlot(hashtag);

      // check correct correlation
      assertThat(i).isEqualTo(slotNumber);
    }
  }

  // Function under test: correlate
  // Reason for testing: ensure the object can be constructed
  // Failure explanation: the object cannot be constructed
  @Test
  public void correlateEnsureConstruction() throws Exception {
    new RedisSlotToHash();
  }

  // Function under test: correlateRange
  // Reason for testing: given a slot range a correct hashtag is dynamically derived
  // Failure explanation: the hashtag does not correlate back to the slot number
  @Test
  public void correlateRangeCorrectHashtagFoundForSlotRange() throws Exception {
    // convert to hashtag
    String hashtag = RedisSlotToHash.correlateRange(100, 200);

    // convert hashtag back to slot
    int slotNumber = JedisClusterCRC16.getSlot(hashtag);

    // check correct correlation
    assertThat(slotNumber >= 100 && slotNumber <= 200).isTrue();
  }

  // Function under test: correlateRangeWithPrefix
  // Reason for testing: given a slot range a correct hashtag is dynamically derived
  // Failure explanation: the hashtag does not correlate back to the slot number
  @Test
  public void correlateRangeWithPrefixCorrectHashtagFoundForSlotRange() throws Exception {
    // convert to hashtag
    String hashtag =
        RedisSlotToHash.correlateRangesWithPrefix(
            ImmutableList.of(ImmutableList.of(100l, 200l)), "Execution");

    // convert hashtag back to slot
    int slotNumber = JedisClusterCRC16.getSlot(hashtag);

    // check correct correlation
    assertThat(slotNumber >= 100 && slotNumber <= 200).isTrue();

    // check correct hashtag prefix
    assertThat(hashtag.startsWith("Execution:")).isTrue();
  }
}
