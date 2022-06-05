// Copyright 2022 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker.shard;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.ExecutedActionMetadata;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.redis.RedisClient;
import build.buildfarm.instance.shard.JedisClusterFactory;
import build.buildfarm.instance.shard.ShardActionCache;
import com.google.common.truth.Truth;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import java.util.concurrent.Executors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ShardActionCacheTest {
  // Function under test: ShardActionCach
  // Reason for testing: Test that the action cache can be constructed without issue.
  // Failure explanation: This will catch any issues with initial setup of object.
  @Test
  public void TestConstruction() throws Exception {
    // ARRANGE
    RedisClient client = new RedisClient(JedisClusterFactory.createTest());
    new ShardActionCache(
        client,
        "action-cache",
        10000,
        10000,
        MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(24)));
  }

  // Function under test: put
  // Reason for testing: Test that action cache results can be put into the cache.
  // Failure explanation: Catch any failures putting cache results.
  @Test
  public void PutsSucceed() throws Exception {
    // ARRANGE
    RedisClient client = new RedisClient(JedisClusterFactory.createTest());
    ShardActionCache cache =
        new ShardActionCache(
            client,
            "action-cache",
            10000,
            10000,
            MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(24)));

    // Add an element
    {
      // ACT
      DigestUtil digestUtil = DigestUtil.forHash("SHA256");
      ActionKey actionKey =
          digestUtil.asActionKey(digestUtil.compute(ByteString.copyFromUtf8("foo1")));
      ActionResult actionResult = ActionResult.newBuilder().build();
      cache.put(client, actionKey, actionResult);

      // ASSERT
      int size = client.call(jedis -> cache.size(jedis));
      Truth.assertThat(size).isEqualTo(1);
    }

    // Add an element
    {
      // ACT
      DigestUtil digestUtil = DigestUtil.forHash("SHA256");
      ActionKey actionKey =
          digestUtil.asActionKey(digestUtil.compute(ByteString.copyFromUtf8("foo2")));
      ActionResult actionResult = ActionResult.newBuilder().build();
      cache.put(client, actionKey, actionResult);

      // ASSERT
      int size = client.call(jedis -> cache.size(jedis));
      Truth.assertThat(size).isEqualTo(2);
    }

    // Add an element
    {
      // ACT
      DigestUtil digestUtil = DigestUtil.forHash("SHA256");
      ActionKey actionKey =
          digestUtil.asActionKey(digestUtil.compute(ByteString.copyFromUtf8("foo3")));
      ActionResult actionResult = ActionResult.newBuilder().build();
      cache.put(client, actionKey, actionResult);

      // ASSERT
      int size = client.call(jedis -> cache.size(jedis));
      Truth.assertThat(size).isEqualTo(3);
    }
  }

  // Function under test: put
  // Reason for testing: Test that duplicate action cache results are not put into the cache.
  // Failure explanation: Catch any failures putting duplicate cache results.
  @Test
  public void PutsAvoidDuplicate() throws Exception {
    // ARRANGE
    RedisClient client = new RedisClient(JedisClusterFactory.createTest());
    ShardActionCache cache =
        new ShardActionCache(
            client,
            "action-cache",
            10000,
            10000,
            MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(24)));

    // Add an element
    {
      // ACT
      DigestUtil digestUtil = DigestUtil.forHash("SHA256");
      ActionKey actionKey =
          digestUtil.asActionKey(digestUtil.compute(ByteString.copyFromUtf8("foo")));
      ActionResult actionResult = ActionResult.newBuilder().build();
      cache.put(client, actionKey, actionResult);

      // ASSERT
      int size = client.call(jedis -> cache.size(jedis));
      Truth.assertThat(size).isEqualTo(1);
    }

    // Add a duplicate element
    {
      // ACT
      DigestUtil digestUtil = DigestUtil.forHash("SHA256");
      ActionKey actionKey =
          digestUtil.asActionKey(digestUtil.compute(ByteString.copyFromUtf8("foo")));
      ActionResult actionResult = ActionResult.newBuilder().build();
      cache.put(client, actionKey, actionResult);

      // ASSERT
      int size = client.call(jedis -> cache.size(jedis));
      Truth.assertThat(size).isEqualTo(1);
    }
  }

  // Function under test: put / get
  // Reason for testing: Test that put actions can be found
  // Failure explanation: Unable to get a saved action result
  @Test
  public void PutsCanGet() throws Exception {
    // ARRANGE
    RedisClient client = new RedisClient(JedisClusterFactory.createTest());
    ShardActionCache cache =
        new ShardActionCache(
            client,
            "action-cache",
            10000,
            10000,
            MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(24)));

    // ACT
    DigestUtil digestUtil = DigestUtil.forHash("SHA256");
    ActionKey actionKey =
        digestUtil.asActionKey(digestUtil.compute(ByteString.copyFromUtf8("foo")));
    ActionResult actionResult =
        ActionResult.newBuilder()
            .setExecutionMetadata(ExecutedActionMetadata.newBuilder().setWorker("worker1").build())
            .build();
    cache.put(client, actionKey, actionResult);

    // ASSERT
    ActionResult fetched = cache.get(actionKey).get();
    Truth.assertThat(fetched).isEqualTo(actionResult);
  }

  // Function under test: get
  // Reason for testing: Missing item returns null
  // Failure explanation: Did not get null as expected
  @Test
  public void MissingGetIsNull() throws Exception {
    // ARRANGE
    RedisClient client = new RedisClient(JedisClusterFactory.createTest());
    ShardActionCache cache =
        new ShardActionCache(
            client,
            "action-cache",
            10000,
            10000,
            MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(24)));

    // ACT
    DigestUtil digestUtil = DigestUtil.forHash("SHA256");
    ActionKey actionKey =
        digestUtil.asActionKey(digestUtil.compute(ByteString.copyFromUtf8("foo")));
    ActionResult fetched = cache.get(actionKey).get();

    // ASSERT
    Truth.assertThat(fetched).isEqualTo(null);
  }

  // Function under test: remove
  // Reason for testing: Key is correctly removed.
  // Failure explanation: Key is not removed as expected
  @Test
  public void ItemRemoved() throws Exception {
    // ARRANGE
    RedisClient client = new RedisClient(JedisClusterFactory.createTest());
    ShardActionCache cache =
        new ShardActionCache(
            client,
            "action-cache",
            10000,
            10000,
            MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(24)));

    DigestUtil digestUtil = DigestUtil.forHash("SHA256");
    ActionKey actionKey =
        digestUtil.asActionKey(digestUtil.compute(ByteString.copyFromUtf8("foo")));
    ActionResult actionResult =
        ActionResult.newBuilder()
            .setExecutionMetadata(ExecutedActionMetadata.newBuilder().setWorker("worker1").build())
            .build();
    cache.put(client, actionKey, actionResult);

    // ACT
    client.run(jedis -> cache.remove(jedis, actionKey));

    // ASSERT
    Truth.assertThat(cache.get(actionKey).get()).isEqualTo(null);
    int size = client.call(jedis -> cache.size(jedis));
    Truth.assertThat(size).isEqualTo(0);
  }

  // Function under test: removeL2
  // Reason for testing: Items are fetched from L1.
  // Failure explanation: Cache not working as expected
  @Test
  public void L1CacheCanGet() throws Exception {
    // ARRANGE
    RedisClient client = new RedisClient(JedisClusterFactory.createTest());
    ShardActionCache cache =
        new ShardActionCache(
            client,
            "action-cache",
            10000,
            10000,
            MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(24)));

    DigestUtil digestUtil = DigestUtil.forHash("SHA256");
    ActionKey actionKey =
        digestUtil.asActionKey(digestUtil.compute(ByteString.copyFromUtf8("foo")));
    ActionResult actionResult =
        ActionResult.newBuilder()
            .setExecutionMetadata(ExecutedActionMetadata.newBuilder().setWorker("worker1").build())
            .build();
    cache.put(client, actionKey, actionResult);

    // ACT
    client.run(jedis -> cache.removeL2(jedis, actionKey));

    // ASSERT
    Truth.assertThat(cache.get(actionKey).get()).isEqualTo(actionResult);
  }

  // Function under test: removeL1
  // Reason for testing: Items are fetched from L2.
  // Failure explanation: Cache not working as expected
  @Test
  public void L2CacheCanGet() throws Exception {
    // ARRANGE
    RedisClient client = new RedisClient(JedisClusterFactory.createTest());
    ShardActionCache cache =
        new ShardActionCache(
            client,
            "action-cache",
            10000,
            10000,
            MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(24)));

    DigestUtil digestUtil = DigestUtil.forHash("SHA256");
    ActionKey actionKey =
        digestUtil.asActionKey(digestUtil.compute(ByteString.copyFromUtf8("foo")));
    ActionResult actionResult =
        ActionResult.newBuilder()
            .setExecutionMetadata(ExecutedActionMetadata.newBuilder().setWorker("worker1").build())
            .build();
    cache.put(client, actionKey, actionResult);

    // ACT
    cache.removeL1(actionKey);

    // ASSERT
    Truth.assertThat(cache.get(actionKey).get()).isEqualTo(actionResult);
  }

  // Function under test: putL2
  // Reason for testing: Items fetched from L2 are saved in L1.
  // Failure explanation: Item cannot be fetched from L1 as expected.
  @Test
  public void L2PopulatesL1() throws Exception {
    // ARRANGE
    RedisClient client = new RedisClient(JedisClusterFactory.createTest());
    ShardActionCache cache =
        new ShardActionCache(
            client,
            "action-cache",
            10000,
            10000,
            MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(24)));

    // ACT
    DigestUtil digestUtil = DigestUtil.forHash("SHA256");
    ActionKey actionKey =
        digestUtil.asActionKey(digestUtil.compute(ByteString.copyFromUtf8("foo")));
    ActionResult actionResult =
        ActionResult.newBuilder()
            .setExecutionMetadata(ExecutedActionMetadata.newBuilder().setWorker("worker1").build())
            .build();
    cache.putL2(client, actionKey, actionResult);
    cache.get(actionKey).get();
    client.run(jedis -> cache.removeL2(jedis, actionKey));

    // ASSERT
    Truth.assertThat(cache.get(actionKey).get()).isEqualTo(actionResult);
  }

  // Function under test: clear
  // Reason for testing: All items can be removed from the cache.
  // Failure explanation: Clear functionality not working.
  @Test
  public void Clear() throws Exception {
    // ARRANGE
    RedisClient client = new RedisClient(JedisClusterFactory.createTest());
    ShardActionCache cache =
        new ShardActionCache(
            client,
            "action-cache",
            10000,
            10000,
            MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(24)));

    // ACT
    DigestUtil digestUtil = DigestUtil.forHash("SHA256");
    ActionKey actionKey =
        digestUtil.asActionKey(digestUtil.compute(ByteString.copyFromUtf8("foo")));
    ActionResult actionResult =
        ActionResult.newBuilder()
            .setExecutionMetadata(ExecutedActionMetadata.newBuilder().setWorker("worker1").build())
            .build();
    cache.put(client, actionKey, actionResult);
    client.run(jedis -> cache.clear(jedis));

    // ASSERT
    Truth.assertThat(cache.get(actionKey).get()).isEqualTo(null);
    int size = client.call(jedis -> cache.size(jedis));
    Truth.assertThat(size).isEqualTo(0);
  }
}
