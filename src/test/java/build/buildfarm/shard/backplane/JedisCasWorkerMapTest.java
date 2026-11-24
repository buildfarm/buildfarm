// Copyright 2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.shard.backplane;

import static com.google.common.truth.Truth.assertThat;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.v1test.Digest;
import com.github.fppt.jedismock.RedisServer;
import com.github.fppt.jedismock.server.ServiceOptions;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

@RunWith(JUnit4.class)
public class JedisCasWorkerMapTest {
  private static final String CAS_PREFIX = "ContentAddressableStorage";

  private RedisServer redisServer;
  private JedisCluster jedis;
  private JedisCasWorkerMap jedisCasWorkerMap;

  @Before
  public void setup() throws IOException {
    redisServer =
        RedisServer.newRedisServer(0, InetAddress.getByName("localhost"))
            .setOptions(ServiceOptions.defaultOptions().withClusterModeEnabled())
            .start();
    jedis =
        new JedisCluster(
            Collections.singleton(
                new HostAndPort(redisServer.getHost(), redisServer.getBindPort())),
            DefaultJedisClientConfig.builder().serverDefaultProtocol().build());
    jedisCasWorkerMap = new JedisCasWorkerMap(jedis, CAS_PREFIX, 60);
  }

  @Test
  public void testSetExpire() throws IOException {
    Digest testDigest1 = Digest.newBuilder().setHash("abc").build();
    Digest testDigest2 = Digest.newBuilder().setHash("xyz").build();

    String casKey1 = key(testDigest1);
    String casKey2 = key(testDigest2);

    jedis.sadd(casKey1, "worker1");
    jedisCasWorkerMap.setExpire(Arrays.asList(testDigest1, testDigest2));

    assertThat(jedis.ttl(casKey1)).isGreaterThan(0);
    assertThat(jedis.ttl(casKey2)).isEqualTo(-2);
  }

  @Test
  public void testGetResetsTtl() {
    Digest digest = digest(0);
    String redisKey = key(digest);
    jedis.sadd(redisKey, "worker");
    assertThat(jedisCasWorkerMap.get(digest)).containsExactly("worker");
    assertThat(jedis.ttl(redisKey)).isGreaterThan(0);
    assertConnectionsReturned();
  }

  @Test
  public void testGetAnyResetsTtl() {
    Digest digest = digest(0);
    String redisKey = key(digest);
    jedis.sadd(redisKey, "worker");
    jedisCasWorkerMap.getAny(digest);
    assertThat(jedis.ttl(redisKey)).isGreaterThan(0);
    assertConnectionsReturned();
  }

  @Test
  public void testAdjustResetsTtl() {
    Digest digest = digest(0);
    String redisKey = key(digest);
    jedisCasWorkerMap.add(digest, "original");
    assertThat(jedis.ttl(redisKey)).isGreaterThan(0);
    jedis.persist(redisKey);
    jedisCasWorkerMap.adjust(digest, Set.of("added", "overlap"), Set.of("original", "overlap"));
    assertThat(jedis.smembers(redisKey)).containsExactly("added");
    assertThat(jedis.ttl(redisKey)).isGreaterThan(0);
    jedis.persist(redisKey);
    jedisCasWorkerMap.adjust(digest, Set.of(), Set.of());
    assertThat(jedis.ttl(redisKey)).isGreaterThan(0);
    assertThat(jedis.smembers(redisKey)).containsExactly("added");
    assertConnectionsReturned();
  }

  @Test
  public void testAddAllSetsTtlOnNewKeys() {
    List<Digest> digests = List.of(digest(0), digest(1));
    jedisCasWorkerMap.addAll(digests, "worker");
    for (Digest digest : digests) {
      assertThat(jedis.smembers(key(digest))).containsExactly("worker");
      assertThat(jedis.ttl(key(digest))).isGreaterThan(0);
    }
    assertConnectionsReturned();
  }

  private void assertConnectionsReturned() {
    jedis.getClusterNodes().values().forEach(pool -> assertThat(pool.getNumActive()).isEqualTo(0));
  }

  private static Digest digest(int index) {
    return Digest.newBuilder().setHash("blob" + index).build();
  }

  private static String key(Digest digest) {
    return CAS_PREFIX + ":" + DigestUtil.toString(digest);
  }

  @After
  public void tearDown() throws IOException {
    jedis.close();
    redisServer.stop();
  }
}
