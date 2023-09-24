package build.buildfarm.instance.shard;

import static com.google.common.truth.Truth.assertThat;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.redis.RedisClient;
import com.github.fppt.jedismock.RedisServer;
import com.github.fppt.jedismock.server.ServiceOptions;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

@RunWith(JUnit4.class)
public class JedisCasWorkerMapTest {

  private final String casPrefix = "ContentAddressableStorage";

  private RedisServer redisServer;
  private RedisClient redisClient;
  private JedisCasWorkerMap jedisCasWorkerMap;

  @Before
  public void setup() throws IOException {
    redisServer =
        RedisServer.newRedisServer()
            .setOptions(ServiceOptions.defaultOptions().withClusterModeEnabled())
            .start();
    Set<HostAndPort> x = new HashSet<>();
    x.add(new HostAndPort(redisServer.getHost(), redisServer.getBindPort()));
    redisClient = new RedisClient(new JedisCluster(x));
    jedisCasWorkerMap = new JedisCasWorkerMap(casPrefix, 60);
  }

  @Test
  public void testSetExpire() throws IOException {
    Digest testDigest1 = Digest.newBuilder().setHash("abc").build();
    Digest testDigest2 = Digest.newBuilder().setHash("xyz").build();

    String casKey1 = casPrefix + ":" + DigestUtil.toString(testDigest1);
    String casKey2 = casPrefix + ":" + DigestUtil.toString(testDigest2);

    redisClient.run(jedis -> jedis.sadd(casKey1, "worker1"));
    jedisCasWorkerMap.setExpire(redisClient, Arrays.asList(testDigest1, testDigest2));

    assertThat((Long) redisClient.call(jedis -> jedis.ttl(casKey1))).isGreaterThan(0L);
    assertThat((Long) redisClient.call(jedis -> jedis.ttl(casKey2))).isEqualTo(-2L);
  }

  @After
  public void tearDown() throws IOException {
    redisServer.stop();
  }
}
