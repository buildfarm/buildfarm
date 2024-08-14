package build.buildfarm.common.redis;

import static com.google.common.truth.Truth.assertThat;

import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.instance.shard.JedisClusterFactory;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import redis.clients.jedis.UnifiedJedis;

@RunWith(JUnit4.class)
public class RedisSortedSetTest {
  private BuildfarmConfigs configs = BuildfarmConfigs.getInstance();
  private UnifiedJedis redis;
  private RedisSortedSet redisSortedSet;

  @Before
  public void setUp() throws Exception {
    this.configs.getBackplane().setRedisUri("redis://localhost:6379");
    this.redis = JedisClusterFactory.createTest();
    this.redisSortedSet = new RedisSortedSet("test_sorted_set");
  }

  @After
  public void tearDown() throws IOException {
    this.redis.close();
  }

  @Test
  public void testIncrementMembersScore() {
    Map<String, Integer> membersScoreInput =
        Map.of("key1", 100, "key2", 50, "key3", 75, "key4", 25, "key5", 85);

    Map<String, Integer> membersScoreResponse =
        redisSortedSet.incrementMembersScore(redis, membersScoreInput);

    membersScoreResponse.forEach(
        (member, score) -> assertThat(membersScoreInput.get(member)).isEqualTo(score));
    membersScoreResponse = redisSortedSet.incrementMembersScore(redis, membersScoreInput);
    membersScoreResponse.forEach(
        (member, score) -> assertThat(2 * membersScoreInput.get(member)).isEqualTo(score));
  }

  @Test
  public void testRemoveMembers() {
    Map<String, Integer> membersToAdd =
        Map.of("key1", 100, "key2", 50, "key3", 75, "key4", 25, "key5", 85);

    Iterable<String> membersToRemove = Arrays.asList("key1", "key2", "key7");

    redisSortedSet.incrementMembersScore(redis, membersToAdd);

    int membersRemoved = redisSortedSet.removeMembers(redis, membersToRemove);

    assertThat(membersRemoved).isEqualTo(2);
  }
}
