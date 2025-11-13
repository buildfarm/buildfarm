package build.buildfarm.common.redis;

import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

// maybe RedisMap deserves a generic for type safety
public class RedisSetMap extends RedisMap {
  // some funkiness
  // parent should really be a 'cluster scannable'
  // expiration_s from protected is not great
  private final boolean expireOnEach;

  public RedisSetMap(String name, int timeout_s, boolean expireOnEach) {
    super(name, new IdentityTranslator(), timeout_s);
    this.expireOnEach = expireOnEach;
  }

  public void add(UnifiedJedis jedis, String key, String value) {
    if (key.isEmpty()) {
      return;
    }

    String keyName = createKeyName(key);
    if (jedis.sadd(keyName, value) == 1 || expireOnEach) {
      jedis.expire(keyName, expiration_s);
    }
  }

  public ScanResult<String> scan(UnifiedJedis jedis, String key, String setCursor, int count) {
    String keyName = createKeyName(key);
    OffsetScanner<String> offsetScanner =
        new OffsetScanner<String>() {
          @Override
          protected ScanResult<String> scan(String cursor, int remaining) {
            return jedis.sscan(keyName, cursor, new ScanParams().count(remaining));
          }
        };
    return offsetScanner.fill(setCursor, count);
  }
}
