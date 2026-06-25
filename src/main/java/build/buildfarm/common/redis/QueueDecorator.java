package build.buildfarm.common.redis;

import build.buildfarm.common.Queue;
import redis.clients.jedis.UnifiedJedis;

public interface QueueDecorator<E> {
  Queue<E> decorate(UnifiedJedis jedis, String name);
}
