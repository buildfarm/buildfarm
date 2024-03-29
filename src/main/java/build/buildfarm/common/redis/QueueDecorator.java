package build.buildfarm.common.redis;

import build.buildfarm.common.Queue;
import redis.clients.jedis.Jedis;

public interface QueueDecorator<E> {
  Queue<E> decorate(Jedis jedis, String name);
}
