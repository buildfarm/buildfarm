package build.buildfarm.common.redis;

import java.util.concurrent.Executor;
import redis.clients.jedis.AbstractPipeline;

// must also imply parent of UnifiedJedis
public interface Unified {
  AbstractPipeline pipelined(Executor executor);
}
