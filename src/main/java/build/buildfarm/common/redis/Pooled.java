package build.buildfarm.common.redis;

import java.util.concurrent.Executor;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Connection;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.PipelineBase;

public class Pooled extends JedisPooled implements Unified {
  public Pooled(
      GenericObjectPoolConfig<Connection> poolConfig,
      HostAndPort clusterNode,
      JedisClientConfig clientConfig) {
    super(poolConfig, clusterNode, clientConfig);
  }

  @Override
  public PipelineBase pipelined(Executor executor) {
    return super.pipelined();
  }
}
