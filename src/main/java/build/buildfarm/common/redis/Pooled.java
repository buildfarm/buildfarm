package build.buildfarm.common.redis;

import java.time.Duration;
import java.util.concurrent.Executor;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.Connection;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.PipelineBase;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.providers.PooledConnectionProvider;

public class Pooled extends UnifiedJedis implements Unified {
  public Pooled(
      GenericObjectPoolConfig<Connection> poolConfig,
      HostAndPort hostAndPort,
      JedisClientConfig clientConfig,
      int maxAttempts,
      int retryDurationMillis
  ) {
    super(
      new PooledConnectionProvider(
        hostAndPort,
        clientConfig,
        poolConfig
      ),
      maxAttempts, 
      Duration.ofMillis(
        retryDurationMillis
      )
    );
  }

  @Override
  public PipelineBase pipelined(Executor executor) {
    return super.pipelined();
  }

  public Connection getConnection() {
    return super.provider.getConnection();
  } 
}
