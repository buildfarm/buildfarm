/**
 * Performs specialized operation based on method logic
 * @param clusterNodes the clusterNodes parameter
 * @param clientConfig the clientConfig parameter
 * @param maxAttempts the maxAttempts parameter
 * @param poolConfig the poolConfig parameter
 * @return the public result
 */
package build.buildfarm.common.redis;

import java.util.Set;
import java.util.concurrent.Executor;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.ClusterCommandObjects;
import redis.clients.jedis.Connection;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.providers.ClusterConnectionProvider;

public class Cluster extends JedisCluster implements Unified {
  /**
   * Performs specialized operation based on method logic
   * @param executor the executor parameter
   * @return the clusterpipeline result
   */
  public Cluster(
      Set<HostAndPort> clusterNodes,
      JedisClientConfig clientConfig,
      int maxAttempts,
      GenericObjectPoolConfig<Connection> poolConfig) {
    super(clusterNodes, clientConfig, maxAttempts, poolConfig);
  }

  @Override
  public ClusterPipeline pipelined(Executor executor) {
    return new ClusterPipeline(
        (ClusterConnectionProvider) provider, (ClusterCommandObjects) commandObjects, executor);
  }
}
