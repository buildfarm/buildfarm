package build.buildfarm.common.redis;

import java.util.concurrent.Executor;
import redis.clients.jedis.ClusterCommandArguments;
import redis.clients.jedis.ClusterCommandObjects;
import redis.clients.jedis.CommandArguments;
import redis.clients.jedis.Connection;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.RedisProtocol;
import redis.clients.jedis.providers.ClusterConnectionProvider;
import redis.clients.jedis.util.IOUtils;

public class ClusterPipeline extends MultiNodePipelineBase {
  private final ClusterConnectionProvider provider;
  private AutoCloseable closeable = null;

  public ClusterPipeline(
      ClusterConnectionProvider provider, ClusterCommandObjects commandObjects, Executor executor) {
    super(commandObjects, executor);
    this.provider = provider;
  }

  private static ClusterCommandObjects createClusterCommandObjects(RedisProtocol protocol) {
    ClusterCommandObjects cco = new ClusterCommandObjects();
    if (protocol == RedisProtocol.RESP3) cco.setProtocol(protocol);
    return cco;
  }

  @Override
  public void close() {
    try {
      super.close();
    } finally {
      IOUtils.closeQuietly(closeable);
    }
  }

  @Override
  protected HostAndPort getNodeKey(CommandArguments args) {
    return provider.getNode(((ClusterCommandArguments) args).getCommandHashSlot());
  }

  @Override
  protected Connection getConnection(HostAndPort nodeKey) {
    return provider.getConnection(nodeKey);
  }
}
