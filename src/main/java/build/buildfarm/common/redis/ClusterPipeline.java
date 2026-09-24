package build.buildfarm.common.redis;

import java.util.Set;
import java.util.concurrent.Executor;
import redis.clients.jedis.ClusterCommandObjects;
import redis.clients.jedis.CommandArguments;
import redis.clients.jedis.Connection;
import redis.clients.jedis.HostAndPort;
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
    Set<Integer> slots = args.getKeyHashSlots();
    if (slots.size() > 1) {
      throw new IllegalArgumentException("Cannot get NodeKey for command with multiple hash slots");
    }
    if (slots.isEmpty()) {
      return null;
    }
    return provider.getNode(slots.iterator().next());
  }

  @Override
  protected Connection getConnection(HostAndPort nodeKey) {
    return provider.getConnection(nodeKey);
  }
}
