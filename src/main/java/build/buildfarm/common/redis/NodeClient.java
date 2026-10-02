package build.buildfarm.common.redis;

import redis.clients.jedis.Connection;
import redis.clients.jedis.UnifiedJedis;

public class NodeClient extends UnifiedJedis {
  public NodeClient(Connection connection) {
    super(connection);
  }
}
