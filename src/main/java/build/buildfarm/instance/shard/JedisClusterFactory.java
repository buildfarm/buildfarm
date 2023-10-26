// Copyright 2020 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.instance.shard;

import build.buildfarm.common.config.BuildfarmConfigs;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import javax.naming.ConfigurationException;
import redis.clients.jedis.ConnectionPool;
import redis.clients.jedis.ConnectionPoolConfig;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisClusterOperationException;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.util.JedisURIHelper;

/**
 * @class JedisClusterFactory
 * @brief Create a jedis cluster instance from proto configs.
 * @details A factory for creating a jedis cluster instance.
 */
public class JedisClusterFactory {
  private static BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  /**
   * @brief Create a jedis cluster instance.
   * @details Use proto configuration to connect to a redis cluster server and provide a jedis
   *     client.
   * @param identifier Redis Client name.
   * @return An established jedis client used to operate on the redis cluster.
   * @note Suggested return identifier: jedis.
   * @link <a href="https://redis.io/commands/client-setname/">Redis Client name</a>
   */
  public static Supplier<UnifiedJedis> create(String identifier) throws ConfigurationException {
    // null password is required to elicit no auth in jedis
    String[] redisNodes = configs.getBackplane().getRedisNodes();
    if (redisNodes != null && redisNodes.length > 0) {
      return createJedisClusterFactory(
          identifier,
          list2Set(redisNodes),
          configs.getBackplane().getTimeout(),
          configs.getBackplane().getMaxAttempts(),
          Strings.emptyToNull(configs.getBackplane().getRedisPassword()),
          createConnectionPoolConfig());
    }

    // support "" as redis password.
    return createJedisClusterFactory(
        identifier,
        parseUri(configs.getBackplane().getRedisUri()),
        configs.getBackplane().getTimeout(),
        configs.getBackplane().getMaxAttempts(),
        Strings.emptyToNull(configs.getBackplane().getRedisPassword()),
        createConnectionPoolConfig());
  }

  /**
   * @brief Create a test jedis cluster instance.
   * @details Use pre-defined proto configuration to connect to a redis cluster server and provide a
   *     jedis client.
   * @return An established test jedis client used to operate on a redis cluster.
   * @note Suggested return identifier: jedis.
   */
  public static UnifiedJedis createTest() throws Exception {
    UnifiedJedis redis = JedisClusterFactory.create("test").get();

    // use the client to create an empty redis cluster
    // this will prevent any persistent data across test runs
    // it also means that tests will not be able to run in parallel.
    deleteExistingKeys(redis);

    return redis;
  }

  /**
   * @brief Delete existing keys on a redis cluster.
   * @details Delete all of the keys on a redis cluster and ensure that the database is empty.
   * @param cluster An established jedis client to operate on a redis cluster.
   * @note Overloaded.
   */
  private static void deleteExistingKeys(UnifiedJedis jedis) throws Exception {
    if (jedis instanceof JedisCluster) {
      JedisCluster cluster = (JedisCluster) jedis;
      for (ConnectionPool pool : cluster.getClusterNodes().values()) {
        UnifiedJedis node = new UnifiedJedis(pool.getResource());
        deleteNodeExistingKeys(node);
      }
    } else {
      deleteNodeExistingKeys(jedis);
    }
  }

  /**
   * @brief Delete existing keys on a redis node.
   * @details Delete all of the keys on a particular redis node and ensure that the node's
   *     contribution to the database is empty.
   * @param node An established jedis client to operate on a redis node.
   * @note Overloaded.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void deleteNodeExistingKeys(UnifiedJedis node) {
    String nextCursor = "0";
    Set<String> matchingKeys = new HashSet<>();
    ScanParams params = new ScanParams();
    params.match("*");

    // get all of the keys for the particular node
    do {
      ScanResult scanResult = node.scan(nextCursor, params);
      List<String> keys = scanResult.getResult();
      nextCursor = scanResult.getCursor();

      matchingKeys.addAll(keys);

    } while (!nextCursor.equals("0"));

    if (matchingKeys.isEmpty()) {
      return;
    }

    // we cannot pass all of the keys to del because of the following error:
    // "CROSSSLOT Keys in request don't hash to the same slot"
    // so iterate over and delete them individually.
    for (String key : matchingKeys.toArray(new String[0])) {
      node.del(key);
    }
  }

  private static UnifiedJedis createJedis(
      Set<HostAndPort> hostAndPorts,
      int connectionTimeout,
      int soTimeout,
      int maxAttempts,
      String password,
      String identifier,
      ConnectionPoolConfig poolConfig,
      boolean ssl) {
    try {
      return new JedisCluster(
          hostAndPorts,
          connectionTimeout,
          soTimeout,
          maxAttempts,
          password,
          identifier,
          poolConfig,
          ssl);
    } catch (JedisClusterOperationException e) {
      // probably not a cluster
      return new JedisPooled(
          poolConfig,
          Iterables.getOnlyElement(hostAndPorts),
          DefaultJedisClientConfig.builder()
              .connectionTimeoutMillis(connectionTimeout)
              .socketTimeoutMillis(soTimeout)
              .password(password)
              .clientName(identifier)
              .ssl(ssl)
              .build());
    }
  }

  /**
   * @brief Create a jedis cluster instance with connection settings.
   * @details Use the URI, pool and connection information to connect to a redis cluster server and
   *     provide a jedis client.
   * @param redisUri A valid uri to a redis instance.
   * @param timeout Connection timeout
   * @param maxAttempts Number of connection attempts
   * @param poolConfig Configuration related to redis pools.
   * @return An established jedis client used to operate on the redis cluster.
   * @note Suggested return identifier: jedis.
   */
  private static Supplier<UnifiedJedis> createJedisClusterFactory(
      String identifier,
      URI redisUri,
      int timeout,
      int maxAttempts,
      String password,
      ConnectionPoolConfig poolConfig) {
    return () ->
        createJedis(
            ImmutableSet.of(new HostAndPort(redisUri.getHost(), redisUri.getPort())),
            /* connectionTimeout=*/ Integer.max(2000, timeout),
            /* soTimeout=*/ Integer.max(2000, timeout),
            Integer.max(5, maxAttempts),
            password,
            identifier,
            poolConfig,
            /* ssl=*/ JedisURIHelper.isRedisSSLScheme(redisUri));
  }

  /**
   * @brief Create a jedis cluster instance with connection settings.
   * @details Use the nodes addresses, pool and connection information to connect to a redis cluster
   *     server and provide a jedis client.
   * @param redisUrisNodes A valid uri set to a redis nodes instances.
   * @param timeout Connection timeout
   * @param maxAttempts Number of connection attempts
   * @param poolConfig Configuration related to redis pools.
   * @return An established jedis client used to operate on the redis cluster.
   * @note Suggested return identifier: jedis.
   */
  private static Supplier<UnifiedJedis> createJedisClusterFactory(
      String identifier,
      Set<HostAndPort> redisUrisNodes,
      int timeout,
      int maxAttempts,
      String password,
      ConnectionPoolConfig poolConfig) {
    return () ->
        createJedis(
            redisUrisNodes,
            /* connectionTimeout=*/ Integer.max(2000, timeout),
            /* soTimeout=*/ Integer.max(2000, timeout),
            Integer.max(5, maxAttempts),
            password,
            identifier,
            poolConfig,
            /* ssl=*/ false);
  }
  /**
   * @brief Create a connection pool config.
   * @details Use configuration to build the appropriate jedis pool configuration.
   * @return A created jedis pool config.
   * @note Suggested return identifier: poolConfig.
   */
  private static ConnectionPoolConfig createConnectionPoolConfig() {
    ConnectionPoolConfig connectionPoolConfig = new ConnectionPoolConfig();
    connectionPoolConfig.setMaxTotal(configs.getBackplane().getJedisPoolMaxTotal());
    return connectionPoolConfig;
  }

  /**
   * @brief Parse string URI into URI object.
   * @details Convert the string representation of the URI into a URI object. If the URI object is
   *     invalid a configuration exception will be thrown.
   * @param uri A uri.
   * @return A parsed and valid URI.
   * @note Suggested return identifier: uri.
   */
  private static URI parseUri(String uri) throws ConfigurationException {
    try {
      return new URI(uri);
    } catch (URISyntaxException e) {
      throw new ConfigurationException(e.getMessage());
    }
  }

  /**
   * @brief Convert protobuff list to set
   * @details Convert the string list representation of the nodes URIs into a set of HostAndPort
   *     objects. If the URI object is invalid a configuration exception will be thrown.
   * @param nodes The redis nodes.
   * @return A parsed and valid HostAndPort set.
   */
  private static Set<HostAndPort> list2Set(String[] nodes) throws ConfigurationException {
    Set<HostAndPort> jedisClusterNodes = new HashSet<>();
    try {
      for (String node : nodes) {
        URI redisUri = new URI(node);
        jedisClusterNodes.add(new HostAndPort(redisUri.getHost(), redisUri.getPort()));
      }
      return jedisClusterNodes;
    } catch (URISyntaxException e) {
      throw new ConfigurationException(e.getMessage());
    }
  }
}
