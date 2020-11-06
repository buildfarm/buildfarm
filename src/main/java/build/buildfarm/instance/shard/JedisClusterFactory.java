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

import build.buildfarm.v1test.RedisShardBackplaneConfig;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.naming.ConfigurationException;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

///
/// @class   JedisClusterFactory
/// @brief   Create a jedis cluster instance from proto configs.
/// @details A factory for creating a jedis cluster instance.
///
public class JedisClusterFactory {

  ///
  /// @brief   Create a jedis cluster instance.
  /// @details Use proto configuration to connect to a redis cluster server and
  ///          provide a jedis client.
  /// @param   config Configuration for connecting to a redis cluster server.
  /// @return  An established jedis client used to operate on the redis cluster.
  /// @note    Suggested return identifier: jedis.
  ///
  public static Supplier<JedisCluster> create(RedisShardBackplaneConfig config)
      throws ConfigurationException {
    // null password is required to elicit no auth in jedis
    return createJedisClusterFactory(
        parseUri(config.getRedisUri()),
        config.getTimeout(),
        config.getMaxAttempts(),
        config.getRedisPassword().isEmpty() ? null : config.getRedisPassword(),
        createJedisPoolConfig(config));
  }
  ///
  /// @brief   Create a test jedis cluster instance.
  /// @details Use pre-defined proto configuration to connect to a redis
  ///          cluster server and provide a jedis client.
  /// @return  An established test jedis client used to operate on a redis cluster.
  /// @note    Suggested return identifier: jedis.
  ///
  public static JedisCluster createTest() throws Exception {
    // create the a client to interact with redis.
    // we assume you are running a local cluster of redis.
    // configuration values (port chosen by redis create-clusters).
    RedisShardBackplaneConfig config =
        RedisShardBackplaneConfig.newBuilder()
            .setRedisUri("redis://localhost:30001")
            .setJedisPoolMaxTotal(3)
            .build();
    JedisCluster redis = JedisClusterFactory.create(config).get();

    // use the client to create an empty redis cluster
    // this will prevent any persistent data across test runs
    // it also means that tests will not be able to run in parallel.
    deleteExistingKeys(redis);

    return redis;
  }
  ///
  /// @brief   Delete existing keys on a redis cluster.
  /// @details Delete all of the keys on a redis cluster and ensure that the
  ///          database is empty.
  /// @param   cluster An established jedis client to operate on a redis cluster.
  /// @note    Overloaded.
  ///
  private static void deleteExistingKeys(JedisCluster cluster) throws Exception {
    for (JedisPool pool : cluster.getClusterNodes().values()) {
      Jedis node = pool.getResource();
      deleteExistingKeys(node);
    }
  }
  ///
  /// @brief   Delete existing keys on a redis node.
  /// @details Delete all of the keys on a particular redis node and ensure
  ///          that the node's contribution to the database is empty.
  /// @param   node An established jedis client to operate on a redis node.
  /// @note    Overloaded.
  ///
  private static void deleteExistingKeys(Jedis node) throws Exception {
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

    if (matchingKeys.size() == 0) {
      return;
    }

    // we cannot pass all of the keys to del because of the following error:
    // "CROSSSLOT Keys in request don't hash to the same slot"
    // so iterate over and delete them individually.
    for (String key : matchingKeys.toArray(new String[matchingKeys.size()])) {
      node.del(key);
    }
  }

  ///
  /// @brief   Create a jedis cluster instance with connection settings.
  /// @details Use the URI, pool and connection information to connect to a redis cluster
  ///          server and provide a jedis client.
  /// @param   redisUri   A valid uri to a redis instance.
  /// @param   timeout Connection timeout
  /// @param   maxAttempts Number of connection attempts
  /// @param   poolConfig Configuration related to redis pools.
  /// @return  An established jedis client used to operate on the redis cluster.
  /// @note    Suggested return identifier: jedis.
  ///
  private static Supplier<JedisCluster> createJedisClusterFactory(
      URI redisUri, int timeout, int maxAttempts, String password, JedisPoolConfig poolConfig) {
    return () -> {
      Set<HostAndPort> hostsAndPorts;
      try {
        hostsAndPorts =
            Arrays.stream(InetAddress.getAllByName(redisUri.getHost()))
                .map(a -> new HostAndPort(a.getHostAddress(), redisUri.getPort()))
                .collect(Collectors.toSet());
      } catch (UnknownHostException e) {
        hostsAndPorts = new HashSet<>();
        hostsAndPorts.add(new HostAndPort(redisUri.getHost(), redisUri.getPort()));
      }
      return new JedisCluster(
          hostsAndPorts,
          /* connectionTimeout=*/ Integer.max(2000, timeout),
          /* soTimeout=*/ Integer.max(2000, timeout),
          Integer.max(5, maxAttempts),
          password,
          poolConfig);
    };
  }
  ///
  /// @brief   Create a jedis pool config.
  /// @details Use configuration to build the appropriate jedis pool
  ///          configuration.
  /// @param   config Configuration for connecting to a redis cluster server.
  /// @return  A created jedis pool config.
  /// @note    Suggested return identifier: poolConfig.
  ///
  private static JedisPoolConfig createJedisPoolConfig(RedisShardBackplaneConfig config) {
    JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    jedisPoolConfig.setMaxTotal(config.getJedisPoolMaxTotal());
    return jedisPoolConfig;
  }
  ///
  /// @brief   Parse string URI into URI object.
  /// @details Convert the string representation of the URI into a URI object.
  ///          If the URI object is invalid a configuration exception will be
  ///          thrown.
  /// @param   uri A uri.
  /// @return  A parsed and valid URI.
  /// @note    Suggested return identifier: uri.
  ///
  private static URI parseUri(String uri) throws ConfigurationException {
    try {
      return new URI(uri);
    } catch (URISyntaxException e) {
      throw new ConfigurationException(e.getMessage());
    }
  }
}
