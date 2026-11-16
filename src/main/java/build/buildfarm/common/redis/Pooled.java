// Copyright 2024-2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.common.redis;

import java.util.concurrent.Executor;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.AbstractPipeline;
import redis.clients.jedis.Connection;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.executors.DefaultCommandExecutor;
import redis.clients.jedis.providers.PooledConnectionProvider;
import redis.clients.jedis.util.Pool;

public class Pooled extends UnifiedJedis implements Unified {
  public Pooled(
      GenericObjectPoolConfig<Connection> poolConfig,
      HostAndPort clusterNode,
      JedisClientConfig clientConfig) {
    this(new PooledConnectionProvider(clusterNode, clientConfig, poolConfig), clientConfig);
  }

  private Pooled(PooledConnectionProvider provider, JedisClientConfig clientConfig) {
    super(new DefaultCommandExecutor(provider), provider, clientConfig, null);
  }

  public Pool<Connection> getPool() {
    return ((PooledConnectionProvider) provider).getPool();
  }

  @Override
  public AbstractPipeline pipelined(Executor executor) {
    return super.pipelined();
  }
}
