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

import com.google.common.collect.Iterables;
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
    return provider.getNode(Iterables.getOnlyElement(slots));
  }

  @Override
  protected Connection getConnection(HostAndPort nodeKey) {
    return provider.getConnection(nodeKey);
  }
}
