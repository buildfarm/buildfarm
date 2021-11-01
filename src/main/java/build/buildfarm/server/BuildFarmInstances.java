// Copyright 2017 The Bazel Authors. All rights reserved.
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

package build.buildfarm.server;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.memory.MemoryInstance;
import build.buildfarm.instance.shard.ShardInstance;
import build.buildfarm.v1test.InstanceConfig;
import javax.naming.ConfigurationException;

public class BuildFarmInstances {
  public static Instance createInstance(
      String session, InstanceConfig instanceConfig, Runnable onStop)
      throws InterruptedException, ConfigurationException {
    String name = instanceConfig.getName();
    HashFunction hashFunction = getValidHashFunction(instanceConfig);
    DigestUtil digestUtil = new DigestUtil(hashFunction);
    Instance instance;
    switch (instanceConfig.getTypeCase()) {
      default:
      case TYPE_NOT_SET:
        throw new IllegalArgumentException("Instance type not set in config");
      case MEMORY_INSTANCE_CONFIG:
        instance = new MemoryInstance(name, digestUtil, instanceConfig.getMemoryInstanceConfig());
        break;
      case SHARD_INSTANCE_CONFIG:
        instance =
            new ShardInstance(
                name,
                session + "-" + name,
                digestUtil,
                instanceConfig.getShardInstanceConfig(),
                onStop);
        break;
    }
    return instance;
  }

  private static HashFunction getValidHashFunction(InstanceConfig config)
      throws ConfigurationException {
    try {
      return HashFunction.get(config.getDigestFunction());
    } catch (IllegalArgumentException e) {
      throw new ConfigurationException("hash_function value unrecognized");
    }
  }
}
