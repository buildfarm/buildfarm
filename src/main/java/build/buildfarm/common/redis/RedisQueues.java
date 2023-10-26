// Copyright 2020-2022 The Bazel Authors. All rights reserved.
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

package build.buildfarm.common.redis;

import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.common.config.Queue.QUEUE_TYPE;

/**
 * @class RedisQueues
 * @brief Managed redis queue creation.
 */
public final class RedisQueues {
  private static BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  private RedisQueues() {}

  public static QueueInterface create(QUEUE_TYPE queueType, String name) {
    if (queueType == QUEUE_TYPE.standard) {
      return new RedisQueue(name);
    }
    if (queueType == QUEUE_TYPE.priority) {
      return new RedisPriorityQueue(name, configs.getBackplane().getPriorityPollIntervalMillis());
    }
    throw new IllegalArgumentException("Unknown queueType " + queueType);
  }
}
