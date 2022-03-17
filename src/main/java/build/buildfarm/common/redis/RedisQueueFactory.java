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

/**
 * @class RedisQueueFactory
 * @brief A redis queue factory.
 * @details A redis queue is an implementation of a queue data structure which internally uses redis
 *     to store and distribute the data. Its important to know that the lifetime of the queue
 *     persists before and after the queue data structure is created (since it exists in redis).
 *     Therefore, two redis queues with the same name, would in fact be the same underlying redis
 *     queue.
 */
public class RedisQueueFactory {

  public QueueInterface getQueue(String queueType, String name) {
    if (queueType == null) {
      return null;
    }
    if (queueType.equalsIgnoreCase("regular")) {
      return new RedisQueue(name);

    } else if (queueType.equalsIgnoreCase("priority")) {
      return new RedisPriorityQueue(name);
    }
    return null;
  }
}
