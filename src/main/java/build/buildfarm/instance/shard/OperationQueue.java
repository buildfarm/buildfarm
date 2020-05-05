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

import build.buildfarm.common.StringVisitor;
import build.buildfarm.common.redis.ProvisionedRedisQueue;
import build.buildfarm.v1test.QueueStatus;
import java.util.List;
import redis.clients.jedis.JedisCluster;

///
/// @class   OperationQueue
/// @brief   The operation queue of the shard backplane.
/// @details The operation queue can be split into multiple queues according
///          to platform execution information.
///
public class OperationQueue {

  ///
  /// @field   queues
  /// @brief   Different queues based on platform execution requirements.
  /// @details The appropriate queues are chosen based on given properties.
  ///
  private List<ProvisionedRedisQueue> queues;

  ///
  /// @brief   Constructor.
  /// @details Construct the operation queue with various provisioned redis
  ///          queues.
  /// @param   queues Provisioned queues.
  ///
  public OperationQueue(List<ProvisionedRedisQueue> queues) {
    this.queues = queues;
  }
  ///
  /// @brief   Visit each element in the dequeue.
  /// @details Enacts a visitor over each element in the dequeue.
  /// @param   jedis   Jedis cluster client.
  /// @param   visitor A visitor for each visited element in the queue.
  ///
  public void visitDequeue(JedisCluster jedis, StringVisitor visitor) {
    for (ProvisionedRedisQueue pQueue : queues) {
      pQueue.queue().visitDequeue(jedis, visitor);
    }
  }
  ///
  /// @brief   Remove element from dequeue.
  /// @details Removes an element from the dequeue and specifies whether it was
  ///          removed.
  /// @param   jedis Jedis cluster client.
  /// @param   val   The value to remove.
  /// @return  Whether or not the value was removed.
  /// @note    Suggested return identifier: wasRemoved.
  ///
  public boolean removeFromDequeue(JedisCluster jedis, String val) {
    for (ProvisionedRedisQueue pQueue : queues) {
      if (pQueue.queue().removeFromDequeue(jedis, val)) {
        return true;
      }
    }
    return false;
  }
  ///
  /// @brief   Visit each element in the queue.
  /// @details Enacts a visitor over each element in the queue.
  /// @param   jedis   Jedis cluster client.
  /// @param   visitor A visitor for each visited element in the queue.
  ///
  public void visit(JedisCluster jedis, StringVisitor visitor) {
    for (ProvisionedRedisQueue pQueue : queues) {
      pQueue.queue().visit(jedis, visitor);
    }
  }
  ///
  /// @brief   Get size.
  /// @details Checks the current length of the queue.
  /// @param   jedis Jedis cluster client.
  /// @return  The current length of the queue.
  /// @note    Suggested return identifier: length.
  ///
  public long size(JedisCluster jedis) {
    // the accumulated size of all of the queues
    return queues.stream().mapToInt(i -> (int) i.queue().size(jedis)).sum();
  }
  ///
  /// @brief   Get dequeue name.
  /// @details Get the name of the internal dequeue used by the queue. since
  ///          each internal queue has their own dequeue, this name is generic
  ///          without the hashtag.
  /// @return  The name of the queue.
  /// @note    Suggested return identifier: name.
  ///
  public String getDequeueName() {
    return queues.get(0).queue().getDequeueName();
  }
  ///
  /// @brief   Push a value onto the queue.
  /// @details Adds the value into one of the internal backend redis queues.
  /// @param   jedis Jedis cluster client.
  /// @param   val   The value to push onto the queue.
  ///
  public void push(JedisCluster jedis, String val) {
    queues.get(0).queue().push(jedis, val);
  }
  ///
  /// @brief   Pop element into internal dequeue and return value.
  /// @details This pops the element from one queue atomically into an internal
  ///          list called the dequeue. It will perform an exponential backoff.
  ///          Null is returned if the overall backoff times out.
  /// @param   jedis Jedis cluster client.
  /// @return  The value of the transfered element. null if the thread was interrupted.
  /// @note    Suggested return identifier: val.
  ///
  public String dequeue(JedisCluster jedis) throws InterruptedException {
    return queues.get(0).queue().dequeue(jedis);
  }
  ///
  /// @brief   Get status information about the queue.
  /// @details Helpful for understanding the current load on the queue and how
  ///          elements are balanced.
  /// @param   jedis Jedis cluster client.
  /// @return  The current status of the queue.
  /// @note    Suggested return identifier: status.
  ///
  public QueueStatus status(JedisCluster jedis) {
    return queues.get(0).queue().status(jedis);
  }
}
