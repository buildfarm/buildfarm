// Copyright 2022 The Buildfarm Authors. All rights reserved.
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

import build.buildfarm.common.redis.BalancedRedisQueue;
import build.buildfarm.common.redis.RedisHashMap;
import build.buildfarm.common.redis.RedisMap;
import build.buildfarm.common.redis.RedisSetMap;

/**
 * @class DistributedState
 * @brief The backplane contains distributed state that is shared across the entire cluster. We use
 *     containerized wrappers to abstract over any backend system managing state.
 * @details Traditionally redis has been chosen for distributed state management, but alternative
 *     backends can be chosen by choosing customized containers.
 */
public class DistributedState {
  /**
   * @field executeWorkers
   * @brief All of the execute-capable workers register themselves to the cluster.
   * @details This is done to keep track of which machines are online and known by the rest of the
   *     cluster.
   */
  public RedisHashMap executeWorkers;

  /**
   * @field storageWorkers
   * @brief All of the storage-capable workers register themselves to the cluster.
   * @details This is done to keep track of which machines are online and known by the rest of the
   *     cluster.
   */
  public RedisHashMap storageWorkers;

  /**
   * @field servers
   * @brief All of the active servers register themselves to the cluster.
   * @details This is done to keep track of which servers are online and known by the rest of the
   *     cluster.
   */
  public RedisHashMap servers;

  /**
   * @field prequeue
   * @brief Where execution requests are initially placed once recieved from the frontend.
   * @details This is to allow the frontend to quickly place requests and use its resources to take
   *     on more requests.
   */
  public BalancedRedisQueue prequeue;

  /**
   * @field actionCache
   * @brief The implementation of an action cache.
   * @details See
   *     https://github.com/bazelbuild/remote-apis/blob/3b4b6402103539d66fcdd1a4d945f660742665ca/build/bazel/remote/execution/v2/remote_execution.proto#L144
   *     for the remote API definition of an action cache.
   */
  public RedisMap actionCache;

  /**
   * @field executionQueue
   * @brief The queue that holds executions that are ready to be executed by the worker pool.
   * @details The operation queue is actually made of multiple queues based on platform properties.
   *     There is a matching algorithm that decides how servers place items onto it, and how workers
   *     pop requests off of it.
   */
  public ExecutionQueue executionQueue;

  /**
   * @field executions
   * @brief Action executions on the system.
   * @details We keep track of them in the distributed state to avoid them getting lost if a
   *     particular machine goes down. They should also exist for some period of time after a build
   *     invocation has finished so that developers can lookup the status of their build and
   *     information about the operations that ran.
   */
  public Executions executions;

  /**
   * @field processingExecutions
   * @brief Executions that are being processed from the prequeue to the operation queue.
   * @details We keep track of them in the distributed state to avoid them getting lost if a
   *     particular machine goes down.
   */
  public RedisMap processingExecutions;

  /**
   * @field dispatchingExecutions
   * @brief Executions that are in the process of being dispatched from the operation queue.
   * @details We keep track of them in the distributed state to avoid them getting lost if a
   *     particular machine goes down.
   */
  public RedisMap dispatchingExecutions;

  /**
   * @field dispatchedExecutions
   * @brief Executions that currently being executed.
   * @details We keep track of them here so they can be re-executed if the progress of the execution
   *     is lost.
   */
  public RedisHashMap dispatchedExecutions;

  /**
   * @field blockedInvocations
   * @brief Invocations that the cluster has decided it no longer wants to execute in the future.
   * @details The requests tied to these invocations will be denied and the client will not get
   *     their action request executed. The use-case is blocking a particular user or client from
   *     continuing their requests.
   */
  public RedisMap blockedInvocations;

  /**
   * @field blockedActions
   * @brief Actions that the cluster has decided it no longer wants to execute in the future.
   * @details The requests tied to these actions will be denied and the client will not get their
   *     action request executed. The use case is blocking a certain action hash they we didn't like
   *     and will refuse to run again.
   */
  public RedisMap blockedActions;

  public RedisSetMap toolInvocations;

  public RedisSetMap correlatedInvocations;

  public RedisSetMap correlatedInvocationsIndex;
}
