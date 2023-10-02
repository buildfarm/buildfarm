// Copyright 2022 The Bazel Authors. All rights reserved.
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

import build.buildfarm.common.distributed.DistributedMap;
import build.buildfarm.common.redis.BalancedRedisQueue;

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
  public DistributedMap executeWorkers;

  /**
   * @field storageWorkers
   * @brief All of the storage-capable workers register themselves to the cluster.
   * @details This is done to keep track of which machines are online and known by the rest of the
   *     cluster.
   */
  public DistributedMap storageWorkers;

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
  public DistributedMap actionCache;

  /**
   * @field operationQueue
   * @brief The queue that holds operations that are ready to be executed by the worker pool.
   * @details The operation queue is actually made of multiple queues based on platform properties.
   *     There is a matching algorithm that decides how servers place items onto it, and how workers
   *     pop requests off of it.
   */
  public OperationQueue operationQueue;

  /**
   * @field operations
   * @brief Operations involved in action execution on the system.
   * @details We keep track of them in the distributed state to avoid them getting lost if a
   *     particular machine goes down. They should also exist for some period of time after a build
   *     invocation has finished so that developers can lookup the status of their build and
   *     information about the operations that ran.
   */
  public DistributedMap operations;

  /**
   * @field processingOperations
   * @brief Operations that are being processed from the prequeue to the operation queue.
   * @details We keep track of them in the distributed state to avoid them getting lost if a
   *     particular machine goes down.
   */
  public DistributedMap processingOperations;

  /**
   * @field dispatchingOperations
   * @brief Operations that are in the process of being dispatched from the operation queue.
   * @details We keep track of them in the distributed state to avoid them getting lost if a
   *     particular machine goes down.
   */
  public DistributedMap dispatchingOperations;

  /**
   * @field dispatchedOperations
   * @brief Operations that currently being executed.
   * @details We keep track of them here so they can be re-executed if the progress of the execution
   *     is lost.
   */
  public DistributedMap dispatchedOperations;

  /**
   * @field casWorkerMap
   * @brief This map keeps track of which workers have which CAS blobs.
   * @details Its the worker's responsibility to share this information to others. We do this to
   *     inform other machines where to find CAS data.
   */
  public CasWorkerMap casWorkerMap;

  /**
   * @field blockedInvocations
   * @brief Invocations that the cluster has decided it no longer wants to execute in the future.
   * @details The requests tied to these invocations will be denied and the client will not get
   *     their action request executed. The use-case is blocking a particular user or client from
   *     continuing their requests.
   */
  public DistributedMap blockedInvocations;

  /**
   * @field blockedActions
   * @brief Actions that the cluster has decided it no longer wants to execute in the future.
   * @details The requests tied to these actions will be denied and the client will not get their
   *     action request executed. The use case is blocking a certain action hash they we didn't like
   *     and will refuse to run again.
   */
  public DistributedMap blockedActions;
}
