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

package build.buildfarm.common.redis;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

///
/// @class   ProvisionedRedisQueueMockTest
/// @brief   tests A queue that is designed to hold particularly provisioned
///          elements.
/// @details A provisioned redis queue is an implementation of a queue data
///          structure which internally uses a redis cluster to distribute the
///          data across shards. Its important to know that the lifetime of
///          the queue persists before and after the queue data structure is
///          created (since it exists in redis). Therefore, two redis queues
///          with the same name, would in fact be the same underlying redis
///          queue. This redis queue comes with a list of required provisions.
///          If the queue element does not meet the required provisions, it
///          should not be stored in the queue. Provision queues are intended
///          to represent particular operations that should only be processed
///          by particular workers. An example use case for this would be to
///          have two dedicated provision queues for CPU and GPU operations.
///          CPU/GPU requirements would be determined through the remote api's
///          command platform properties. We designate provision queues to
///          have a set of "required provisions" (which match the platform
///          properties). This allows the scheduler to distribute operations
///          by their properties and allows workers to dequeue from particular
///          queues.
///
@RunWith(JUnit4.class)
public class ProvisionedRedisQueueMockTest {

  // Function under test: provisionedRedisQueue
  // Reason for testing: the object can be constructed
  // Failure explanation: the object cannot be constructed
  @Test
  public void provisionedRedisQueueCanConstruct() throws Exception {

    ProvisionedRedisQueue queue =
        new ProvisionedRedisQueue("name", ImmutableList.of(), HashMultimap.create());
  }
}
