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

package build.buildfarm.instance.queues;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.google.longrunning.Operation;
import java.util.List;

/**
 * A "worker queue" represents particular operations that should only be processed by particular
 * workers.
 *
 * <p>An example use case for this would be to have two dedicated worker queues for CPU and GPU
 * operations. CPU/GPU requirements would be determined through the remote api's command platform
 * properties. We designate worker queues to have a set of "required provisions" (which match the
 * platform properties). This allows specific workers to be chosen and specific operations to be
 * queued.
 */
public class WorkerQueue {

  /* a name for the queue */
  public String name;

  /* The required provisions to allow workers and operations to be added to the queue. */
  public SetMultimap<String, String> requiredProvisions = LinkedHashMultimap.create();

  /* The queue itself */
  public List<Operation> operations = Lists.newArrayList();

  /* The worker's whose own configured provisions allow them to be eligible to access this queue. */
  public List<Worker> workers = Lists.newArrayList();
}
