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

package build.buildfarm.worker;

import com.google.common.collect.Sets;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Slots {
  // input queue
  // these inputs should become jobs and take claims
  public BlockingQueue<OperationContext> intake = new ArrayBlockingQueue<>(1);

  // the number of work units to share among jobs.
  public int width = 0;

  // the total number of work units currently claimed
  public AtomicInteger claims = new AtomicInteger(0);

  // BlockingQueue claims;

  // number of concurrent jobs running
  // should this be a thread pool?
  public Set<Thread> jobs = Sets.newHashSet();
}
