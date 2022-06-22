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
import java.util.concurrent.Semaphore;

public class Slots {
  public Slots(int width) {
    this.width = width;
    this.claims = new Semaphore(width, true);
  }

  // Each stage has an input context queue.  These contexts are evaluated to become jobs that take
  // claims.
  public BlockingQueue<OperationContext> intake = new ArrayBlockingQueue<>(1);

  // The total number of claims available between jobs.  If every job took 1 claim, the width would
  // correspond to the total number of jobs that can run concurrently in a stage.
  public int width;

  // The current number of claims currently held by all the jobs.  Keep in mind, that claims amount
  // isn't necessarily job amount.
  public Semaphore claims;

  // The number of concurrent jobs running.  Should this be a thread pool?
  public Set<Thread> jobs = Sets.newHashSet();
}
