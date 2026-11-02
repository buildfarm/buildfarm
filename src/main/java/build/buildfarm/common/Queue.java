// Copyright 2024-2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.common;

import java.time.Duration;
import java.util.function.Supplier;
import redis.clients.jedis.AbstractPipeline;
import redis.clients.jedis.resps.ScanResult;

public interface Queue<E> {
  // java.util.BlockingQueue-ish
  E take(Duration timeout) throws InterruptedException;

  // java.util.Queue
  E poll();

  boolean offer(E e);

  // our special variety
  boolean offer(E e, double priority);

  // java.util.Collection
  long size();

  Supplier<Long> size(AbstractPipeline pipeline);

  // maybe switch to iterator?
  void visit(Visitor<E> visitor);

  void visitDequeue(Visitor<E> visitor);

  boolean removeFromDequeue(E e);

  void removeFromDequeue(AbstractPipeline pipeline, E e);

  ScanResult<E> scan(String cursor, int count, String match);

  default int reclaimStaleMessages(long minIdleMillis) {
    return 0;
  }
}
