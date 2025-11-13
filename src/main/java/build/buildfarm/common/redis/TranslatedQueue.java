// Copyright 2025 The Buildfarm Authors. All rights reserved.
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

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayList;

import build.buildfarm.common.Queue;
import build.buildfarm.common.Visitor;
import com.google.protobuf.Message;
import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;
import redis.clients.jedis.AbstractPipeline;
import redis.clients.jedis.resps.ScanResult;

class TranslatedQueue<E extends Message> implements Queue<E> {
  private final Queue<String> queue;
  private final StringTranslator<E> translator;

  public TranslatedQueue(Queue<String> queue, StringTranslator<E> translator) {
    this.queue = queue;
    this.translator = translator;
  }

  // java.util.BlockingQueue-ish
  @Override
  public E take(Duration timeout) throws InterruptedException {
    return translator.parse(queue.take(timeout));
  }

  // java.util.Queue
  @Override
  public E poll() {
    return translator.parse(queue.poll());
  }

  @Override
  public boolean offer(E e) {
    return queue.offer(translator.print(e));
  }

  // our special variety
  @Override
  public boolean offer(E e, double priority) {
    return queue.offer(translator.print(e), priority);
  }

  // java.util.Collection
  @Override
  public long size() {
    return queue.size();
  }

  @Override
  public Supplier<Long> size(AbstractPipeline pipeline) {
    return queue.size(pipeline);
  }

  // maybe switch to iterator?
  @Override
  public void visit(Visitor<E> visitor) {
    queue.visit(value -> visitor.visit(translator.parse(value)));
  }

  @Override
  public void visitDequeue(Visitor<E> visitor) {
    queue.visitDequeue(value -> visitor.visit(translator.parse(value)));
  }

  @Override
  public boolean removeFromDequeue(E e) {
    return queue.removeFromDequeue(translator.print(e));
  }

  @Override
  public void removeFromDequeue(AbstractPipeline pipeline, E e) {
    queue.removeFromDequeue(pipeline, translator.print(e));
  }

  @Override
  public ScanResult<E> scan(String cursor, int count, String match) {
    ScanResult<String> scanResults = queue.scan(cursor, count, match);
    List<E> results =
        newArrayList(transform(scanResults.getResult(), value -> translator.parse(value)));
    return new ScanResult<>(scanResults.getCursor(), results);
  }
}
