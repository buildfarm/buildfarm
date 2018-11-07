// Copyright 2018 The Bazel Authors. All rights reserved.
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

import build.buildfarm.common.ShardBackplane;
import build.buildfarm.v1test.ShardDispatchedOperation;
import com.google.rpc.Code;
import com.google.rpc.Status;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.logging.Logger;

class DispatchedMonitor implements Runnable {
  private final static Logger logger = Logger.getLogger(DispatchedMonitor.class.getName());

  private final ShardBackplane backplane;
  private final BiPredicate<String, Status.Builder> onRequeue;
  private final BiConsumer<String, Status> onCancel;

  DispatchedMonitor(
      ShardBackplane backplane,
      BiPredicate<String, Status.Builder> onRequeue,
      BiConsumer<String, Status> onCancel) {
    this.backplane = backplane;
    this.onRequeue = onRequeue;
    this.onCancel = onCancel;
  }

  @Override
  public synchronized void run() {
    logger.info("DispatchedMonitor: Running");
    while (true) {
      try {
        long now = System.currentTimeMillis(); /* FIXME sync */
        boolean canQueueNow = backplane.canQueue();
        /* iterate over dispatched */
        for (ShardDispatchedOperation o : backplane.getDispatchedOperations()) {
          /* if now > dispatchedOperation.getExpiresAt() */
          if (now >= o.getRequeueAt()) {
            logger.info("DispatchedMonitor: Testing " + o.getName() + " because " + now + " >= " + o.getRequeueAt());
            long startTime = System.nanoTime();
            Status.Builder statusBuilder = Status.newBuilder()
                .setCode(Code.UNKNOWN.getNumber());
            boolean shouldCancel = !canQueueNow || !onRequeue.test(o.getName(), statusBuilder);
            if (Thread.interrupted()) {
              throw new InterruptedException();
            }
            long endTime = System.nanoTime();
            float ms = (endTime - startTime) / 1000000.0f;
            logger.info(String.format("DispatchedMonitor::run: onRequeue(%s) %gms", o.getName(), ms));
            if (shouldCancel) {
              backplane.completeOperation(o.getName());
              logger.info("DispatchedMonitor: Cancel " + o.getName());
              onCancel.accept(o.getName(), statusBuilder.build());
              if (Thread.interrupted()) {
                throw new InterruptedException();
              }
            } else {
              logger.info("DispatchedMonitor: Requeued " + o.getName());
            }
          }
        }
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    logger.info("DispatchedMonitor: Exiting");
  }
}
