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
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

class DispatchedMonitor implements Runnable {
  private final ShardBackplane backplane;
  private final Function<String, Boolean> onRequeue;
  private final Consumer<String> onCancel;

  DispatchedMonitor(
      ShardBackplane backplane,
      Function<String, Boolean> onRequeue,
      Consumer<String> onCancel) {
    this.backplane = backplane;
    this.onRequeue = onRequeue;
    this.onCancel = onCancel;
  }

  @Override
  public synchronized void run() {
    System.out.println("DispatchedMonitor: Running");
    while (true) {
      try {
        long now = System.currentTimeMillis(); /* FIXME sync */
        /* iterate over dispatched */
        for (ShardDispatchedOperation o : backplane.getDispatchedOperations()) {
          /* if now > dispatchedOperation.getExpiresAt() */
          if (now >= o.getRequeueAt()) {
            System.out.println("DispatchedMonitor: Testing " + o.getName() + " because " + now + " >= " + o.getRequeueAt());
            long startTime = System.nanoTime();
            boolean shouldCancel = !onRequeue.apply(o.getName());
            if (Thread.interrupted()) {
              throw new InterruptedException();
            }
            long endTime = System.nanoTime();
            float ms = (endTime - startTime) / 1000000.0f;
            System.out.println(String.format("DispatchedMonitor::run: onRequeue(%s) %gms", o.getName(), ms));
            if (shouldCancel) {
              backplane.completeOperation(o.getName());
              System.out.println("DispatchedMonitor: Cancel " + o.getName());
              onCancel.accept(o.getName());
              if (Thread.interrupted()) {
                throw new InterruptedException();
              }
            } else {
              System.out.println("DispatchedMonitor: Requeued " + o.getName());
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
    System.out.println("DispatchedMonitor: Exiting");
  }
}
