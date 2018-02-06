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

class CompletedCollector implements Runnable {
  private final ShardBackplane backplane;
  private final Consumer<String> onCollected;
  private final long maxCompletedOperationsCount;

  CompletedCollector(
      ShardBackplane backplane,
      Consumer<String> onCollected,
      long maxCompletedOperationsCount) {
    this.backplane = backplane;
    this.onCollected = onCollected;
    this.maxCompletedOperationsCount = maxCompletedOperationsCount;
  }

  @Override
  public synchronized void run() {
    for (;;) {
      try {
        long completedOperationsCount = backplane.getCompletedOperationsCount();

        if (completedOperationsCount > maxCompletedOperationsCount) {
          String operationName = backplane.popOldestCompletedOperation();
          onCollected.accept(operationName);
        } else {
          TimeUnit.SECONDS.sleep(1);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
