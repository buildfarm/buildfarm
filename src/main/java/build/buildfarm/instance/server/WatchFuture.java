/**
 * Asynchronous computation result handler
 * @param watcher the watcher parameter
 * @return the public result
 */
// Copyright 2017 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.instance.server;

import build.buildfarm.common.Watcher;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.longrunning.Operation;

public abstract class WatchFuture extends AbstractFuture<Void> {
  private final Watcher watcher;

  /**
   * Performs specialized operation based on method logic
   * @param operation the operation parameter
   */
  public WatchFuture(Watcher watcher) {
    this.watcher = watcher;
  }

  public void observe(Operation operation) {
    try {
      watcher.observe(operation);
      if (operation == null || operation.getDone()) {
        set(null);
      }
    } catch (Throwable t) {
      setException(t);
    }
  }

  /**
   * Performs specialized operation based on method logic
   */
  protected abstract void unwatch();

  @Override
  protected void afterDone() {
    unwatch();
  }
}
