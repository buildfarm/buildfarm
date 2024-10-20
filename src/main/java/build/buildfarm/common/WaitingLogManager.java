// Copyright 2020 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.common;

import java.util.logging.LogManager;
import javax.annotation.concurrent.GuardedBy;

@SuppressWarnings("PMD.MutableStaticState")
public class WaitingLogManager extends LogManager {
  private static WaitingLogManager instance = null;

  @GuardedBy("this")
  private boolean resetCalled = false;

  @GuardedBy("this")
  private boolean released = false;

  public WaitingLogManager() {
    instance = this;
  }

  @Override
  public synchronized void reset() {
    resetCalled = true;
    reset0();
  }

  @GuardedBy("this")
  private void reset0() {
    if (released && resetCalled) {
      super.reset();
    }
  }

  @SuppressWarnings("GuardedBy")
  private synchronized void releaseReset() {
    released = true;
    instance.reset0();
  }

  public static void release() {
    if (instance != null) {
      instance.releaseReset();
    }
  }
}
