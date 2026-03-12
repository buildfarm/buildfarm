// Copyright 2026 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.cas.cfc;

import static com.google.common.base.Preconditions.checkState;

class State {
  enum Value {
    STOPPED,
    STARTING, // files are checked for existence/read
    WRITING, // only writable state
    READ_ONLY
  }

  private Value value;

  State() {
    value = Value.STOPPED;
  }

  synchronized boolean isWritable() {
    return value == Value.WRITING;
  }

  synchronized boolean shouldRecordAccess() {
    return value == Value.WRITING || value == Value.READ_ONLY;
  }

  synchronized boolean setReadOnly(boolean readOnly) {
    if (readOnly && (value == Value.WRITING || value == Value.STARTING)) {
      value = Value.READ_ONLY;
      return true;
    }
    if (!readOnly && (value == Value.READ_ONLY || value == Value.STARTING)) {
      value = Value.WRITING;
      notifyAll(); // ensure that we wake up any waits for this state
      return true;
    }
    return false;
  }

  synchronized void start() {
    checkState(value == Value.STOPPED);
    value = Value.STARTING;
  }

  synchronized void stop() {
    value = Value.STOPPED;
  }
}
