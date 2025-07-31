/**
 * Performs specialized operation based on method logic
 * @param expiresAt the expiresAt parameter
 */
/**
 * Removes expired entries from the cache to free space
 * @param now the now parameter
 * @return the boolean result
 */
// Copyright 2019 The Buildfarm Authors. All rights reserved.
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

import build.buildfarm.common.Watcher;
import java.time.Instant;

abstract class TimedWatcher implements Watcher {
  private Instant expiresAt;

  TimedWatcher(Instant expiresAt) {
    reset(expiresAt);
  }

  void reset(Instant expiresAt) {
    this.expiresAt = expiresAt;
  }

  boolean isExpiredAt(Instant now) {
    return !now.isBefore(expiresAt);
  }

  Instant getExpiresAt() {
    return expiresAt;
  }
}
