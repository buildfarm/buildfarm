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

import static com.google.common.truth.Truth.assertThat;

import java.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TimedWatcherTest {
  @Test
  public void expiresNowIsExpiredNow() {
    Instant now = Instant.now();
    TimedWatcher timedWatcher = new UnobservableWatcher(now);
    assertThat(timedWatcher.isExpiredAt(now)).isTrue();
  }

  @Test
  public void expiresEarlyIsExpiredNow() {
    TimedWatcher timedWatcher = new UnobservableWatcher(Instant.EPOCH);
    assertThat(timedWatcher.isExpiredAt(Instant.now())).isTrue();
  }

  @Test
  public void expiresLaterIsNotExpiredNow() {
    TimedWatcher timedWatcher = new UnobservableWatcher(Instant.MAX);
    assertThat(timedWatcher.isExpiredAt(Instant.now())).isFalse();
  }

  @Test
  public void expiresReflectsReset() {
    TimedWatcher timedWatcher = new UnobservableWatcher(Instant.EPOCH);
    assertThat(timedWatcher.getExpiresAt()).isEqualTo(Instant.EPOCH);
    Instant now = Instant.now();
    timedWatcher.reset(now);
    assertThat(timedWatcher.getExpiresAt()).isEqualTo(now);
    timedWatcher.reset(Instant.MAX);
    assertThat(timedWatcher.getExpiresAt()).isEqualTo(Instant.MAX);
  }
}
