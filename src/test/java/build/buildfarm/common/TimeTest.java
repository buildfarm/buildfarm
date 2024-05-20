// Copyright 2024 The Bazel Authors. All rights reserved.
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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.protobuf.Timestamp;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TimeTest {
  @Test
  public void toDurationMs() {
    Timestamp t1 = Timestamp.newBuilder().setSeconds(123).setNanos(100).build();
    Timestamp t2 = Timestamp.newBuilder().setSeconds(t1.getSeconds() + 120).setNanos(200).build();
    // t2 is 60sec+100ns after t1.
    // The result is rounded to the nearest millisecond.
    assertThat(Time.toDurationMs(t1, t2)).isEqualTo(120000.0);
  }

  @Test
  public void toDurationMsEndBeforeStart_Seconds() {
    // Seconds
    Timestamp t1 = Timestamp.newBuilder().setSeconds(123).setNanos(100).build();
    Timestamp t2 = Timestamp.newBuilder().setSeconds(111).setNanos(t1.getNanos()).build();
    // t2 is _before_ t1, which is a precondition failure
    assertThrows(IllegalArgumentException.class, () -> Time.toDurationMs(t1, t2));
  }

  @Test
  public void toDurationMsEndBeforeStart_Nanoseconds() {
    // Nanoseconds
    Timestamp t1 = Timestamp.newBuilder().setSeconds(123).setNanos(777).build();
    Timestamp t2 = Timestamp.newBuilder().setSeconds(t1.getSeconds()).setNanos(222).build();
    // t2 is _before_ t1, which is a precondition failure
    assertThrows(IllegalArgumentException.class, () -> Time.toDurationMs(t1, t2));
  }

  /** "Borrow" from the seconds units into 1e9 more nanoseconds. */
  @Test
  public void toDurationBorrow() {
    Timestamp t1 =
        Timestamp.newBuilder()
            .setSeconds(123)
            .setNanos(999999800)
            .build(); // 200 nanoseconds short of a full second.
    Timestamp t2 = Timestamp.newBuilder().setSeconds(125).setNanos(0).build();
    // The result is rounded to the nearest millisecond.
    assertThat(Time.toDurationMs(t1, t2)).isEqualTo(1000.0);
  }

  @Test
  public void toDurationMsEqual() {
    Timestamp t1 = Timestamp.newBuilder().setSeconds(123).setNanos(100).build();
    Timestamp t2 = Timestamp.newBuilder(t1).build();
    assertThat(t1).isNotSameInstanceAs(t2);
    assertThat(t1.getSeconds()).isEqualTo(t2.getSeconds());
    assertThat(t1.getNanos()).isEqualTo(t2.getNanos());
    assertThat(Time.toDurationMs(t1, t2)).isEqualTo(0);
  }
}
