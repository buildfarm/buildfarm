// Copyright 2020 The Bazel Authors. All rights reserved.
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

import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import io.grpc.Deadline;
import java.util.concurrent.TimeUnit;

///
/// @class   Time
/// @brief   Utilities related to time, durations, deadlines, timeouts, etc.
/// @details Contains converters between different time data types.
///
public class Time {

  ///
  /// @brief   Convert a protobuf duration to a grpc deadline.
  /// @details Deadline will have nanosecond precision.
  /// @param   duration A protobuf duration.
  /// @return  A converted grpc deadline.
  /// @note    Suggested return identifier: deadline.
  ///
  public static Deadline toDeadline(Duration duration) {
    return Deadline.after(
        secondsToNanoseconds(duration.getSeconds()) + duration.getNanos(), TimeUnit.NANOSECONDS);
  }
  ///
  /// @brief   Convert a grpc deadline to a protobuf duration.
  /// @details Duration will have nanosecond precision.
  /// @param   deadline A converted grpc deadline.
  /// @return  A protobuf duration.
  /// @note    Suggested return identifier: duration.
  ///
  public static Duration toDuration(Deadline deadline) {
    return Durations.fromNanos(deadline.timeRemaining(TimeUnit.NANOSECONDS));
  }
  ///
  /// @brief   Seconds to nanoseconds.
  /// @details Seconds to nanoseconds.
  /// @param   seconds Seconds to convert.
  /// @return  Nanoseconds converted from seconds.
  /// @note    Suggested return identifier: nanoseconds.
  ///
  public static long secondsToNanoseconds(long seconds) {
    return seconds * 1000000000;
  }
}
