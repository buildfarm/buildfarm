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

package build.buildfarm.common.redis;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

///
/// @class   RedisMapMockTest
/// @brief   tests A redis map.
/// @details A redis map is an implementation of a map data structure which
///          internally uses redis to store and distribute the data. Its
///          important to know that the lifetime of the map persists before
///          and after the map data structure is created (since it exists in
///          redis). Therefore, two redis maps with the same name, would in
///          fact be the same underlying redis map.
///
@RunWith(JUnit4.class)
public class RedisMapMockTest {

  // Function under test: redisMap
  // Reason for testing: the map can be constructed with a valid cluster instance and name
  // Failure explanation: the map is throwing an exception upon construction
  @Test
  public void redisMapConstructsWithoutError() throws Exception {

    // ARRANGE
    RedisMap map = new RedisMap("test");
  }
}
