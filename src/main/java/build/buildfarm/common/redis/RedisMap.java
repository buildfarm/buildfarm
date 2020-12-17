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


///
/// @class   RedisMap
/// @brief   A redis map.
/// @details A redis map is an implementation of a map data structure which
///          internally uses redis to store and distribute the data. Its
///          important to know that the lifetime of the map persists before
///          and after the map data structure is created (since it exists in
///          redis). Therefore, two redis maps with the same name, would in
///          fact be the same underlying redis map.
///
public class RedisMap {

  ///
  /// @field   name
  /// @brief   The unique name of the map.
  /// @details The name is used by the redis cluster client to access the map
  ///          data. If two maps had the same name, they would be instances of
  ///          the same underlying redis map.
  ///
  private final String name;

  ///
  /// @brief   Constructor.
  /// @details Construct a named redis map with an established redis cluster.
  /// @param   name The global name of the map.
  ///
  public RedisMap(String name) {
    this.name = name;
  }
}
