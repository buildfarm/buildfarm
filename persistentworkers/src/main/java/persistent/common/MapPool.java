// Copyright 2023-2024 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package persistent.common;

import java.util.HashMap;
import java.util.function.Function;

public class MapPool<K, V> implements ObjectPool<K, V> {
  private final HashMap<K, V> map;

  private final Function<K, V> objFactory;

  public MapPool(Function<K, V> factory) {
    this.map = new HashMap<>();
    this.objFactory = factory;
  }

  @Override
  public V obtain(K key) {
    if (map.containsKey(key)) {
      return map.remove(key);
    }
    return objFactory.apply(key);
  }

  @Override
  public void release(K key, V obj) {
    map.put(key, obj);
  }
}
