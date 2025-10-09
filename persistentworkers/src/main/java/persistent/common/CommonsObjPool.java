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

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;

// ObjectPool which uses Apache Commons
public abstract class CommonsObjPool<K, V> extends GenericKeyedObjectPool<K, V>
    implements ObjectPool<K, V> {
  public CommonsObjPool(
      KeyedPooledObjectFactory<K, V> factory, GenericKeyedObjectPoolConfig<V> config) {
    super(factory, config);
  }

  public V obtain(K key) throws Exception {
    return this.borrowObject(key);
  }

  public void release(K key, V obj) {
    this.returnObject(key, obj);
  }
}
