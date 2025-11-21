// Copyright 2023-2025 The Buildfarm Authors. All rights reserved.
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

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * A Supervisor is an ObjectFactory with default `wrap` and `destroyObject` overrides, and abstract
 * `create` and `validateObject` methods.
 *
 * <p>An aside: Why does an ObjectFactory do more than create the object?
 *
 * @param <K>
 * @param <V> must be a Destructable
 */
public abstract class CommonsSupervisor<K, V extends Destructable>
    extends BaseKeyedPooledObjectFactory<K, V> implements Supervisor<K, V, PooledObject<V>> {
  private static final Logger logger = Logger.getLogger(CommonsSupervisor.class.getName());

  public abstract V create(K k) throws Exception;

  public abstract boolean validateObject(K key, PooledObject<V> p);

  protected Logger getLogger() {
    return logger;
  }

  @Override
  public PooledObject<V> wrap(V v) {
    return new DefaultPooledObject<>(v);
  }

  @Override
  public void destroyObject(K key, PooledObject<V> p) {
    V obj = p.getObject();

    var maybeOverriddenLogger = getLogger();
    final var logLevel = Level.FINE;
    if (maybeOverriddenLogger.isLoggable(logLevel)) {
      StringBuilder msgBuilder = new StringBuilder();
      msgBuilder.append("Supervisor.destroyObject() from key:\n");
      msgBuilder.append(key);
      msgBuilder.append("\nobj.toString():\n");
      msgBuilder.append(obj);
      msgBuilder.append("\nSupervisor.destroyObject() stackTrack:");
      for (StackTraceElement e : Thread.currentThread().getStackTrace()) {
        msgBuilder.append("\n\t");
        msgBuilder.append(e);
      }

      maybeOverriddenLogger.log(logLevel, msgBuilder.toString());
    }

    obj.destroy();
  }
}
