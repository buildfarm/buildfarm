// Copyright 2026 The Buildfarm Authors. All rights reserved.
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

import static java.lang.String.format;

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
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

    Logger maybeOverriddenLogger = getLogger();
    final Level logLevel = Level.FINE;
    if (maybeOverriddenLogger.isLoggable(logLevel)) {
      String trace =
          Arrays.stream(Thread.currentThread().getStackTrace())
              .map(e -> "\n\t" + e)
              .collect(Collectors.joining());
      maybeOverriddenLogger.log(
          logLevel,
          format(
              "Supervisor.destroyObject() from key:\n"
                  + " %s\n"
                  + "obj.toString():\n"
                  + "%s\n"
                  + "Supervisor.destroyObject() stackTrace:%s",
              key, obj, trace));
    }

    obj.destroy();
  }
}
