// Copyright 2014 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Set;
import org.junit.runners.Suite;
import org.junit.runners.model.RunnerBuilder;

/**
 * A JUnit4 suite implementation that delegates the class finding to a {@code suite()} method on the
 * annotated class. To be used in combination with {@link TestSuiteBuilder}.
 */
public final class CustomSuite extends Suite {
  /** Only called reflectively. Do not use programmatically. */
  public CustomSuite(Class<?> klass, RunnerBuilder builder) throws Throwable {
    super(builder, klass, getClasses(klass));
  }

  private static Class<?>[] getClasses(Class<?> klass) {
    Set<Class<?>> result = evalSuite(klass);
    return result.toArray(new Class<?>[0]);
  }

  @SuppressWarnings("unchecked") // unchecked cast to a generic type
  private static Set<Class<?>> evalSuite(Class<?> klass) {
    try {
      Method m = klass.getMethod("suite");
      if (!Modifier.isStatic(m.getModifiers())) {
        throw new IllegalStateException("suite() must be static");
      }
      return (Set<Class<?>>) m.invoke(null);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
