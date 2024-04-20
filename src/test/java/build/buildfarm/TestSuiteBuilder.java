// Copyright 2018 The Bazel Authors. All rights reserved.
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

import build.buildfarm.Classpath.ClassPathException;
import com.google.common.collect.Sets;
import java.lang.reflect.Modifier;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Set;
import junit.framework.TestCase;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@SuppressWarnings("PMD.TestClassWithoutTestCases")
final class TestSuiteBuilder {
  private static class TestClassNameComparator implements Comparator<Class<?>> {
    @Override
    public int compare(Class<?> o1, Class<?> o2) {
      return o1.getName().compareTo(o2.getName());
    }
  }

  private final Set<Class<?>> testClasses = Sets.newTreeSet(new TestClassNameComparator());

  /**
   * Adds the tests found (directly) in class {@code c} to the set of tests this builder will
   * search.
   */
  public TestSuiteBuilder addTestClass(Class<?> c) {
    testClasses.add(c);
    return this;
  }

  /**
   * Adds all the test classes (top-level or nested) found in package {@code pkgName} or its
   * subpackages to the set of tests this builder will search.
   */
  public TestSuiteBuilder addPackageRecursive(String pkgName) {
    for (Class<?> c : getClassesRecursive(pkgName)) {
      addTestClass(c);
    }
    return this;
  }

  private Set<Class<?>> getClassesRecursive(String pkgName) {
    Set<Class<?>> result = new LinkedHashSet<>();
    try {
      for (Class<?> clazz : Classpath.findClasses(pkgName)) {
        if (isTestClass(clazz)) {
          result.add(clazz);
        }
      }
    } catch (ClassPathException e) {
      throw new AssertionError("Cannot retrive classes: " + e.getMessage());
    }
    return result;
  }

  /**
   * Determines if a given class is a test class.
   *
   * @param container class to test
   * @return <code>true</code> if the test is a test class.
   */
  private static boolean isTestClass(Class<?> container) {
    return (isJunit4Test(container) || isJunit3Test(container))
        && !isSuite(container)
        && Modifier.isPublic(container.getModifiers())
        && !Modifier.isAbstract(container.getModifiers());
  }

  private static boolean isJunit4Test(Class<?> container) {
    return container.isAnnotationPresent(RunWith.class);
  }

  private static boolean isJunit3Test(Class<?> container) {
    return TestCase.class.isAssignableFrom(container);
  }

  /**
   * Classes that have a {@code RunWith} annotation for {@link Suite} or are automatically excluded
   * to avoid picking up the suite class itself.
   */
  private static boolean isSuite(Class<?> container) {
    RunWith runWith = container.getAnnotation(RunWith.class);
    return (runWith != null) && ((runWith.value() == CustomSuite.class));
  }

  /**
   * Creates and returns a TestSuite containing the tests from the given classes and/or packages.
   */
  public Set<Class<?>> create() {
    return testClasses;
  }
}
