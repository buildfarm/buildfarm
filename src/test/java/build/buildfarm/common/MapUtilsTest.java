// Copyright 2021 The Buildfarm Authors. All rights reserved.
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * @class MapUtilsTest
 * @brief tests utility functions for working with Java maps.
 */
@RunWith(JUnit4.class)
public class MapUtilsTest {
  // Function under test: incrementValue
  // Reason for testing: empty map goes to 1
  // Failure explanation: increment value not as expected
  @Test
  public void incrementValueEmptyToOne() throws Exception {
    // ARRANGE
    Map<String, Integer> map = new HashMap<>();

    // ACT
    MapUtils.incrementValue(map, "foo");
    Integer result = map.get("foo");

    // ASSERT
    assertThat(result).isEqualTo(1);
  }

  // Function under test: incrementValue
  // Reason for testing: empty map can be incremented
  // Failure explanation: incrementing not working as expected
  @Test
  public void incrementValueEmptyIncrementThree() throws Exception {
    // ARRANGE
    Map<String, Integer> map = new HashMap<>();

    // ACT
    MapUtils.incrementValue(map, "foo");
    MapUtils.incrementValue(map, "foo");
    MapUtils.incrementValue(map, "foo");
    Integer result = map.get("foo");

    // ASSERT
    assertThat(result).isEqualTo(3);
  }

  // Function under test: incrementValue
  // Reason for testing: nonempty map can be incremented
  // Failure explanation: incrementing not working as expected
  @Test
  public void incrementValueNonEmptyIncrement() throws Exception {
    // ARRANGE
    Map<String, Integer> map = new HashMap<>();

    // ACT
    map.put("foo", 10);
    MapUtils.incrementValue(map, "foo");
    Integer result = map.get("foo");

    // ASSERT
    assertThat(result).isEqualTo(11);
  }

  // Function under test: toString
  // Reason for testing: empty map can be printed
  // Failure explanation: empty map not printed as expected
  @Test
  public void toStringEmpty() throws Exception {
    // ARRANGE
    Map<String, Integer> map = new HashMap<>();

    // ACT
    String result = MapUtils.toString(map);

    // ASSERT
    assertThat(result).isEqualTo("{}");
  }

  // Function under test: toString
  // Reason for testing: single element map can be printed
  // Failure explanation: map not printed as expected
  @Test
  public void toStringSingleElement() throws Exception {
    // ARRANGE
    Map<String, Integer> map = new HashMap<>();
    map.put("foo", 1);

    // ACT
    String result = MapUtils.toString(map);

    // ASSERT
    assertThat(result).isEqualTo("{foo=1}");
  }

  // Function under test: toString
  // Reason for testing: multiple element map can be printed
  // Failure explanation: map not printed as expected
  @Test
  public void toStringMultiElement() throws Exception {
    // ARRANGE
    Map<String, Integer> map = new HashMap<>();
    map.put("foo", 1);
    map.put("bar", 2);
    map.put("baz", 456);

    // ACT
    String result = MapUtils.toString(map);

    // ASSERT
    assertThat(result).isEqualTo("{bar=2, foo=1, baz=456}");
  }

  // Function under test: envMapToList
  // Reason for testing: empty maps produce empty lists
  // Failure explanation: edge case not met
  @Test
  public void envMapToListEmpty() throws Exception {
    // ARRANGE
    Map<String, String> map = new HashMap<>();

    // ACT
    List<String> result = MapUtils.envMapToList(map);

    // ASSERT
    assertThat(result).isEmpty();
  }

  // Function under test: envMapToList
  // Reason for testing: test conversions are in expected format
  // Failure explanation: conversion not correct
  @Test
  public void envMapToListConversion() throws Exception {
    // ARRANGE
    Map<String, String> map = new HashMap<>();
    map.put("KEY1", "VAL1");
    map.put("KEY2", "VAL2");
    map.put("KEY3", "VAL3");

    // ACT
    List<String> result = MapUtils.envMapToList(map);

    // ASSERT
    assertThat(result).contains("KEY1=VAL1");
    assertThat(result).contains("KEY2=VAL2");
    assertThat(result).contains("KEY3=VAL3");
  }
}
