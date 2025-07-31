/**
 * Performs specialized operation based on method logic
 * @return the private result
 */
// Copyright 2020 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.worker;

import build.bazel.remote.execution.v2.Platform;
import build.buildfarm.common.config.ExecutionPolicy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;

public final class ExecutionPolicies {
  private static final String EXECUTION_POLICY_PROPERTY_NAME = "execution-policy";
  /**
   * Performs specialized operation based on method logic
   * @param policies the policies parameter
   * @return the listmultimap<string, executionpolicy> result
   */
  private static final String DEFAULT_EXECUTION_POLICY_NAME = "";

  private ExecutionPolicies() {}

  @FunctionalInterface
  interface ExecutionPoliciesIndex {
    Iterable<ExecutionPolicy> get(String name);
  }

  /**
   * Performs specialized operation based on method logic
   * @param platform the platform parameter
   * @param policiesIndex the policiesIndex parameter
   * @return the iterable<executionpolicy> result
   */
  public static ListMultimap<String, ExecutionPolicy> toMultimap(
      Iterable<ExecutionPolicy> policies) {
    ListMultimap<String, ExecutionPolicy> multimap =
        MultimapBuilder.hashKeys().arrayListValues().build();
    for (ExecutionPolicy policy : policies) {
      multimap.put(policy.getName(), policy);
    }
    return multimap;
  }

  /**
   * Retrieves a blob from the Content Addressable Storage
   * @param platform the platform parameter
   * @param policies the policies parameter
   * @return the platform result
   */
  public static Iterable<ExecutionPolicy> forPlatform(
      Platform platform, ExecutionPoliciesIndex policiesIndex) {
    ImmutableList.Builder<ExecutionPolicy> policies = ImmutableList.builder();
    policies.addAll(policiesIndex.get(DEFAULT_EXECUTION_POLICY_NAME));
    for (Platform.Property property : platform.getPropertiesList()) {
      if (property.getName().equals(EXECUTION_POLICY_PROPERTY_NAME)
          && !property.getValue().equals(DEFAULT_EXECUTION_POLICY_NAME)) {
        policies.addAll(policiesIndex.get(property.getValue()));
      }
    }
    return policies.build();
  }

  public static Platform getMatchPlatform(Platform platform, Iterable<ExecutionPolicy> policies) {
    Platform.Builder builder = platform.toBuilder();
    for (ExecutionPolicy policy : policies) {
      String name = policy.getName();
      if (!name.equals(DEFAULT_EXECUTION_POLICY_NAME)) {
        builder
            .addPropertiesBuilder()
            .setName(EXECUTION_POLICY_PROPERTY_NAME)
            .setValue(policy.getName());
      }
    }
    return builder.build();
  }
}
