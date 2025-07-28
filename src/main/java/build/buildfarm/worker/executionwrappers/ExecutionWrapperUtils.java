// Copyright 2023 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.worker.executionwrappers;

import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Maps.uniqueIndex;

import build.bazel.remote.execution.v2.Platform.Property;
import build.buildfarm.common.Claim;
import build.buildfarm.common.config.ExecutionPolicy;
import build.buildfarm.common.config.ExecutionWrapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class ExecutionWrapperUtils {
  private static final class Interpolator {
    private final Iterable<?> value;

    Interpolator(String value) {
      this(ImmutableList.of(value));
    }

    Interpolator(Iterable<?> value) {
      this.value = value;
    }

    void inject(Consumer<String> consumer) {
      Iterables.transform(value, String::valueOf).forEach(consumer);
    }
  }

  private static Map<String, Interpolator> createInterpolations(
      Claim claim, Iterable<Property> properties) {
    Map<String, Interpolator> interpolations = new HashMap<>();
    if (claim != null) {
      for (Map.Entry<String, List<?>> pool : claim.getPools()) {
        List<?> ids = pool.getValue();
        interpolations.put(pool.getKey(), new Interpolator(ids));
        for (int i = 0; i < ids.size(); i++) {
          interpolations.put(pool.getKey() + "-" + i, new Interpolator(String.valueOf(ids.get(i))));
        }
      }
    }
    interpolations.putAll(
        transformValues(
            uniqueIndex(properties, Property::getName),
            property -> new Interpolator(property.getValue())));
    return interpolations;
  }

  private static Iterable<String> transformWrapper(
      ExecutionWrapper wrapper, Map<String, Interpolator> interpolations) {
    ImmutableList.Builder<String> arguments = ImmutableList.builder();

    arguments.add(wrapper.getPath());

    if (wrapper.getArguments() != null) {
      for (String argument : wrapper.getArguments()) {
        // If the argument is of the form <propertyName>, substitute the value of
        // the property from the platform specification.
        if (!argument.equals("<>")
            && argument.charAt(0) == '<'
            && argument.charAt(argument.length() - 1) == '>') {
          // substitute with matching interpolation
          // if this property is not present, the wrapper is ignored
          String name = argument.substring(1, argument.length() - 1);
          Interpolator interpolator = interpolations.get(name);
          if (interpolator == null) {
            return ImmutableList.of();
          }
          interpolator.inject(arguments::add);
        } else {
          // If the argument isn't of the form <propertyName>, add the argument directly:
          arguments.add(argument);
        }
      }
    }

    return arguments.build();
  }

  public static ImmutableList<String> evaluateExecutionWrappers(
      Iterable<ExecutionPolicy> policies, Claim claim, Iterable<Property> platformProperties) {
    ImmutableList.Builder<String> arguments = ImmutableList.builder();
    Map<String, Interpolator> interpolations = createInterpolations(claim, platformProperties);

    for (ExecutionPolicy policy : policies) {
      if (policy.getExecutionWrapper() != null) {
        arguments.addAll(transformWrapper(policy.getExecutionWrapper(), interpolations));
      }
    }

    return arguments.build();
  }
}
