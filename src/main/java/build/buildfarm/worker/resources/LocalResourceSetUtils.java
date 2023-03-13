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

package build.buildfarm.worker.resources;

import build.bazel.remote.execution.v2.Platform;
import build.buildfarm.common.config.LimitedResource;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import org.apache.commons.lang3.StringUtils;

/**
 * @class LocalResourceSetUtils
 * @brief Utilities for working with the worker's set of local limited resources.
 * @details The methods help with allocation / de-allocation of claims, as well as metrics printing.
 */
public class LocalResourceSetUtils {
  private static final LocalResourceSetMetrics metrics = new LocalResourceSetMetrics();

  public static LocalResourceSet create(List<LimitedResource> resources) {
    LocalResourceSet resourceSet = new LocalResourceSet();
    for (LimitedResource resource : resources) {
      resourceSet.resources.put(resource.getName(), new Semaphore(resource.getAmount()));
      metrics.resourceTotalMetric.labels(resource.getName()).set(resource.getAmount());
    }
    return resourceSet;
  }

  public static boolean claimResources(Platform platform, LocalResourceSet resourceSet) {
    List<Map.Entry<String, Integer>> claimed = new ArrayList<>();

    boolean allClaimed = true;
    for (Platform.Property property : platform.getPropertiesList()) {
      // Skip properties that are not requesting a limited resource.
      String resourceName = getResourceName(property);
      Semaphore resource = resourceSet.resources.get(resourceName);
      if (resource == null) {
        continue;
      }

      // Attempt to claim.  If claiming fails, we must return all other claims.
      int requestAmount = getResourceRequestAmount(property);
      boolean wasAcquired = semaphoreAquire(resource, resourceName, requestAmount);
      if (wasAcquired) {
        claimed.add(new AbstractMap.SimpleEntry<>(resourceName, requestAmount));
      } else {
        allClaimed = false;
        break;
      }
    }

    // cleanup remaining resources if they were not all claimed.
    if (!allClaimed) {
      for (Map.Entry<String, Integer> claim : claimed) {
        semaphoreRelease(
            resourceSet.resources.get(claim.getKey()), claim.getKey(), claim.getValue());
      }
    }

    return allClaimed;
  }

  public static void releaseClaims(Platform platform, LocalResourceSet resourceSet) {
    for (Platform.Property property : platform.getPropertiesList()) {
      String resourceName = getResourceName(property);
      Semaphore resource = resourceSet.resources.get(resourceName);
      if (resource == null) {
        continue;
      }
      int requestAmount = getResourceRequestAmount(property);
      semaphoreRelease(resource, resourceName, requestAmount);
    }
  }

  private static boolean semaphoreAquire(Semaphore resource, String resourceName, int amount) {
    boolean wasAcquired = resource.tryAcquire(amount);
    if (wasAcquired) {
      metrics.resourceUsageMetric.labels(resourceName).inc(amount);
    }
    metrics.requestersMetric.labels(resourceName).inc();
    return wasAcquired;
  }

  private static void semaphoreRelease(Semaphore resource, String resourceName, int amount) {
    resource.release(amount);
    metrics.resourceUsageMetric.labels(resourceName).dec(amount);
    metrics.requestersMetric.labels(resourceName).dec();
  }

  private static int getResourceRequestAmount(Platform.Property property) {
    // We support resource values that are not numbers and interpret them as a request for 1
    // resource.  For example "gpu:RTX-4090" is equivalent to resource:gpu:1".
    try {
      return Integer.parseInt(property.getValue());
    } catch (NumberFormatException e) {
      return 1;
    }
  }

  private static String getResourceName(Platform.Property property) {
    // We match to keys whether they are prefixed "resource:" or not.
    // "resource:gpu:1" requests the gpu resource in the same way that "gpu:1" does.
    // The prefix originates from bazel's syntax for the --extra_resources flag.
    return StringUtils.removeStart(property.getName(), "resource:");
  }
}
