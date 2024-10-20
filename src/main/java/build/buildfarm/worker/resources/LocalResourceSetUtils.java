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

package build.buildfarm.worker.resources;

import build.bazel.remote.execution.v2.Platform;
import build.buildfarm.common.config.LimitedResource;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * @class LocalResourceSetUtils
 * @brief Utilities for working with the worker's set of local limited resources.
 * @details The methods help with allocation / de-allocation of claims, as well as metrics printing.
 */
public class LocalResourceSetUtils {
  private static final LocalResourceSetMetrics metrics = new LocalResourceSetMetrics();

  public static final class LocalResourceSetClaim implements Claim {
    private final List<Entry<String, Integer>> semaphores;
    @Getter private final List<Entry<String, List<Integer>>> pools;
    private final LocalResourceSet resourceSet;

    LocalResourceSetClaim(
        List<Entry<String, Integer>> semaphores,
        List<Entry<String, List<Integer>>> pools,
        LocalResourceSet resourceSet) {
      this.semaphores = semaphores;
      this.pools = pools;
      this.resourceSet = resourceSet;
    }

    // safe for multiple sequential calls with removal to release
    @Override
    public synchronized void release() {
      while (!semaphores.isEmpty()) {
        Entry<String, Integer> entry = semaphores.removeFirst();
        String resourceName = entry.getKey();
        Semaphore resource = resourceSet.resources.get(resourceName);
        semaphoreRelease(resource, resourceName, entry.getValue());
      }

      while (!pools.isEmpty()) {
        Entry<String, List<Integer>> entry = pools.removeFirst();
        String resourceName = entry.getKey();
        Queue<Integer> resource = resourceSet.poolResources.get(resourceName);
        poolRelease(resource, resourceName, entry.getValue());
      }
    }
  }

  public static LocalResourceSet create(List<LimitedResource> resources) {
    LocalResourceSet resourceSet = new LocalResourceSet();
    for (LimitedResource resource : resources) {
      switch (resource.getType()) {
        case SEMAPHORE:
          resourceSet.resources.put(resource.getName(), new Semaphore(resource.getAmount()));
          break;
        case POOL:
          resourceSet.poolResources.put(
              resource.getName(),
              new ArrayDeque(IntStream.range(0, resource.getAmount()).boxed().toList()));
          break;
        default:
          throw new IllegalStateException("unrecognized resource type: " + resource.getType());
      }
      metrics.resourceTotalMetric.labels(resource.getName()).set(resource.getAmount());
    }
    return resourceSet;
  }

  public static @Nullable Claim claimResources(Platform platform, LocalResourceSet resourceSet) {
    List<Entry<String, Integer>> semaphoreClaimed = new ArrayList<>();
    List<Entry<String, List<Integer>>> poolClaimed = new ArrayList<>();

    boolean allClaimed = true;
    for (Platform.Property property : platform.getPropertiesList()) {
      String resourceName = getResourceName(property.getName());
      int requestAmount = getResourceRequestAmount(property);
      if (resourceSet.resources.containsKey(resourceName)) {
        Semaphore resource = resourceSet.resources.get(resourceName);

        // Attempt to claim.  If claiming fails, we must return all other claims.
        boolean wasAcquired = semaphoreAquire(resource, resourceName, requestAmount);
        if (wasAcquired) {
          semaphoreClaimed.add(new SimpleEntry<>(resourceName, requestAmount));
        } else {
          allClaimed = false;
          break;
        }
      } else if (resourceSet.poolResources.containsKey(resourceName)) {
        Queue<Integer> resource = resourceSet.poolResources.get(resourceName);
        List<Integer> claimed = new ArrayList<>();
        poolClaimed.add(new SimpleEntry<>(resourceName, claimed));

        // Attempt to claim.  If claiming fails, we must return all other claims.
        if (!poolAcquire(resource, resourceName, requestAmount, claimed::add)) {
          allClaimed = false;
          break;
        }
      }
    }

    // cleanup remaining resources if they were not all claimed.
    if (!allClaimed) {
      for (Entry<String, Integer> claim : semaphoreClaimed) {
        semaphoreRelease(
            resourceSet.resources.get(claim.getKey()), claim.getKey(), claim.getValue());
      }
      for (Entry<String, List<Integer>> claim : poolClaimed) {
        poolRelease(
            resourceSet.poolResources.get(claim.getKey()), claim.getKey(), claim.getValue());
      }
    }

    return allClaimed
        ? new LocalResourceSetClaim(semaphoreClaimed, poolClaimed, resourceSet)
        : null;
  }

  private static boolean semaphoreAquire(Semaphore resource, String resourceName, int amount) {
    boolean wasAcquired = resource.tryAcquire(amount);
    if (wasAcquired) {
      metrics.resourceUsageMetric.labels(resourceName).inc(amount);
    }
    metrics.requestersMetric.labels(resourceName).inc();
    return wasAcquired;
  }

  private static boolean poolAcquire(
      Queue<Integer> resource, String resourceName, int amount, Consumer<Integer> onClaimed) {
    metrics.requestersMetric.labels(resourceName).inc();
    for (int i = 0; i < amount; i++) {
      Integer id = resource.poll();
      if (id == null) {
        return false;
      }
      // still don't like this
      onClaimed.accept(id);
    }
    // only records when fully acquired
    metrics.resourceUsageMetric.labels(resourceName).inc(amount);
    return true;
  }

  private static void semaphoreRelease(Semaphore resource, String resourceName, int amount) {
    resource.release(amount);
    metrics.resourceUsageMetric.labels(resourceName).dec(amount);
    metrics.requestersMetric.labels(resourceName).dec();
  }

  private static void poolRelease(
      Queue<Integer> resource, String resourceName, List<Integer> claims) {
    claims.forEach(resource::add);
    metrics.resourceUsageMetric.labels(resourceName).dec(claims.size());
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

  private static String getResourceName(String propertyName) {
    // We match to keys whether they are prefixed "resource:" or not.
    // "resource:gpu:1" requests the gpu resource in the same way that "gpu:1" does.
    // The prefix originates from bazel's syntax for the --extra_resources flag.
    return StringUtils.removeStart(propertyName, "resource:");
  }

  public static boolean satisfies(LocalResourceSet resourceSet, Platform.Property property) {
    String name = property.getName();
    return name.startsWith("resource:")
        && (resourceSet.resources.containsKey(getResourceName(name))
            || resourceSet.poolResources.containsKey(getResourceName(name)));
  }
}
