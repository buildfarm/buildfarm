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

import static com.google.common.collect.Iterables.transform;

import build.bazel.remote.execution.v2.Platform;
import build.buildfarm.common.Claim;
import build.buildfarm.common.config.LimitedResource;
import build.buildfarm.worker.resources.LocalResourceSet.PoolResource;
import build.buildfarm.worker.resources.LocalResourceSet.SemaphoreResource;
import java.nio.file.attribute.UserPrincipal;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/**
 * @class LocalResourceSetUtils
 * @brief Utilities for working with the worker's set of local limited resources.
 * @details The methods help with allocation / de-allocation of claims, as well as metrics printing.
 */
public class LocalResourceSetUtils {
  private static final LocalResourceSetMetrics metrics = new LocalResourceSetMetrics();

  private record SemaphoreLease(String name, int amount, Claim.Stage stage) {}

  private record PoolLease(String name, List<Object> claims, Claim.Stage stage) {}

  public static final class LocalResourceSetClaim implements Claim {
    private final List<SemaphoreLease> semaphores;
    private final List<PoolLease> pools;
    private final LocalResourceSet resourceSet;

    LocalResourceSetClaim(
        List<SemaphoreLease> semaphores, List<PoolLease> pools, LocalResourceSet resourceSet) {
      this.semaphores = semaphores;
      this.pools = pools;
      this.resourceSet = resourceSet;
    }

    @Override
    public synchronized void release(Stage stage) {
      // only remove resources that match the specified stage
      // O(n) search for staged resources
      Iterator<SemaphoreLease> semaphoreIter = semaphores.iterator();
      while (semaphoreIter.hasNext()) {
        SemaphoreLease semaphore = semaphoreIter.next();
        if (semaphore.stage() != stage) {
          continue;
        }
        semaphoreIter.remove();
        String resourceName = semaphore.name();
        SemaphoreResource resource = resourceSet.resources.get(resourceName);
        semaphoreRelease(resource.semaphore(), resourceName, semaphore.amount());
      }
      Iterator<PoolLease> poolIter = pools.iterator();
      while (poolIter.hasNext()) {
        PoolLease pool = poolIter.next();
        if (pool.stage() != stage) {
          continue;
        }
        poolIter.remove();
        String resourceName = pool.name();
        PoolResource resource = resourceSet.poolResources.get(resourceName);
        poolRelease(resource.pool(), resourceName, pool.claims());
      }
    }

    // safe for multiple sequential calls with removal to release
    @Override
    public synchronized void release() {
      while (!semaphores.isEmpty()) {
        SemaphoreLease semaphore = semaphores.removeFirst();
        String resourceName = semaphore.name();
        SemaphoreResource resource = resourceSet.resources.get(resourceName);
        semaphoreRelease(resource.semaphore(), resourceName, semaphore.amount());
      }

      while (!pools.isEmpty()) {
        PoolLease pool = pools.removeFirst();
        String resourceName = pool.name();
        PoolResource resource = resourceSet.poolResources.get(resourceName);
        poolRelease(resource.pool(), resourceName, pool.claims());
      }
    }

    @Override
    public UserPrincipal owner() {
      return null;
    }

    @Override
    public Iterable<Entry<String, List<Object>>> getPools() {
      return transform(pools, pool -> new SimpleEntry<>(pool.name(), pool.claims()));
    }
  }

  public static LocalResourceSet create(Iterable<LimitedResource> resources) {
    LocalResourceSet resourceSet = new LocalResourceSet();
    for (LimitedResource resource : resources) {
      switch (resource.getType()) {
        case SEMAPHORE:
          resourceSet.resources.put(
              resource.getName(),
              new SemaphoreResource(
                  new Semaphore(resource.getAmount()), resource.getReleaseStage()));
          break;
        case POOL:
          resourceSet.poolResources.put(
              resource.getName(),
              new PoolResource(
                  new ArrayDeque<Object>(IntStream.range(0, resource.getAmount()).boxed().toList()),
                  resource.getReleaseStage()));
          break;
        default:
          throw new IllegalStateException("unrecognized resource type: " + resource.getType());
      }
      metrics.resourceTotalMetric.labels(resource.getName()).set(resource.getAmount());
    }
    return resourceSet;
  }

  public static Set<String> exhausted(LocalResourceSet resourceSet) {
    Set<String> exhausted = new HashSet<>();
    for (Entry<String, SemaphoreResource> resource : resourceSet.resources.entrySet()) {
      if (resource.getValue().semaphore().availablePermits() == 0) {
        exhausted.add(resource.getKey());
      }
    }
    for (Entry<String, PoolResource> resource : resourceSet.poolResources.entrySet()) {
      if (resource.getValue().pool().isEmpty()) {
        exhausted.add(resource.getKey());
      }
    }
    return exhausted;
  }

  public static @Nullable Claim claimResources(Platform platform, LocalResourceSet resourceSet) {
    List<SemaphoreLease> semaphoreClaimed = new ArrayList<>();
    List<PoolLease> poolClaimed = new ArrayList<>();

    boolean allClaimed = true;
    for (Platform.Property property : platform.getPropertiesList()) {
      String resourceName = getResourceName(property.getName());
      int requestAmount = getResourceRequestAmount(property);
      if (resourceSet.resources.containsKey(resourceName)) {
        SemaphoreResource resource = resourceSet.resources.get(resourceName);

        // Attempt to claim.  If claiming fails, we must return all other claims.
        boolean wasAcquired = semaphoreAquire(resource.semaphore(), resourceName, requestAmount);
        if (wasAcquired) {
          semaphoreClaimed.add(new SemaphoreLease(resourceName, requestAmount, resource.stage()));
        } else {
          allClaimed = false;
          break;
        }
      } else if (resourceSet.poolResources.containsKey(resourceName)) {
        PoolResource resource = resourceSet.poolResources.get(resourceName);
        List<Object> claimed = new ArrayList<>();
        poolClaimed.add(new PoolLease(resourceName, claimed, resource.stage()));

        // Attempt to claim.  If claiming fails, we must return all other claims.
        if (!poolAcquire(resource.pool(), resourceName, requestAmount, claimed::add)) {
          allClaimed = false;
          break;
        }
      }
    }

    // cleanup remaining resources if they were not all claimed.
    if (!allClaimed) {
      for (SemaphoreLease semaphore : semaphoreClaimed) {
        semaphoreRelease(
            resourceSet.resources.get(semaphore.name()).semaphore(),
            semaphore.name(),
            semaphore.amount());
      }
      for (PoolLease pool : poolClaimed) {
        poolRelease(resourceSet.poolResources.get(pool.name()).pool(), pool.name(), pool.claims());
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
      metrics.requestersMetric.labels(resourceName).inc();
    }
    return wasAcquired;
  }

  private static boolean poolAcquire(
      Queue<Object> resource, String resourceName, int amount, Consumer<Object> onClaimed) {
    for (int i = 0; i < amount; i++) {
      Object id = resource.poll();
      if (id == null) {
        return false;
      }
      // still don't like this
      onClaimed.accept(id);
    }
    // only records when fully acquired
    metrics.resourceUsageMetric.labels(resourceName).inc(amount);
    metrics.requestersMetric.labels(resourceName).inc();
    return true;
  }

  private static void semaphoreRelease(Semaphore resource, String resourceName, int amount) {
    resource.release(amount);
    metrics.resourceUsageMetric.labels(resourceName).dec(amount);
    metrics.requestersMetric.labels(resourceName).dec();
  }

  private static void poolRelease(
      Queue<Object> resource, String resourceName, List<Object> claims) {
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

  public static String getResourceName(String propertyName) {
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
