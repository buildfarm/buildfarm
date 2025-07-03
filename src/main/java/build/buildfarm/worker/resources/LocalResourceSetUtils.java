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
import build.buildfarm.common.Claim;
import build.buildfarm.common.Claim.Lease;
import build.buildfarm.common.config.LimitedResource;
import build.buildfarm.worker.resources.LocalResourceSet.PoolResource;
import build.buildfarm.worker.resources.LocalResourceSet.SemaphoreResource;
import java.nio.file.attribute.UserPrincipal;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Semaphore;
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

  public static final class LocalResourceSetClaim implements Claim {
    private final Map<String, Lease> claimed;

    LocalResourceSetClaim(Map<String, Lease> claimed) {
      this.claimed = claimed;
    }

    @Override
    public synchronized void release(Stage stage) {
      // only remove resources that match the specified stage
      // O(n) search for staged resources
      Iterator<Map.Entry<String, Lease>> leaseIterator = claimed.entrySet().iterator();
      while (leaseIterator.hasNext()) {
        Lease lease = leaseIterator.next().getValue();
        if (lease.getStage() != stage) {
          continue;
        }
        leaseIterator.remove();
        lease.release();
      }
    }

    // safe for multiple sequential calls with removal to release
    @Override
    public synchronized void release() {
      Iterator<Map.Entry<String, Lease>> leaseIterator = claimed.entrySet().iterator();

      while (leaseIterator.hasNext()) {
        Lease lease = leaseIterator.next().getValue();

        leaseIterator.remove();
        lease.release();
      }
    }

    @Override
    public UserPrincipal owner() {
      return null;
    }

    @Override
    public Iterable<Entry<String, List<Object>>> getPools() {
      return claimed.entrySet().stream()
          .filter(entry -> entry.getValue() instanceof LocalResourceSet.PoolLease)
          .<Entry<String, List<Object>>>map(
              entry -> {
                LocalResourceSet.PoolLease poolLease =
                    (LocalResourceSet.PoolLease) entry.getValue();

                return new SimpleEntry<>(entry.getKey(), poolLease.claims());
              })
          .toList();
    }
  }

  public static LocalResourceSet create(Iterable<LimitedResource> resources) {
    LocalResourceSet resourceSet = new LocalResourceSet();
    for (LimitedResource resource : resources) {
      LocalResource localResource;

      switch (resource.getType()) {
        case SEMAPHORE:
          localResource =
              new SemaphoreResource(
                  new Semaphore(resource.getAmount()), resource.getReleaseStage());
          break;
        case POOL:
          localResource =
              new PoolResource(
                  new ArrayDeque<Object>(IntStream.range(0, resource.getAmount()).boxed().toList()),
                  resource.getReleaseStage());
          break;
        default:
          throw new IllegalStateException("unrecognized resource type: " + resource.getType());
      }
      resourceSet.resources.put(resource.getName(), localResource);
      metrics.resourceTotalMetric.labels(resource.getName()).set(resource.getAmount());
    }
    return resourceSet;
  }

  public static Set<String> exhausted(LocalResourceSet resourceSet) {
    Set<String> exhausted = new HashSet<>();
    for (Entry<String, LocalResource> resource : resourceSet.resources.entrySet()) {
      if (resource.getValue().available() == 0) {
        exhausted.add(resource.getKey());
      }
    }
    return exhausted;
  }

  public static @Nullable Claim claimResources(Platform platform, LocalResourceSet resourceSet) {
    Map<String, Lease> claimed = new HashMap<>();

    boolean allClaimed = true;
    for (Platform.Property property : platform.getPropertiesList()) {
      String resourceName = getResourceName(property.getName());
      int requestAmount = getResourceRequestAmount(property);
      if (resourceSet.resources.containsKey(resourceName)) {
        LocalResource resource = resourceSet.resources.get(resourceName);

        // Attempt to claim.  If claiming fails, we must return all other claims.
        Optional<Lease> lease = resource.tryAcquire(requestAmount);
        if (lease.isPresent()) {
          claimed.put(resourceName, lease.get());

          metrics.resourceUsageMetric.labels(resourceName).inc(requestAmount);
          metrics.requestersMetric.labels(resourceName).inc();
        } else {
          allClaimed = false;
          break;
        }
      }
    }

    // cleanup remaining resources if they were not all claimed.
    if (!allClaimed) {
      for (Map.Entry<String, Lease> entry : claimed.entrySet()) {
        String resourceName = entry.getKey();

        Lease lease = entry.getValue();

        lease.release();

        metrics.resourceUsageMetric.labels(resourceName).dec(lease.getAmount());
        metrics.requestersMetric.labels(resourceName).dec();
      }
    }

    return allClaimed ? new LocalResourceSetClaim(claimed) : null;
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
    return name.startsWith("resource:") && resourceSet.resources.containsKey(getResourceName(name));
  }
}
