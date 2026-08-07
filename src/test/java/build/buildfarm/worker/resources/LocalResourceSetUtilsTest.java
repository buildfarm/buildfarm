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

package build.buildfarm.worker.resources;

import static build.buildfarm.common.Claim.Stage.EXECUTE_ACTION_STAGE;
import static com.google.common.truth.Truth.assertThat;

import build.bazel.remote.execution.v2.Platform;
import build.buildfarm.common.Claim;
import build.buildfarm.common.config.LimitedResource;
import build.buildfarm.worker.resources.LocalResourceSet.SemaphoreResource;
import com.google.common.collect.ImmutableList;
import java.util.concurrent.Semaphore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * @class LocalResourceSetUtilsTest
 * @brief Tests how local resources are claimed and released.
 * @details Shows behaviour of local resource claims and releases.
 */
@RunWith(JUnit4.class)
public class LocalResourceSetUtilsTest {
  // Function under test: releaseClaims
  // Reason for testing: Show its okay to return claims that were never taken.
  // Failure explanation: can't return claims that were never taken.
  @Test
  public void multipleClaimReleaseHasNoEffect() throws Exception {
    // ARRANGE
    LocalResourceSet resourceSet = new LocalResourceSet();
    Semaphore foo = new Semaphore(2);
    foo.acquire(); // create room to put two resources back on if malfunctioning
    resourceSet.resources.put("FOO", new SemaphoreResource(foo, EXECUTE_ACTION_STAGE));

    Platform platform =
        Platform.newBuilder()
            .addProperties(Platform.Property.newBuilder().setName("resource:FOO").setValue("1"))
            .build();

    // ACT
    Claim claim = LocalResourceSetUtils.claimResources(platform, resourceSet);
    assertThat(foo.availablePermits()).isEqualTo(0);
    claim.release();
    assertThat(foo.availablePermits()).isEqualTo(1);
    claim.release();
    assertThat(foo.availablePermits()).isEqualTo(1);
  }

  private static LimitedResource resource(String name, int amount, LimitedResource.Type type) {
    LimitedResource resource = new LimitedResource();
    resource.setName(name);
    resource.setAmount(amount);
    resource.setType(type);
    return resource;
  }

  private static Platform platformFor(String propertyName, String value) {
    return Platform.newBuilder()
        .addProperties(Platform.Property.newBuilder().setName(propertyName).setValue(value))
        .build();
  }

  @Test
  public void createBuildsSemaphoreAndPoolResources() {
    LocalResourceSet set =
        LocalResourceSetUtils.create(
            ImmutableList.of(
                resource("sem", 3, LimitedResource.Type.SEMAPHORE),
                resource("pool", 2, LimitedResource.Type.POOL)));

    assertThat(set.resources).containsKey("sem");
    assertThat(set.resources.get("sem").semaphore().availablePermits()).isEqualTo(3);
    assertThat(set.poolResources).containsKey("pool");
    assertThat(set.poolResources.get("pool").pool()).hasSize(2);
  }

  @Test
  public void exhaustedReportsDepletedResources() {
    LocalResourceSet set =
        LocalResourceSetUtils.create(
            ImmutableList.of(
                resource("free", 1, LimitedResource.Type.SEMAPHORE),
                resource("used", 1, LimitedResource.Type.SEMAPHORE),
                resource("drained", 1, LimitedResource.Type.POOL)));

    // deplete "used" and "drained" but leave "free" available
    set.resources.get("used").semaphore().acquireUninterruptibly();
    set.poolResources.get("drained").pool().poll();

    assertThat(LocalResourceSetUtils.exhausted(set)).containsExactly("used", "drained");
  }

  @Test
  public void claimResourcesAcquiresFromPool() {
    LocalResourceSet set =
        LocalResourceSetUtils.create(
            ImmutableList.of(resource("gpu", 2, LimitedResource.Type.POOL)));

    Claim claim = LocalResourceSetUtils.claimResources(platformFor("resource:gpu", "1"), set);

    assertThat(claim).isNotNull();
    assertThat(set.poolResources.get("gpu").pool()).hasSize(1);
    claim.release();
    assertThat(set.poolResources.get("gpu").pool()).hasSize(2);
  }

  @Test
  public void claimResourcesReturnsNullWhenSemaphoreInsufficient() {
    LocalResourceSet set =
        LocalResourceSetUtils.create(
            ImmutableList.of(resource("cpu", 1, LimitedResource.Type.SEMAPHORE)));

    // request 2 from a resource that only has 1
    Claim claim = LocalResourceSetUtils.claimResources(platformFor("resource:cpu", "2"), set);

    assertThat(claim).isNull();
    // the failed claim must not leak permits
    assertThat(set.resources.get("cpu").semaphore().availablePermits()).isEqualTo(1);
  }

  @Test
  public void claimResourcesRollsBackPartialClaimOnFailure() {
    LocalResourceSet set =
        LocalResourceSetUtils.create(
            ImmutableList.of(
                resource("first", 1, LimitedResource.Type.SEMAPHORE),
                resource("second", 1, LimitedResource.Type.SEMAPHORE)));

    // first is satisfiable, second requests more than available -> whole claim fails and rolls back
    Platform platform =
        Platform.newBuilder()
            .addProperties(Platform.Property.newBuilder().setName("resource:first").setValue("1"))
            .addProperties(Platform.Property.newBuilder().setName("resource:second").setValue("5"))
            .build();

    Claim claim = LocalResourceSetUtils.claimResources(platform, set);

    assertThat(claim).isNull();
    assertThat(set.resources.get("first").semaphore().availablePermits()).isEqualTo(1);
    assertThat(set.resources.get("second").semaphore().availablePermits()).isEqualTo(1);
  }

  @Test
  public void nonNumericResourceValueClaimsOne() {
    LocalResourceSet set =
        LocalResourceSetUtils.create(
            ImmutableList.of(resource("gpu", 2, LimitedResource.Type.POOL)));

    // "RTX-4090" is non-numeric and interpreted as a request for a single unit
    Claim claim =
        LocalResourceSetUtils.claimResources(platformFor("resource:gpu", "RTX-4090"), set);

    assertThat(claim).isNotNull();
    assertThat(set.poolResources.get("gpu").pool()).hasSize(1);
  }

  @Test
  public void getResourceNameStripsResourcePrefix() {
    assertThat(LocalResourceSetUtils.getResourceName("resource:gpu")).isEqualTo("gpu");
    assertThat(LocalResourceSetUtils.getResourceName("gpu")).isEqualTo("gpu");
  }

  @Test
  public void satisfiesRequiresPrefixAndKnownResource() {
    LocalResourceSet set =
        LocalResourceSetUtils.create(
            ImmutableList.of(resource("gpu", 1, LimitedResource.Type.SEMAPHORE)));

    assertThat(
            LocalResourceSetUtils.satisfies(
                set, Platform.Property.newBuilder().setName("resource:gpu").build()))
        .isTrue();
    // missing the resource: prefix
    assertThat(
            LocalResourceSetUtils.satisfies(
                set, Platform.Property.newBuilder().setName("gpu").build()))
        .isFalse();
    // prefixed but unknown resource
    assertThat(
            LocalResourceSetUtils.satisfies(
                set, Platform.Property.newBuilder().setName("resource:unknown").build()))
        .isFalse();
  }
}
