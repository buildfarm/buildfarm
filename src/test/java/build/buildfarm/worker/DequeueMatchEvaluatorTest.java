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

package build.buildfarm.worker;

import static build.buildfarm.common.ExecutionProperties.CORES;
import static build.buildfarm.common.ExecutionProperties.MAX_CORES;
import static build.buildfarm.common.ExecutionProperties.MIN_CORES;
import static build.buildfarm.worker.DequeueMatchEvaluator.acquireClaim;
import static com.google.common.truth.Truth.assertThat;

import build.bazel.remote.execution.v2.Platform;
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.worker.resources.Claim;
import build.buildfarm.worker.resources.LocalResourceSet;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import java.util.concurrent.Semaphore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * @class DequeueMatchEvaluatorTest
 * @brief tests Algorithm for deciding whether a worker should keep a dequeued operation.
 * @details When a worker takes an entry off of the queue, should it decide to keep that entry or
 *     reject and requeue it? In some sense, it should keep all entries because they have already
 *     been vetted for that particular worker. This is because the scheduler matches operations to
 *     particular queues, and workers match themselves to which queues they want to read from. But
 *     should the worker always blindly take what it pops off? And can they trust the scheduler?
 *     There may be situations where the worker chooses to give operations back based on particular
 *     contexts not known to the scheduler. For example, you might have a variety of workers with
 *     different amounts of cpu cores all sharing the same queue. The queue may accept N-core
 *     operations, because N-core workers exist in the pool, but there are additionally some lower
 *     core workers that would need to forfeit running the operation. All the reasons a worker may
 *     decide it can't take on the operation and should give it back are implemented here. The
 *     settings provided allow varying amount of leniency when evaluating the platform properties.
 */
@RunWith(JUnit4.class)
public class DequeueMatchEvaluatorTest {
  private static BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  // Function under test: acquireClaim
  // Reason for testing: empty plaform queue entries should be kept
  // Failure explanation: properties are being evaluated differently now
  @Test
  public void shouldKeepOperationKeepEmptyPlatform() throws Exception {
    // ARRANGE
    SetMultimap<String, String> workerProvisions = HashMultimap.create();
    LocalResourceSet resourceSet = new LocalResourceSet();

    // ACT
    Claim claim = acquireClaim(workerProvisions, resourceSet, Platform.getDefaultInstance());

    // ASSERT
    assertThat(claim).isNotNull();
  }

  // Function under test: acquireClaim
  // Reason for testing: the entry should be kept because the min cores are valid for the worker
  // properties
  // Failure explanation: either the property names changed or we evaluate these properties
  // differently
  @Test
  public void shouldKeepOperationValidMinCoresPlatform() throws Exception {
    // ARRANGE
    SetMultimap<String, String> workerProvisions = HashMultimap.create();
    LocalResourceSet resourceSet = new LocalResourceSet();
    workerProvisions.put(CORES, "11");

    Platform minCoresPlatform =
        Platform.newBuilder()
            .addProperties(Platform.Property.newBuilder().setName(MIN_CORES).setValue("10"))
            .build();

    // ACT
    Claim claim = acquireClaim(workerProvisions, resourceSet, minCoresPlatform);

    // ASSERT
    // the worker accepts because it has more cores than the min-cores requested
    assertThat(claim).isNotNull();

    Platform coresPlatform =
        Platform.newBuilder()
            .addProperties(Platform.Property.newBuilder().setName(CORES).setValue("10"))
            .build();

    // ACT
    claim = acquireClaim(workerProvisions, resourceSet, coresPlatform);

    // ASSERT
    // the worker accepts because it has more cores than the min-cores requested
    assertThat(claim).isNotNull();
  }

  // Function under test: acquireClaim
  // Reason for testing: the entry should not be kept because the min cores are invalid for the
  // worker properties
  // Failure explanation: either the property names changed or we evaluate these properties
  // differently
  @Test
  public void shouldKeepOperationInvalidMinCoresPlatform() throws Exception {
    // ARRANGE
    SetMultimap<String, String> workerProvisions = HashMultimap.create();
    LocalResourceSet resourceSet = new LocalResourceSet();
    workerProvisions.put(CORES, "10");

    Platform minCoresPlatform =
        Platform.newBuilder()
            .addProperties(Platform.Property.newBuilder().setName(MIN_CORES).setValue("11"))
            .build();

    // ACT
    Claim claim = acquireClaim(workerProvisions, resourceSet, minCoresPlatform);

    // ASSERT
    // the worker rejects because it has less cores than the min-cores requested
    assertThat(claim).isNull();

    Platform coresPlatform =
        Platform.newBuilder()
            .addProperties(Platform.Property.newBuilder().setName(CORES).setValue("11"))
            .build();

    // ACT
    claim = acquireClaim(workerProvisions, resourceSet, coresPlatform);

    // ASSERT
    // the worker rejects because it has less cores than the min-cores requested
    assertThat(claim).isNull();
  }

  // Function under test: acquireClaim
  // Reason for testing: a higher max-core than what the worker has does not result in rejection
  // Failure explanation: either the property names changed or max-cores is evaluated differently
  @Test
  public void shouldKeepOperationMaxCoresDoNotInfluenceAcceptance() throws Exception {
    // ARRANGE
    SetMultimap<String, String> workerProvisions = HashMultimap.create();
    LocalResourceSet resourceSet = new LocalResourceSet();
    workerProvisions.put(CORES, "10");

    Platform minCoresPlatform =
        Platform.newBuilder()
            .addProperties(Platform.Property.newBuilder().setName(MIN_CORES).setValue("10"))
            .addProperties(Platform.Property.newBuilder().setName(MAX_CORES).setValue("20"))
            .build();

    // ACT
    Claim claim = acquireClaim(workerProvisions, resourceSet, minCoresPlatform);

    // ASSERT
    // the worker accepts because it has the same cores as the min-cores requested
    assertThat(claim).isNotNull();

    Platform coresPlatform =
        Platform.newBuilder()
            .addProperties(Platform.Property.newBuilder().setName(CORES).setValue("10"))
            .addProperties(Platform.Property.newBuilder().setName(MAX_CORES).setValue("20"))
            .build();

    // ACT
    claim = acquireClaim(workerProvisions, resourceSet, coresPlatform);

    // ASSERT
    // the worker accepts because it has the same cores as the cores requested
    assertThat(claim).isNotNull();
  }

  // Function under test: acquireClaim
  // Reason for testing: the worker should reject a property if it is not provided in the worker
  // platform
  // Failure explanation: ensuring exact property matches is not behaving correctly by default
  @Test
  public void shouldKeepOperationUnmatchedPropertiesRejectionAcceptance() throws Exception {
    // ARRANGE
    configs.getWorker().getDequeueMatchSettings().setAllowUnmatched(false);
    SetMultimap<String, String> workerProvisions = HashMultimap.create();
    LocalResourceSet resourceSet = new LocalResourceSet();

    Platform platform =
        Platform.newBuilder()
            .addProperties(Platform.Property.newBuilder().setName("foo-key").setValue("foo-value"))
            .build();

    // ACT
    Claim claim = acquireClaim(workerProvisions, resourceSet, platform);

    // ASSERT
    assertThat(claim).isNull();

    // ARRANGE
    configs.getWorker().getDequeueMatchSettings().setAllowUnmatched(true);

    // ACT
    claim = acquireClaim(workerProvisions, resourceSet, platform);

    // ASSERT
    assertThat(claim).isNotNull();
  }

  // Function under test: acquireClaim
  // Reason for testing: the local resource should be claimed
  // Failure explanation: semaphore claim did not work as expected.
  @Test
  public void shouldKeepOperationClaimsResource() throws Exception {
    // ARRANGE
    configs.getWorker().getDequeueMatchSettings().setAllowUnmatched(true);
    SetMultimap<String, String> workerProvisions = HashMultimap.create();
    LocalResourceSet resourceSet = new LocalResourceSet();
    resourceSet.resources.put("FOO", new Semaphore(1));

    Platform platform =
        Platform.newBuilder()
            .addProperties(Platform.Property.newBuilder().setName("resource:FOO").setValue("1"))
            .build();

    // PRE-ASSERT
    assertThat(resourceSet.resources.get("FOO").availablePermits()).isEqualTo(1);

    // ACT
    Claim claim = acquireClaim(workerProvisions, resourceSet, platform);

    // ASSERT
    // the worker accepts because the resource is available.
    assertThat(claim).isNotNull();
    assertThat(resourceSet.resources.get("FOO").availablePermits()).isEqualTo(0);

    // ACT
    claim = acquireClaim(workerProvisions, resourceSet, platform);

    // ASSERT
    // the worker rejects because there are no resources left.
    assertThat(claim).isNull();
    assertThat(resourceSet.resources.get("FOO").availablePermits()).isEqualTo(0);
  }

  // Function under test: acquireClaim
  // Reason for testing: the local resource should be claimed
  // Failure explanation: semaphore claim did not work as expected.
  @Test
  public void rejectOperationIgnoresResource() throws Exception {
    // ARRANGE
    configs.getWorker().getDequeueMatchSettings().setAllowUnmatched(false);
    SetMultimap<String, String> workerProvisions = HashMultimap.create();
    LocalResourceSet resourceSet = new LocalResourceSet();
    resourceSet.resources.put("FOO", new Semaphore(1));

    Platform platform =
        Platform.newBuilder()
            .addProperties(Platform.Property.newBuilder().setName("resource:FOO").setValue("1"))
            .addProperties(Platform.Property.newBuilder().setName("os").setValue("randos"))
            .build();

    // PRE-ASSERT
    assertThat(resourceSet.resources.get("FOO").availablePermits()).isEqualTo(1);

    // ACT
    Claim claim = acquireClaim(workerProvisions, resourceSet, platform);

    // ASSERT
    // the worker rejects because the os is not satisfied
    assertThat(claim).isNull();
    assertThat(resourceSet.resources.get("FOO").availablePermits()).isEqualTo(1);
  }

  // Function under test: acquireClaim
  // Reason for testing: the local resources should be claimed
  // Failure explanation: semaphore claim did not work as expected.
  @Test
  public void shouldKeepOperationClaimsMultipleResource() throws Exception {
    // ARRANGE
    configs.getWorker().getDequeueMatchSettings().setAllowUnmatched(true);
    SetMultimap<String, String> workerProvisions = HashMultimap.create();
    LocalResourceSet resourceSet = new LocalResourceSet();
    resourceSet.resources.put("FOO", new Semaphore(2));
    resourceSet.resources.put("BAR", new Semaphore(4));

    Platform platform =
        Platform.newBuilder()
            .addProperties(Platform.Property.newBuilder().setName("resource:FOO").setValue("1"))
            .addProperties(Platform.Property.newBuilder().setName("resource:BAR").setValue("2"))
            .build();

    // PRE-ASSERT
    assertThat(resourceSet.resources.get("FOO").availablePermits()).isEqualTo(2);
    assertThat(resourceSet.resources.get("BAR").availablePermits()).isEqualTo(4);

    // ACT
    Claim claim = acquireClaim(workerProvisions, resourceSet, platform);

    // ASSERT
    // the worker accepts because the resource is available.
    assertThat(claim).isNotNull();
    assertThat(resourceSet.resources.get("FOO").availablePermits()).isEqualTo(1);
    assertThat(resourceSet.resources.get("BAR").availablePermits()).isEqualTo(2);

    // ACT
    claim = acquireClaim(workerProvisions, resourceSet, platform);

    // ASSERT
    // the worker accepts because the resource is available.
    assertThat(claim).isNotNull();
    assertThat(resourceSet.resources.get("FOO").availablePermits()).isEqualTo(0);
    assertThat(resourceSet.resources.get("BAR").availablePermits()).isEqualTo(0);

    // ACT
    claim = acquireClaim(workerProvisions, resourceSet, platform);

    // ASSERT
    // the worker rejects because there are no resources left.
    assertThat(claim).isNull();
    assertThat(resourceSet.resources.get("FOO").availablePermits()).isEqualTo(0);
    assertThat(resourceSet.resources.get("BAR").availablePermits()).isEqualTo(0);
  }

  // Function under test: acquireClaim
  // Reason for testing: the local resources should fail to claim, and the existing amount should be
  // the same.
  // Failure explanation: semaphore claim did not work as expected.
  @Test
  public void shouldKeepOperationFailsToClaimSameAmountRemains() throws Exception {
    // ARRANGE
    configs.getWorker().getDequeueMatchSettings().setAllowUnmatched(true);
    SetMultimap<String, String> workerProvisions = HashMultimap.create();
    LocalResourceSet resourceSet = new LocalResourceSet();
    resourceSet.resources.put("FOO", new Semaphore(50));
    resourceSet.resources.put("BAR", new Semaphore(100));
    resourceSet.resources.put("BAZ", new Semaphore(200));

    Platform platform =
        Platform.newBuilder()
            .addProperties(Platform.Property.newBuilder().setName("resource:FOO").setValue("20"))
            .addProperties(Platform.Property.newBuilder().setName("resource:BAR").setValue("101"))
            .addProperties(Platform.Property.newBuilder().setName("resource:BAZ").setValue("20"))
            .build();

    // PRE-ASSERT
    assertThat(resourceSet.resources.get("FOO").availablePermits()).isEqualTo(50);
    assertThat(resourceSet.resources.get("BAR").availablePermits()).isEqualTo(100);
    assertThat(resourceSet.resources.get("BAZ").availablePermits()).isEqualTo(200);

    // ACT
    Claim claim = acquireClaim(workerProvisions, resourceSet, platform);

    // ASSERT
    // the worker rejects because there are no resources left.
    // The same amount are returned.
    assertThat(claim).isNull();
    assertThat(resourceSet.resources.get("FOO").availablePermits()).isEqualTo(50);
    assertThat(resourceSet.resources.get("BAR").availablePermits()).isEqualTo(100);
    assertThat(resourceSet.resources.get("BAZ").availablePermits()).isEqualTo(200);
  }

  @Test
  public void shouldMatchCoresAsMinAndMax() throws Exception {
    SetMultimap<String, String> workerProvisions = HashMultimap.create();
    LocalResourceSet resourceSet = new LocalResourceSet();
    configs.getWorker().getDequeueMatchSettings().setAllowUnmatched(false);

    Platform multicorePlatform =
        Platform.newBuilder()
            .addProperties(Platform.Property.newBuilder().setName(CORES).setValue("2"))
            .build();

    // cores must be present from worker provisions to keep cores specified in platform
    assertThat(acquireClaim(workerProvisions, resourceSet, multicorePlatform)).isNull();
  }
}
