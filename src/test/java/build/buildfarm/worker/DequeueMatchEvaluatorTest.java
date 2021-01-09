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

import static com.google.common.truth.Truth.assertThat;

import build.bazel.remote.execution.v2.Platform;
import build.buildfarm.v1test.QueueEntry;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
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

  // Function under test: shouldKeepOperation
  // Reason for testing: null queue entries should be kept
  // Failure explanation: this decision has changed
  @Test
  public void shouldKeepOperationKeepNullQueueEntry() throws Exception {

    // ARRANGE
    DequeueMatchSettings settings = new DequeueMatchSettings();
    SetMultimap<String, String> workerProvisions = HashMultimap.create();
    QueueEntry entry = null;

    // ACT
    boolean shouldKeep =
        DequeueMatchEvaluator.shouldKeepOperation(settings, workerProvisions, entry);

    // ASSERT
    assertThat(shouldKeep).isTrue();
  }

  // Function under test: shouldKeepOperation
  // Reason for testing: empty plaform queue entries should be kept
  // Failure explanation: properties are being evaluated differently now
  @Test
  public void shouldKeepOperationKeepEmptyQueueEntry() throws Exception {

    // ARRANGE
    DequeueMatchSettings settings = new DequeueMatchSettings();
    SetMultimap<String, String> workerProvisions = HashMultimap.create();
    QueueEntry entry = QueueEntry.newBuilder().setPlatform(Platform.newBuilder()).build();

    // ACT
    boolean shouldKeep =
        DequeueMatchEvaluator.shouldKeepOperation(settings, workerProvisions, entry);

    // ASSERT
    assertThat(shouldKeep).isTrue();
  }

  // Function under test: shouldKeepOperation
  // Reason for testing: the entry should be kept because the min cores are valid for the worker
  // properties
  // Failure explanation: either the property names changed or we evaluate these properties
  // differently
  @Test
  public void shouldKeepOperationValidMinCoresQueueEntry() throws Exception {

    // ARRANGE
    DequeueMatchSettings settings = new DequeueMatchSettings();

    SetMultimap<String, String> workerProvisions = HashMultimap.create();
    workerProvisions.put("cores", "11");

    QueueEntry entry =
        QueueEntry.newBuilder()
            .setPlatform(
                Platform.newBuilder()
                    .addProperties(
                        Platform.Property.newBuilder().setName("min-cores").setValue("10")))
            .build();

    // ACT
    boolean shouldKeep =
        DequeueMatchEvaluator.shouldKeepOperation(settings, workerProvisions, entry);

    // ASSERT
    // the worker accepts because it has more cores than the min-cores requested
    assertThat(shouldKeep).isTrue();
  }

  // Function under test: shouldKeepOperation
  // Reason for testing: the entry should not be kept because the min cores are invalid for the
  // worker properties
  // Failure explanation: either the property names changed or we evaluate these properties
  // differently
  @Test
  public void shouldKeepOperationInvalidMinCoresQueueEntry() throws Exception {

    // ARRANGE
    DequeueMatchSettings settings = new DequeueMatchSettings();

    SetMultimap<String, String> workerProvisions = HashMultimap.create();
    workerProvisions.put("cores", "10");

    QueueEntry entry =
        QueueEntry.newBuilder()
            .setPlatform(
                Platform.newBuilder()
                    .addProperties(
                        Platform.Property.newBuilder().setName("min-cores").setValue("11")))
            .build();

    // ACT
    boolean shouldKeep =
        DequeueMatchEvaluator.shouldKeepOperation(settings, workerProvisions, entry);

    // ASSERT
    // the worker rejects because it has less cores than the min-cores requested
    assertThat(shouldKeep).isFalse();
  }

  // Function under test: shouldKeepOperation
  // Reason for testing: a higher max-core than what the worker has does not result in rejection
  // Failure explanation: either the property names changed or max-cores is evaluated differently
  @Test
  public void shouldKeepOperationMaxCoresDoNotInfluenceAcceptance() throws Exception {

    // ARRANGE
    DequeueMatchSettings settings = new DequeueMatchSettings();

    SetMultimap<String, String> workerProvisions = HashMultimap.create();
    workerProvisions.put("cores", "10");

    QueueEntry entry =
        QueueEntry.newBuilder()
            .setPlatform(
                Platform.newBuilder()
                    .addProperties(
                        Platform.Property.newBuilder().setName("min-cores").setValue("10"))
                    .addProperties(
                        Platform.Property.newBuilder().setName("max-cores").setValue("20")))
            .build();

    // ACT
    boolean shouldKeep =
        DequeueMatchEvaluator.shouldKeepOperation(settings, workerProvisions, entry);

    // ASSERT
    // the worker accepts because it has the same cores as the min-cores requested
    assertThat(shouldKeep).isTrue();
  }

  // Function under test: shouldKeepOperation
  // Reason for testing: the worker should reject a property if it is not provided in the worker
  // platform
  // Failure explanation: ensuring exact property matches is not behaving correctly by default
  @Test
  public void shouldKeepOperationUnmatchedPropertiesRejectionAcceptance() throws Exception {

    // ARRANGE
    DequeueMatchSettings settings = new DequeueMatchSettings();

    SetMultimap<String, String> workerProvisions = HashMultimap.create();

    QueueEntry entry =
        QueueEntry.newBuilder()
            .setPlatform(
                Platform.newBuilder()
                    .addProperties(
                        Platform.Property.newBuilder().setName("foo-key").setValue("foo-value")))
            .build();

    // ACT
    boolean shouldKeep =
        DequeueMatchEvaluator.shouldKeepOperation(settings, workerProvisions, entry);

    // ASSERT
    assertThat(shouldKeep).isFalse();

    // ARRANGE
    settings.acceptEverything = true;

    // ACT
    shouldKeep = DequeueMatchEvaluator.shouldKeepOperation(settings, workerProvisions, entry);

    // ASSERT
    assertThat(shouldKeep).isTrue();

    // ARRANGE
    settings.allowUnmatched = true;

    // ACT
    shouldKeep = DequeueMatchEvaluator.shouldKeepOperation(settings, workerProvisions, entry);

    // ASSERT
    assertThat(shouldKeep).isTrue();
  }
}
