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

package build.buildfarm.common.redis;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.SetMultimap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * @class ProvisionedRedisQueueTest
 * @brief tests A queue that is designed to hold particularly provisioned elements.
 * @details A provisioned redis queue is an implementation of a queue data structure which
 *     internally uses a redis cluster to distribute the data across shards. Its important to know
 *     that the lifetime of the queue persists before and after the queue data structure is created
 *     (since it exists in redis). Therefore, two redis queues with the same name, would in fact be
 *     the same underlying redis queue. This redis queue comes with a list of required provisions.
 *     If the queue element does not meet the required provisions, it should not be stored in the
 *     queue. Provision queues are intended to represent particular operations that should only be
 *     processed by particular workers. An example use case for this would be to have two dedicated
 *     provision queues for CPU and GPU operations. CPU/GPU requirements would be determined through
 *     the remote api's command platform properties. We designate provision queues to have a set of
 *     "required provisions" (which match the platform properties). This allows the scheduler to
 *     distribute operations by their properties and allows workers to dequeue from particular
 *     queues.
 */
@RunWith(JUnit4.class)
public class ProvisionedRedisQueueTest {
  // Function under test: ProvisionedRedisQueue
  // Reason for testing: the object can be constructed
  // Failure explanation: the object cannot be constructed
  @Test
  public void provisionedRedisQueueCanConstruct() throws Exception {
    new ProvisionedRedisQueue("name", ImmutableList.of(), HashMultimap.create());
  }

  // Function under test: ProvisionedRedisQueue
  // Reason for testing: the object can be constructed
  // Failure explanation: the object cannot be constructed
  @Test
  public void provisionedRedisQueueCanConstructOverload() throws Exception {
    new ProvisionedRedisQueue("name", ImmutableList.of(), HashMultimap.create(), true);
  }

  // Function under test: explainEligibility
  // Reason for testing: the queue will accept the properties if no provisions are involved
  // Failure explanation: the queue is unable to accept the properties with no provisions or the
  // explanation is wrong
  @Test
  public void explainEligibilityNoProvisionsAccepted() throws Exception {
    // ARRANGE
    ProvisionedRedisQueue queue =
        new ProvisionedRedisQueue("foo", ImmutableList.of(), HashMultimap.create());

    // ACT
    String explanation = queue.explainEligibility(HashMultimap.create());
    boolean isEligible = queue.isEligible(HashMultimap.create());

    // ASSERT
    String expected_explanation = "The properties are eligible for the foo queue.\n";
    expected_explanationBuilder.append("matched: {}\n");
    expected_explanationBuilder.append("unmatched: {}\n");
    expected_explanationBuilder.append("still required: {}\n");
    assertThat(explanation).isEqualTo(expected_explanation);
    assertThat(isEligible).isTrue();
  }

  // Function under test: explainEligibility
  // Reason for testing: the queue will accept the properties if no provisions are involved
  // Failure explanation: the queue is unable to accept the properties with no provisions or the
  // explanation is wrong
  @Test
  public void explainEligibilityNoProvisionsAcceptedAllowUserUnmatched() throws Exception {
    // ARRANGE
    ProvisionedRedisQueue queue =
        new ProvisionedRedisQueue("foo", ImmutableList.of(), HashMultimap.create(), true);

    // ACT
    String explanation = queue.explainEligibility(HashMultimap.create());
    boolean isEligible = queue.isEligible(HashMultimap.create());

    // ASSERT
    String expected_explanation = "The properties are eligible for the foo queue.\n";
    expected_explanationBuilder.append("matched: {}\n");
    expected_explanationBuilder.append("unmatched: {}\n");
    expected_explanationBuilder.append("still required: {}\n");
    assertThat(explanation).isEqualTo(expected_explanation);
    assertThat(isEligible).isTrue();
  }

  // Function under test: explainEligibility
  // Reason for testing: the queue will accept the properties and show the provision was matched
  // Failure explanation: the queue is unable to accept the properties or the explanation is wrong
  @Test
  public void explainEligibilitySingleProvisionMatched() throws Exception {
    // ARRANGE
    SetMultimap<String, String> queueProperties = HashMultimap.create();
    queueProperties.put("key", "value");
    ProvisionedRedisQueue queue =
        new ProvisionedRedisQueue("foo", ImmutableList.of(), queueProperties);

    // ACT
    String explanation = queue.explainEligibility(queueProperties);
    boolean isEligible = queue.isEligible(queueProperties);

    // ASSERT
    String expected_explanation = "The properties are eligible for the foo queue.\n";
    expected_explanationBuilder.append("matched: {key=[value]}\n");
    expected_explanationBuilder.append("unmatched: {}\n");
    expected_explanationBuilder.append("still required: {}\n");
    assertThat(explanation).isEqualTo(expected_explanation);
    assertThat(isEligible).isTrue();
  }

  // Function under test: explainEligibility
  // Reason for testing: the queue will accept the properties and show the provision was matched
  // Failure explanation: the queue is unable to accept the properties or the explanation is wrong
  @Test
  public void explainEligibilitySingleProvisionMatchedAllowUserUnmatched() throws Exception {
    // ARRANGE
    SetMultimap<String, String> queueProperties = HashMultimap.create();
    queueProperties.put("key", "value");
    ProvisionedRedisQueue queue =
        new ProvisionedRedisQueue("foo", ImmutableList.of(), queueProperties, true);

    // ACT
    String explanation = queue.explainEligibility(queueProperties);
    boolean isEligible = queue.isEligible(queueProperties);

    // ASSERT
    String expected_explanation = "The properties are eligible for the foo queue.\n";
    expected_explanationBuilder.append("matched: {key=[value]}\n");
    expected_explanationBuilder.append("unmatched: {}\n");
    expected_explanationBuilder.append("still required: {}\n");
    assertThat(explanation).isEqualTo(expected_explanation);
    assertThat(isEligible).isTrue();
  }

  // Function under test: explainEligibility
  // Reason for testing: the queue will not accept properties and show the provision was not matched
  // Failure explanation: the queue is still accepting properties or the explanation is wrong
  @Test
  public void explainEligibilitySingleProvisionNotMatched() throws Exception {
    // ARRANGE
    SetMultimap<String, String> queueProperties = HashMultimap.create();
    queueProperties.put("key", "value");
    ProvisionedRedisQueue queue =
        new ProvisionedRedisQueue("foo", ImmutableList.of(), HashMultimap.create());

    // ACT
    String explanation = queue.explainEligibility(queueProperties);
    boolean isEligible = queue.isEligible(queueProperties);

    // ASSERT
    String expected_explanation = "The properties are not eligible for the foo queue.\n";
    expected_explanationBuilder.append("matched: {}\n");
    expected_explanationBuilder.append("unmatched: {key=[value]}\n");
    expected_explanationBuilder.append("still required: {}\n");
    assertThat(explanation).isEqualTo(expected_explanation);
    assertThat(isEligible).isFalse();
  }

  // Function under test: explainEligibility
  // Reason for testing: the queue will accept properties even though some were unmatched
  // Failure explanation: the queue is not accepting properties or the explanation is wrong
  @Test
  public void explainEligibilitySingleProvisionAllowedToNotMatch() throws Exception {
    // ARRANGE
    SetMultimap<String, String> queueProperties = HashMultimap.create();
    queueProperties.put("key", "value");
    ProvisionedRedisQueue queue =
        new ProvisionedRedisQueue("foo", ImmutableList.of(), HashMultimap.create(), true);

    // ACT
    String explanation = queue.explainEligibility(queueProperties);
    boolean isEligible = queue.isEligible(queueProperties);

    // ASSERT
    String expected_explanation = "The properties are eligible for the foo queue.\n";
    expected_explanationBuilder.append("matched: {}\n");
    expected_explanationBuilder.append("unmatched: {key=[value]}\n");
    expected_explanationBuilder.append("still required: {}\n");
    assertThat(explanation).isEqualTo(expected_explanation);
    assertThat(isEligible).isTrue();
  }

  // Function under test: explainEligibility
  // Reason for testing: the queue will not accept properties and show the provision is still
  // required
  // Failure explanation: the queue is still accepting properties or the explanation is wrong
  @Test
  public void explainEligibilitySingleProvisionStillRequired() throws Exception {
    // ARRANGE
    SetMultimap<String, String> queueProperties = HashMultimap.create();
    queueProperties.put("key", "value");
    ProvisionedRedisQueue queue =
        new ProvisionedRedisQueue("foo", ImmutableList.of(), queueProperties);

    // ACT
    String explanation = queue.explainEligibility(HashMultimap.create());
    boolean isEligible = queue.isEligible(HashMultimap.create());

    // ASSERT
    String expected_explanation = "The properties are not eligible for the foo queue.\n";
    expected_explanationBuilder.append("matched: {}\n");
    expected_explanationBuilder.append("unmatched: {}\n");
    expected_explanationBuilder.append("still required: {key=[value]}\n");
    assertThat(explanation).isEqualTo(expected_explanation);
    assertThat(isEligible).isFalse();
  }

  // Function under test: explainEligibility
  // Reason for testing: the queue will not accept properties and show the provision is still
  // required
  // Failure explanation: the queue is still accepting properties or the explanation is wrong
  @Test
  public void explainEligibilitySingleProvisionStillRequiredAllowUserUnmatched() throws Exception {
    // ARRANGE
    SetMultimap<String, String> queueProperties = HashMultimap.create();
    queueProperties.put("key", "value");
    ProvisionedRedisQueue queue =
        new ProvisionedRedisQueue("foo", ImmutableList.of(), queueProperties, true);

    // ACT
    String explanation = queue.explainEligibility(HashMultimap.create());
    boolean isEligible = queue.isEligible(HashMultimap.create());

    // ASSERT
    String expected_explanation = "The properties are not eligible for the foo queue.\n";
    expected_explanationBuilder.append("matched: {}\n");
    expected_explanationBuilder.append("unmatched: {}\n");
    expected_explanationBuilder.append("still required: {key=[value]}\n");
    assertThat(explanation).isEqualTo(expected_explanation);
    assertThat(isEligible).isFalse();
  }

  // Function under test: explainEligibility
  // Reason for testing: the queue will accept the properties because of wildcard
  // Failure explanation: the queue is unable to accept the properties or the explanation is wrong
  @Test
  public void explainEligibilityAcceptedDueToWildcard() throws Exception {
    // ARRANGE
    SetMultimap<String, String> queueProperties = HashMultimap.create();
    queueProperties.put("*", "value");
    ProvisionedRedisQueue queue =
        new ProvisionedRedisQueue("foo", ImmutableList.of(), queueProperties);

    // ACT
    String explanation = queue.explainEligibility(HashMultimap.create());
    boolean isEligible = queue.isEligible(HashMultimap.create());

    // ASSERT
    String expected_explanation = "The properties are eligible for the foo queue.\n";
    expected_explanationBuilder.append("The queue is fully wildcard.\n");
    assertThat(explanation).isEqualTo(expected_explanation);
    assertThat(isEligible).isTrue();
  }

  // Function under test: explainEligibility
  // Reason for testing: the queue will accept the properties because of wildcard
  // Failure explanation: the queue is unable to accept the properties or the explanation is wrong
  @Test
  public void explainEligibilityAcceptedDueToWildcardAllowUserUnmatched() throws Exception {
    // ARRANGE
    SetMultimap<String, String> queueProperties = HashMultimap.create();
    queueProperties.put("*", "value");
    ProvisionedRedisQueue queue =
        new ProvisionedRedisQueue("foo", ImmutableList.of(), queueProperties, true);

    // ACT
    String explanation = queue.explainEligibility(HashMultimap.create());
    boolean isEligible = queue.isEligible(HashMultimap.create());

    // ASSERT
    String expected_explanation = "The properties are eligible for the foo queue.\n";
    expected_explanationBuilder.append("The queue is fully wildcard.\n");
    assertThat(explanation).isEqualTo(expected_explanation);
    assertThat(isEligible).isTrue();
  }

  // Function under test: explainEligibility
  // Reason for testing: this shows a non-desired result which acts as a motivating example for the
  // "allow unmatched feature
  // Failure explanation: the queue is accepting properties or the explanation is wrong
  @Test
  public void explainEligibilityAllowUnmatchedBadUseCaseExample() throws Exception {
    // ARRANGE
    SetMultimap<String, String> queueProperties = HashMultimap.create();
    queueProperties.put("min-cores", "*");
    queueProperties.put("max-cores", "*");
    ProvisionedRedisQueue queue =
        new ProvisionedRedisQueue("cpu", ImmutableList.of(), queueProperties);

    // ACT
    SetMultimap<String, String> userGivenProperties = HashMultimap.create();
    userGivenProperties.put("min-cores", "1");
    userGivenProperties.put("max-cores", "1");
    userGivenProperties.put("env-vars", "{'OMP_NUM_THREAD': '1'}");
    String explanation = queue.explainEligibility(userGivenProperties);
    boolean isEligible = queue.isEligible(userGivenProperties);

    // ASSERT
    String expected_explanation = "The properties are not eligible for the cpu queue.\n";
    expected_explanationBuilder.append("matched: {min-cores=[1], max-cores=[1]}\n");
    expected_explanationBuilder.append("unmatched: {env-vars=[{'OMP_NUM_THREAD': '1'}]}\n");
    expected_explanationBuilder.append("still required: {}\n");
    assertThat(explanation).isEqualTo(expected_explanation);
    assertThat(isEligible).isFalse();
  }

  // Function under test: explainEligibility
  // Reason for testing: this is an example use case we have where you would want to allow unmatched
  // Failure explanation: the queue is not accepting properties or the explanation is wrong
  @Test
  public void explainEligibilityAllowUnmatchedUseCaseExample() throws Exception {
    // ARRANGE
    SetMultimap<String, String> queueProperties = HashMultimap.create();
    queueProperties.put("min-cores", "*");
    queueProperties.put("max-cores", "*");
    ProvisionedRedisQueue queue =
        new ProvisionedRedisQueue("cpu", ImmutableList.of(), queueProperties, true);

    // ACT
    SetMultimap<String, String> userGivenProperties = HashMultimap.create();
    userGivenProperties.put("min-cores", "1");
    userGivenProperties.put("max-cores", "1");
    userGivenProperties.put("env-vars", "{'OMP_NUM_THREAD': '1'}");
    String explanation = queue.explainEligibility(userGivenProperties);
    boolean isEligible = queue.isEligible(userGivenProperties);

    // ASSERT
    String expected_explanation = "The properties are eligible for the cpu queue.\n";
    expected_explanationBuilder.append("matched: {min-cores=[1], max-cores=[1]}\n");
    expected_explanationBuilder.append("unmatched: {env-vars=[{'OMP_NUM_THREAD': '1'}]}\n");
    expected_explanationBuilder.append("still required: {}\n");
    assertThat(explanation).isEqualTo(expected_explanation);
    assertThat(isEligible).isTrue();
  }

  // Function under test: explainEligibility
  // Reason for testing: show that a queue can be specifically selected
  // Failure explanation: the selection key is not properly being recognized
  @Test
  public void explainEligibilitySpecificallySelectQueue() throws Exception {
    // ARRANGE
    ProvisionedRedisQueue queue =
        new ProvisionedRedisQueue("foo", ImmutableList.of(), HashMultimap.create());
    SetMultimap<String, String> userGivenProperties = HashMultimap.create();
    userGivenProperties.put("choose-queue", "foo");

    // ACT
    String explanation = queue.explainEligibility(userGivenProperties);
    boolean isEligible = queue.isEligible(userGivenProperties);

    // ASSERT
    String expected_explanation = "The properties are eligible for the foo queue.\n";
    expected_explanationBuilder.append("The queue was specifically chosen.\n");
    assertThat(explanation).isEqualTo(expected_explanation);
    assertThat(isEligible).isTrue();
  }

  // Function under test: explainEligibility
  // Reason for testing: show that trying to select the queue by the wrong name fails eligibility
  // Failure explanation: the queue is incorrectly still being selected or the explanation is wrong
  @Test
  public void explainEligibilityFailToSpecificallySelectQueue() throws Exception {
    // ARRANGE
    ProvisionedRedisQueue queue =
        new ProvisionedRedisQueue("foo", ImmutableList.of(), HashMultimap.create());
    SetMultimap<String, String> userGivenProperties = HashMultimap.create();
    userGivenProperties.put("choose-queue", "wrong");

    // ACT
    String explanation = queue.explainEligibility(userGivenProperties);
    boolean isEligible = queue.isEligible(userGivenProperties);

    // ASSERT
    String expected_explanation = "The properties are not eligible for the foo queue.\n";
    expected_explanationBuilder.append("matched: {}\n");
    expected_explanationBuilder.append("unmatched: {choose-queue=[wrong]}\n");
    expected_explanationBuilder.append("still required: {}\n");
    assertThat(explanation).isEqualTo(expected_explanation);
    assertThat(isEligible).isFalse();
  }
}
