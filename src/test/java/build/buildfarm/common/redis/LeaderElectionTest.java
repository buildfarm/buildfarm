// Copyright 2025 The Buildfarm Authors. All rights reserved.
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
import static org.junit.Assert.assertThrows;

import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.instance.shard.JedisClusterFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.UnifiedJedis;

/**
 * @class LeaderElectionTest
 * @brief Tests for Redis-based leader election.
 * @details Tests cover single-node and multi-node scenarios, lease renewal, expiration, callbacks,
 *     and error conditions. Tests requiring Redis are tagged appropriately.
 */
@RunWith(JUnit4.class)
public class LeaderElectionTest {
  private static final String TEST_ELECTION_KEY = "test-election";
  private static final Duration SHORT_LEASE = Duration.ofSeconds(2);
  private static final Duration VERY_SHORT_LEASE = Duration.ofMillis(500);
  private static final Duration SHORT_RENEWAL = Duration.ofMillis(500);

  private BuildfarmConfigs configs = BuildfarmConfigs.getInstance();
  private JedisPooled pooled;
  private Jedis redis;
  private UnifiedJedis unified;
  private List<LeaderElection> electionsToClose = new ArrayList<>();

  @Before
  public void setUp() throws Exception {
    configs.getBackplane().setRedisUri("redis://localhost:6379");
    unified = JedisClusterFactory.createTest();
    assertThat(unified).isInstanceOf(JedisPooled.class);
    pooled = (JedisPooled) unified;
    redis = new Jedis(pooled.getPool().getResource());
    redis.flushDB();
  }

  @After
  public void tearDown() {
    // Close all elections created during tests
    for (LeaderElection election : electionsToClose) {
      try {
        election.close();
      } catch (Exception e) {
        // Ignore cleanup errors
      }
    }
    electionsToClose.clear();

    redis.flushDB();
    redis.close();
    pooled.close();
  }

  /** Helper method to create a LeaderElection instance that will be automatically closed. */
  private LeaderElection createElection(String nodeId, Duration lease, Duration renewal) {
    LeaderElection election = new LeaderElection(nodeId, lease, renewal);
    electionsToClose.add(election);
    return election;
  }

  private LeaderElection createElection(String nodeId) {
    return createElection(nodeId, SHORT_LEASE, SHORT_RENEWAL);
  }

  // ===== Constructor and Validation Tests =====

  @Test
  public void testConstructorWithValidParameters() {
    LeaderElection election =
        createElection("node1", Duration.ofSeconds(60), Duration.ofSeconds(20));

    assertThat(election.getNodeId()).isEqualTo("node1");
    assertThat(election.getLeaseDuration()).isEqualTo(Duration.ofSeconds(60));
    assertThat(election.getRenewalInterval()).isEqualTo(Duration.ofSeconds(20));
    assertThat(election.isClosed()).isFalse();
  }

  @Test
  public void testConstructorWithDefaultRenewalInterval() {
    LeaderElection election = new LeaderElection("node1", Duration.ofSeconds(60));
    electionsToClose.add(election);

    assertThat(election.getNodeId()).isEqualTo("node1");
    assertThat(election.getLeaseDuration()).isEqualTo(Duration.ofSeconds(60));
    assertThat(election.getRenewalInterval()).isEqualTo(Duration.ofSeconds(20)); // 60 / 3
  }

  @Test
  public void testConstructorWithAllDefaults() {
    LeaderElection election = createElection("node1");

    assertThat(election.getNodeId()).isEqualTo("node1");
    assertThat(election.getLeaseDuration()).isEqualTo(Duration.ofSeconds(60));
    assertThat(election.getRenewalInterval()).isEqualTo(Duration.ofSeconds(20));
  }

  @Test
  public void testConstructorRejectsNullNodeId() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new LeaderElection(null, Duration.ofSeconds(60), Duration.ofSeconds(20)));
  }

  @Test
  public void testConstructorRejectsEmptyNodeId() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new LeaderElection("", Duration.ofSeconds(60), Duration.ofSeconds(20)));
  }

  @Test
  public void testConstructorRejectsNullLeaseDuration() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new LeaderElection("node1", null, Duration.ofSeconds(20)));
  }

  @Test
  public void testConstructorRejectsNegativeLeaseDuration() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new LeaderElection("node1", Duration.ofSeconds(-10), Duration.ofSeconds(5)));
  }

  @Test
  public void testConstructorRejectsZeroLeaseDuration() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new LeaderElection("node1", Duration.ZERO, Duration.ofSeconds(5)));
  }

  @Test
  public void testConstructorRejectsRenewalGreaterThanLease() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new LeaderElection("node1", Duration.ofSeconds(10), Duration.ofSeconds(20)));
  }

  @Test
  public void testConstructorRejectsRenewalEqualToLease() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new LeaderElection("node1", Duration.ofSeconds(10), Duration.ofSeconds(10)));
  }

  // ===== Single Node Leader Election Tests =====

  @Test
  public void testSingleNodeBecomesLeader() {
    LeaderElection election = createElection("node1");

    boolean becameLeader = election.tryBecomeLeader(unified, TEST_ELECTION_KEY);

    assertThat(becameLeader).isTrue();
    assertThat(election.isLeader(unified, TEST_ELECTION_KEY)).isTrue();
    assertThat(election.getCurrentLeader(unified, TEST_ELECTION_KEY)).isEqualTo("node1");
  }

  @Test
  public void testMultipleAttemptsToBecomLeaderAreIdempotent() {
    LeaderElection election = createElection("node1");

    boolean first = election.tryBecomeLeader(unified, TEST_ELECTION_KEY);
    boolean second = election.tryBecomeLeader(unified, TEST_ELECTION_KEY);
    boolean third = election.tryBecomeLeader(unified, TEST_ELECTION_KEY);

    assertThat(first).isTrue();
    assertThat(second).isTrue();
    assertThat(third).isTrue();
    assertThat(election.isLeader(unified, TEST_ELECTION_KEY)).isTrue();
  }

  @Test
  public void testCanBecomeLeaderForMultipleElections() {
    LeaderElection election = createElection("node1");

    boolean leader1 = election.tryBecomeLeader(unified, "election-1");
    boolean leader2 = election.tryBecomeLeader(unified, "election-2");
    boolean leader3 = election.tryBecomeLeader(unified, "election-3");

    assertThat(leader1).isTrue();
    assertThat(leader2).isTrue();
    assertThat(leader3).isTrue();
    assertThat(election.isLeader(unified, "election-1")).isTrue();
    assertThat(election.isLeader(unified, "election-2")).isTrue();
    assertThat(election.isLeader(unified, "election-3")).isTrue();
  }

  // ===== Multi-Node Competition Tests =====

  @Test
  public void testMultipleNodesOnlyOneBecomesLeader() {
    LeaderElection election1 = createElection("node1");
    LeaderElection election2 = createElection("node2");
    LeaderElection election3 = createElection("node3");

    boolean leader1 = election1.tryBecomeLeader(unified, TEST_ELECTION_KEY);
    boolean leader2 = election2.tryBecomeLeader(unified, TEST_ELECTION_KEY);
    boolean leader3 = election3.tryBecomeLeader(unified, TEST_ELECTION_KEY);

    // Exactly one should be leader
    int leaderCount = (leader1 ? 1 : 0) + (leader2 ? 1 : 0) + (leader3 ? 1 : 0);
    assertThat(leaderCount).isEqualTo(1);

    // Verify the leader is correct
    String currentLeader = election1.getCurrentLeader(unified, TEST_ELECTION_KEY);
    assertThat(currentLeader).isNotNull();
    assertThat(currentLeader).isIn(List.of("node1", "node2", "node3"));
  }

  @Test
  public void testNonLeaderCannotRenewLeadership() {
    LeaderElection election1 = createElection("node1");
    LeaderElection election2 = createElection("node2");

    election1.tryBecomeLeader(unified, TEST_ELECTION_KEY);

    boolean renewed = election2.renewLeadership(unified, TEST_ELECTION_KEY);

    assertThat(renewed).isFalse();
  }

  // ===== Lease Renewal Tests =====

  @Test
  public void testLeaderCanRenewLeadership() {
    LeaderElection election = createElection("node1");

    election.tryBecomeLeader(unified, TEST_ELECTION_KEY);
    boolean renewed = election.renewLeadership(unified, TEST_ELECTION_KEY);

    assertThat(renewed).isTrue();
    assertThat(election.isLeader(unified, TEST_ELECTION_KEY)).isTrue();
  }

  @Test
  public void testAutomaticLeaseRenewal() throws InterruptedException {
    LeaderElection election = createElection("node1", SHORT_LEASE, SHORT_RENEWAL);

    election.tryBecomeLeader(unified, TEST_ELECTION_KEY);

    // Wait for more than the lease duration but let auto-renewal keep it alive
    Thread.sleep(SHORT_LEASE.toMillis() + 500);

    // Should still be leader due to auto-renewal
    assertThat(election.isLeader(unified, TEST_ELECTION_KEY)).isTrue();
  }

  // ===== Lease Expiration Tests =====

  @Test
  public void testLeadershipExpiresWithoutRenewal() throws InterruptedException {
    // Create election with very short lease and no auto-renewal
    LeaderElection election =
        createElection("node1", VERY_SHORT_LEASE, VERY_SHORT_LEASE.dividedBy(3));

    election.tryBecomeLeader(unified, TEST_ELECTION_KEY);
    assertThat(election.isLeader(unified, TEST_ELECTION_KEY)).isTrue();

    // Close to stop auto-renewal
    election.close();

    // Wait for lease to expire
    Thread.sleep(VERY_SHORT_LEASE.toMillis() + 200);

    // Leadership should have expired
    String currentLeader = election.getCurrentLeader(unified, TEST_ELECTION_KEY);
    assertThat(currentLeader).isNull();
  }

  @Test
  public void testLeadershipTransferAfterExpiration() throws InterruptedException {
    LeaderElection election1 =
        createElection("node1", VERY_SHORT_LEASE, VERY_SHORT_LEASE.dividedBy(3));
    LeaderElection election2 = createElection("node2");

    // Node1 becomes leader
    election1.tryBecomeLeader(unified, TEST_ELECTION_KEY);
    assertThat(election1.getCurrentLeader(unified, TEST_ELECTION_KEY)).isEqualTo("node1");

    // Stop node1's renewal
    election1.close();

    // Wait for lease to expire
    Thread.sleep(VERY_SHORT_LEASE.toMillis() + 200);

    // Node2 should now be able to become leader
    boolean becameLeader = election2.tryBecomeLeader(unified, TEST_ELECTION_KEY);
    assertThat(becameLeader).isTrue();
    assertThat(election2.getCurrentLeader(unified, TEST_ELECTION_KEY)).isEqualTo("node2");
  }

  // ===== Resignation Tests =====

  @Test
  public void testLeaderCanResign() {
    LeaderElection election = createElection("node1");

    election.tryBecomeLeader(unified, TEST_ELECTION_KEY);
    boolean resigned = election.resignLeadership(unified, TEST_ELECTION_KEY);

    assertThat(resigned).isTrue();
    assertThat(election.isLeader(unified, TEST_ELECTION_KEY)).isFalse();
    assertThat(election.getCurrentLeader(unified, TEST_ELECTION_KEY)).isNull();
  }

  @Test
  public void testNonLeaderCannotResign() {
    LeaderElection election1 = createElection("node1");
    LeaderElection election2 = createElection("node2");

    election1.tryBecomeLeader(unified, TEST_ELECTION_KEY);
    boolean resigned = election2.resignLeadership(unified, TEST_ELECTION_KEY);

    assertThat(resigned).isFalse();
    assertThat(election1.isLeader(unified, TEST_ELECTION_KEY)).isTrue();
  }

  @Test
  public void testResignationAllowsNewLeader() {
    LeaderElection election1 = createElection("node1");
    LeaderElection election2 = createElection("node2");

    election1.tryBecomeLeader(unified, TEST_ELECTION_KEY);
    election1.resignLeadership(unified, TEST_ELECTION_KEY);

    boolean becameLeader = election2.tryBecomeLeader(unified, TEST_ELECTION_KEY);

    assertThat(becameLeader).isTrue();
    assertThat(election2.getCurrentLeader(unified, TEST_ELECTION_KEY)).isEqualTo("node2");
  }

  // ===== Callback Tests =====

  @Test
  public void testLeadershipAcquisitionCallback() throws InterruptedException {
    LeaderElection election = createElection("node1");
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger callbackCount = new AtomicInteger(0);

    election.addLeadershipCallback(
        (electionKey, isLeader) -> {
          if (isLeader && electionKey.equals(TEST_ELECTION_KEY)) {
            callbackCount.incrementAndGet();
            latch.countDown();
          }
        });

    election.tryBecomeLeader(unified, TEST_ELECTION_KEY);

    assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
    assertThat(callbackCount.get()).isEqualTo(1);
  }

  @Test
  public void testLeadershipLossCallback() throws InterruptedException {
    LeaderElection election =
        createElection("node1", VERY_SHORT_LEASE, VERY_SHORT_LEASE.dividedBy(3));
    CountDownLatch acquisitionLatch = new CountDownLatch(1);
    CountDownLatch lossLatch = new CountDownLatch(1);

    election.addLeadershipCallback(
        (electionKey, isLeader) -> {
          if (electionKey.equals(TEST_ELECTION_KEY)) {
            if (isLeader) {
              acquisitionLatch.countDown();
            } else {
              lossLatch.countDown();
            }
          }
        });

    election.tryBecomeLeader(unified, TEST_ELECTION_KEY);
    assertThat(acquisitionLatch.await(2, TimeUnit.SECONDS)).isTrue();

    // Close to stop auto-renewal and trigger loss
    election.close();

    // Wait a bit longer than the lease for detection
    assertThat(lossLatch.await(VERY_SHORT_LEASE.toMillis() * 2, TimeUnit.MILLISECONDS)).isTrue();
  }

  @Test
  public void testLeadershipResignationCallback() throws InterruptedException {
    LeaderElection election = createElection("node1");
    CountDownLatch lossLatch = new CountDownLatch(1);

    election.addLeadershipCallback(
        (electionKey, isLeader) -> {
          if (!isLeader && electionKey.equals(TEST_ELECTION_KEY)) {
            lossLatch.countDown();
          }
        });

    election.tryBecomeLeader(unified, TEST_ELECTION_KEY);
    election.resignLeadership(unified, TEST_ELECTION_KEY);

    assertThat(lossLatch.await(2, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  public void testMultipleCallbacks() throws InterruptedException {
    LeaderElection election = createElection("node1");
    CountDownLatch latch = new CountDownLatch(3); // 3 callbacks

    election.addLeadershipCallback((key, isLeader) -> latch.countDown());
    election.addLeadershipCallback((key, isLeader) -> latch.countDown());
    election.addLeadershipCallback((key, isLeader) -> latch.countDown());

    election.tryBecomeLeader(unified, TEST_ELECTION_KEY);

    assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  public void testRemoveCallback() throws InterruptedException {
    LeaderElection election = createElection("node1");
    AtomicInteger callbackCount = new AtomicInteger(0);

    BiConsumer<String, Boolean> callback = (key, isLeader) -> callbackCount.incrementAndGet();

    election.addLeadershipCallback(callback);
    election.removeLeadershipCallback(callback);

    election.tryBecomeLeader(unified, TEST_ELECTION_KEY);

    // Give callbacks time to execute (if they would)
    Thread.sleep(200);

    // Callback should not have been invoked
    assertThat(callbackCount.get()).isEqualTo(0);
  }

  // ===== Lifecycle Tests =====

  @Test
  public void testCloseResignsAllLeaderships() throws InterruptedException {
    LeaderElection election = createElection("node1");

    election.tryBecomeLeader(unified, "election-1");
    election.tryBecomeLeader(unified, "election-2");
    election.tryBecomeLeader(unified, "election-3");

    election.close();

    // Give time for resignations to complete
    Thread.sleep(200);

    assertThat(election.getCurrentLeader(unified, "election-1")).isNull();
    assertThat(election.getCurrentLeader(unified, "election-2")).isNull();
    assertThat(election.getCurrentLeader(unified, "election-3")).isNull();
    assertThat(election.isClosed()).isTrue();
  }

  @Test
  public void testOperationsAfterCloseReturnFalse() {
    LeaderElection election = createElection("node1");

    election.close();

    boolean becameLeader = election.tryBecomeLeader(unified, TEST_ELECTION_KEY);
    boolean isLeader = election.isLeader(unified, TEST_ELECTION_KEY);
    boolean renewed = election.renewLeadership(unified, TEST_ELECTION_KEY);
    boolean resigned = election.resignLeadership(unified, TEST_ELECTION_KEY);

    assertThat(becameLeader).isFalse();
    assertThat(isLeader).isFalse();
    assertThat(renewed).isFalse();
    assertThat(resigned).isFalse();
  }

  @Test
  public void testMultipleClosesAreIdempotent() {
    LeaderElection election = createElection("node1");

    election.close();
    election.close();
    election.close();

    assertThat(election.isClosed()).isTrue();
  }

  // ===== Redis Key Management Tests =====

  @Test
  public void testRedisKeyFormat() {
    LeaderElection election = createElection("node1");

    election.tryBecomeLeader(unified, "my-service");

    // Verify the key exists with correct prefix
    String value = redis.get("buildfarm:leader:my-service");
    assertThat(value).isNotNull();
    assertThat(value).startsWith("node1:");
  }

  @Test
  public void testDifferentElectionKeysAreSeparate() {
    LeaderElection election1 = createElection("node1");
    LeaderElection election2 = createElection("node2");

    election1.tryBecomeLeader(unified, "election-A");
    election2.tryBecomeLeader(unified, "election-B");

    assertThat(election1.isLeader(unified, "election-A")).isTrue();
    assertThat(election2.isLeader(unified, "election-B")).isTrue();
    assertThat(election1.isLeader(unified, "election-B")).isFalse();
    assertThat(election2.isLeader(unified, "election-A")).isFalse();
  }

  // ===== Edge Cases and Error Handling =====

  @Test
  public void testGetCurrentLeaderWhenNoLeader() {
    LeaderElection election = createElection("node1");

    String leader = election.getCurrentLeader(unified, "non-existent-election");

    assertThat(leader).isNull();
  }

  @Test
  public void testIsLeaderWhenNoLeader() {
    LeaderElection election = createElection("node1");

    boolean isLeader = election.isLeader(unified, "non-existent-election");

    assertThat(isLeader).isFalse();
  }

  @Test
  public void testRenewWhenNotLeader() {
    LeaderElection election = createElection("node1");

    boolean renewed = election.renewLeadership(unified, "non-existent-election");

    assertThat(renewed).isFalse();
  }

  @Test
  public void testResignWhenNotLeader() {
    LeaderElection election = createElection("node1");

    boolean resigned = election.resignLeadership(unified, "non-existent-election");

    assertThat(resigned).isFalse();
  }

  @Test
  public void testCallbackExceptionDoesNotAffectOtherCallbacks() throws InterruptedException {
    LeaderElection election = createElection("node1");
    CountDownLatch latch = new CountDownLatch(2);

    // First callback throws exception
    election.addLeadershipCallback(
        (key, isLeader) -> {
          throw new RuntimeException("Test exception");
        });

    // Second and third callbacks should still execute
    election.addLeadershipCallback((key, isLeader) -> latch.countDown());
    election.addLeadershipCallback((key, isLeader) -> latch.countDown());

    election.tryBecomeLeader(unified, TEST_ELECTION_KEY);

    assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
  }
}
