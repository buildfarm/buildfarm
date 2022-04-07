// Copyright 2018 The Bazel Authors. All rights reserved.
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

package build.buildfarm.instance.shard;

import static build.buildfarm.instance.shard.RedisShardBackplane.parseOperationChange;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.v1test.DispatchedOperation;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.OperationChange;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.RedisShardBackplaneConfig;
import build.buildfarm.v1test.WorkerChange;
import com.google.common.collect.ImmutableMap;
import com.google.longrunning.Operation;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import redis.clients.jedis.JedisCluster;

@RunWith(JUnit4.class)
public class RedisShardBackplaneTest {
  private RedisShardBackplane backplane;

  @Mock Supplier<JedisCluster> mockJedisClusterFactory;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void workersWithInvalidProtobufAreRemoved() throws IOException {
    RedisShardBackplaneConfig config =
        RedisShardBackplaneConfig.newBuilder()
            .setWorkersHashName("Workers")
            .setWorkerChannel("WorkerChannel")
            .build();
    JedisCluster jedisCluster = mock(JedisCluster.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedisCluster);
    when(jedisCluster.hgetAll(config.getWorkersHashName()))
        .thenReturn(ImmutableMap.of("foo", "foo"));
    when(jedisCluster.hdel(config.getWorkersHashName(), "foo")).thenReturn(1L);
    backplane =
        new RedisShardBackplane(
            config,
            "invalid-protobuf-worker-removed-test",
            (o) -> o,
            (o) -> o,
            mockJedisClusterFactory);
    backplane.start("startTime/test:0000");

    assertThat(backplane.getWorkers()).isEmpty();
    verify(jedisCluster, times(1)).hdel(config.getWorkersHashName(), "foo");
    ArgumentCaptor<String> changeCaptor = ArgumentCaptor.forClass(String.class);
    verify(jedisCluster, times(1)).publish(eq(config.getWorkerChannel()), changeCaptor.capture());
    String json = changeCaptor.getValue();
    WorkerChange.Builder builder = WorkerChange.newBuilder();
    JsonFormat.parser().merge(json, builder);
    WorkerChange workerChange = builder.build();
    assertThat(workerChange.getName()).isEqualTo("foo");
    assertThat(workerChange.getTypeCase()).isEqualTo(WorkerChange.TypeCase.REMOVE);
  }

  void verifyChangePublished(JedisCluster jedis) throws IOException {
    ArgumentCaptor<String> changeCaptor = ArgumentCaptor.forClass(String.class);
    verify(jedis, times(1)).publish(eq(backplane.operationChannel("op")), changeCaptor.capture());
    OperationChange opChange = parseOperationChange(changeCaptor.getValue());
    assertThat(opChange.hasReset()).isTrue();
    assertThat(opChange.getReset().getOperation().getName()).isEqualTo("op");
  }

  @Test
  public void prequeueUpdatesOperationPrequeuesAndPublishes() throws IOException {
    RedisShardBackplaneConfig config =
        RedisShardBackplaneConfig.newBuilder()
            .setOperationChannelPrefix("OperationChannel")
            .setOperationExpire(10)
            .setOperationPrefix("Operation")
            .setPreQueuedOperationsListName("{hash}PreQueuedOperations")
            .build();
    JedisCluster jedisCluster = mock(JedisCluster.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedisCluster);
    backplane =
        new RedisShardBackplane(
            config, "prequeue-operation-test", (o) -> o, (o) -> o, mockJedisClusterFactory);
    backplane.start("startTime/test:0000");

    final String opName = "op";
    ExecuteEntry executeEntry = ExecuteEntry.newBuilder().setOperationName(opName).build();
    Operation op = Operation.newBuilder().setName(opName).build();
    backplane.prequeue(executeEntry, op);

    verify(mockJedisClusterFactory, times(1)).get();
    verify(jedisCluster, times(1))
        .setex(
            backplane.operationKey(opName),
            config.getOperationExpire(),
            RedisShardBackplane.operationPrinter.print(op));
    verify(jedisCluster, times(1))
        .lpush(config.getPreQueuedOperationsListName(), JsonFormat.printer().print(executeEntry));
    verifyChangePublished(jedisCluster);
  }

  @Test
  public void queuingPublishes() throws IOException {
    RedisShardBackplaneConfig config =
        RedisShardBackplaneConfig.newBuilder()
            .setOperationChannelPrefix("OperationChannel")
            .build();
    JedisCluster jedisCluster = mock(JedisCluster.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedisCluster);
    backplane =
        new RedisShardBackplane(
            config, "requeue-operation-test", (o) -> o, (o) -> o, mockJedisClusterFactory);
    backplane.start("startTime/test:0000");

    final String opName = "op";
    backplane.queueing(opName);

    verify(mockJedisClusterFactory, times(1)).get();
    verifyChangePublished(jedisCluster);
  }

  @Test
  public void requeueDispatchedOperationQueuesAndPublishes() throws IOException {
    RedisShardBackplaneConfig config =
        RedisShardBackplaneConfig.newBuilder()
            .setDispatchedOperationsHashName("DispatchedOperations")
            .setOperationChannelPrefix("OperationChannel")
            .setQueuedOperationsListName("{hash}QueuedOperations")
            .build();
    JedisCluster jedisCluster = mock(JedisCluster.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedisCluster);
    backplane =
        new RedisShardBackplane(
            config, "requeue-operation-test", (o) -> o, (o) -> o, mockJedisClusterFactory);
    backplane.start("startTime/test:0000");

    final String opName = "op";
    when(jedisCluster.hdel(config.getDispatchedOperationsHashName(), opName)).thenReturn(1L);

    QueueEntry queueEntry =
        QueueEntry.newBuilder()
            .setExecuteEntry(ExecuteEntry.newBuilder().setOperationName("op").build())
            .build();
    backplane.requeueDispatchedOperation(queueEntry);

    verify(mockJedisClusterFactory, times(1)).get();
    verify(jedisCluster, times(1)).hdel(config.getDispatchedOperationsHashName(), opName);
    verify(jedisCluster, times(1))
        .lpush(config.getQueuedOperationsListName(), JsonFormat.printer().print(queueEntry));
    verifyChangePublished(jedisCluster);
  }

  @Test
  public void dispatchedOperationsShowProperRequeueAmount0to1()
      throws IOException, InterruptedException {
    // ARRANGE
    int STARTING_REQUEUE_AMOUNT = 0;
    int REQUEUE_AMOUNT_WHEN_DISPATCHED = 0;
    int REQUEUE_AMOUNT_WHEN_READY_TO_REQUEUE = 1;

    // create a backplane
    RedisShardBackplaneConfig config =
        RedisShardBackplaneConfig.newBuilder()
            .setDispatchedOperationsHashName("DispatchedOperations")
            .setOperationChannelPrefix("OperationChannel")
            .setQueuedOperationsListName("{hash}QueuedOperations")
            .build();
    JedisCluster jedisCluster = mock(JedisCluster.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedisCluster);
    backplane =
        new RedisShardBackplane(
            config, "requeue-operation-test", (o) -> o, (o) -> o, mockJedisClusterFactory);
    backplane.start("startTime/test:0000");

    // ARRANGE
    // Assume the operation queue is already populated with a first-time operation.
    // this means the operation's requeue amount will be 0.
    // The jedis cluser is also mocked to assume success on other operations.
    QueueEntry queueEntry =
        QueueEntry.newBuilder()
            .setExecuteEntry(ExecuteEntry.newBuilder().setOperationName("op").build())
            .setRequeueAttempts(STARTING_REQUEUE_AMOUNT)
            .build();
    String queueEntryJson = JsonFormat.printer().print(queueEntry);
    when(jedisCluster.brpoplpush(any(String.class), any(String.class), any(Integer.class)))
        .thenReturn(queueEntryJson);

    // PRE-ASSERT
    when(jedisCluster.hsetnx(any(String.class), any(String.class), any(String.class)))
        .thenAnswer(
            args -> {
              // Extract the operation that was dispatched
              String dispatchedOperationJson = args.getArgument(2);
              DispatchedOperation.Builder dispatchedOperationBuilder =
                  DispatchedOperation.newBuilder();
              JsonFormat.parser().merge(dispatchedOperationJson, dispatchedOperationBuilder);
              DispatchedOperation dispatchedOperation = dispatchedOperationBuilder.build();

              assertThat(dispatchedOperation.getQueueEntry().getRequeueAttempts())
                  .isEqualTo(REQUEUE_AMOUNT_WHEN_DISPATCHED);

              return 1L;
            });

    // ACT
    // dispatch the operation and test properties of the QueueEntry and internal jedis calls.
    List<Platform.Property> properties = new ArrayList<>();
    QueueEntry readyForRequeue = backplane.dispatchOperation(properties);

    // ASSERT
    assertThat(readyForRequeue.getRequeueAttempts())
        .isEqualTo(REQUEUE_AMOUNT_WHEN_READY_TO_REQUEUE);
  }

  @Test
  public void dispatchedOperationsShowProperRequeueAmount1to2()
      throws IOException, InterruptedException {
    // ARRANGE
    int STARTING_REQUEUE_AMOUNT = 1;
    int REQUEUE_AMOUNT_WHEN_DISPATCHED = 1;
    int REQUEUE_AMOUNT_WHEN_READY_TO_REQUEUE = 2;

    // create a backplane
    RedisShardBackplaneConfig config =
        RedisShardBackplaneConfig.newBuilder()
            .setDispatchedOperationsHashName("DispatchedOperations")
            .setOperationChannelPrefix("OperationChannel")
            .setQueuedOperationsListName("{hash}QueuedOperations")
            .build();
    JedisCluster jedisCluster = mock(JedisCluster.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedisCluster);
    backplane =
        new RedisShardBackplane(
            config, "requeue-operation-test", (o) -> o, (o) -> o, mockJedisClusterFactory);
    backplane.start("startTime/test:0000");

    // Assume the operation queue is already populated from a first re-queue.
    // this means the operation's requeue amount will be 1.
    // The jedis cluser is also mocked to assume success on other operations.
    QueueEntry queueEntry =
        QueueEntry.newBuilder()
            .setExecuteEntry(ExecuteEntry.newBuilder().setOperationName("op").build())
            .setRequeueAttempts(STARTING_REQUEUE_AMOUNT)
            .build();
    String queueEntryJson = JsonFormat.printer().print(queueEntry);
    when(jedisCluster.brpoplpush(any(String.class), any(String.class), any(Integer.class)))
        .thenReturn(queueEntryJson);

    // PRE-ASSERT
    when(jedisCluster.hsetnx(any(String.class), any(String.class), any(String.class)))
        .thenAnswer(
            args -> {
              // Extract the operation that was dispatched
              String dispatchedOperationJson = args.getArgument(2);
              DispatchedOperation.Builder dispatchedOperationBuilder =
                  DispatchedOperation.newBuilder();
              JsonFormat.parser().merge(dispatchedOperationJson, dispatchedOperationBuilder);
              DispatchedOperation dispatchedOperation = dispatchedOperationBuilder.build();

              assertThat(dispatchedOperation.getQueueEntry().getRequeueAttempts())
                  .isEqualTo(REQUEUE_AMOUNT_WHEN_DISPATCHED);

              return 1L;
            });

    // ACT
    // dispatch the operation and test properties of the QueueEntry and internal jedis calls.
    List<Platform.Property> properties = new ArrayList<>();
    QueueEntry readyForRequeue = backplane.dispatchOperation(properties);

    // ASSERT
    assertThat(readyForRequeue.getRequeueAttempts())
        .isEqualTo(REQUEUE_AMOUNT_WHEN_READY_TO_REQUEUE);
  }

  @Test
  public void completeOperationUndispatches() throws IOException {
    RedisShardBackplaneConfig config =
        RedisShardBackplaneConfig.newBuilder()
            .setDispatchedOperationsHashName("DispatchedOperations")
            .build();
    JedisCluster jedisCluster = mock(JedisCluster.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedisCluster);
    backplane =
        new RedisShardBackplane(
            config, "complete-operation-test", (o) -> o, (o) -> o, mockJedisClusterFactory);
    backplane.start("startTime/test:0000");

    final String opName = "op";

    backplane.completeOperation(opName);

    verify(mockJedisClusterFactory, times(1)).get();
    verify(jedisCluster, times(1)).hdel(config.getDispatchedOperationsHashName(), opName);
  }

  @org.junit.Ignore
  @Test
  public void deleteOperationDeletesAndPublishes() throws IOException {
    RedisShardBackplaneConfig config =
        RedisShardBackplaneConfig.newBuilder()
            .setDispatchedOperationsHashName("DispatchedOperations")
            .setOperationPrefix("Operation")
            .setOperationChannelPrefix("OperationChannel")
            .build();
    JedisCluster jedisCluster = mock(JedisCluster.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedisCluster);
    backplane =
        new RedisShardBackplane(
            config, "delete-operation-test", (o) -> o, (o) -> o, mockJedisClusterFactory);
    backplane.start("startTime/test:0000");

    final String opName = "op";

    backplane.deleteOperation(opName);

    verify(mockJedisClusterFactory, times(1)).get();
    verify(jedisCluster, times(1)).hdel(config.getDispatchedOperationsHashName(), opName);
    verify(jedisCluster, times(1)).del(backplane.operationKey(opName));
    verifyChangePublished(jedisCluster);
  }

  @Test
  public void invocationsCanBeBlacklisted() throws IOException {
    RedisShardBackplaneConfig config =
        RedisShardBackplaneConfig.newBuilder()
            .setInvocationBlacklistPrefix("InvocationBlacklist")
            .build();
    UUID toolInvocationId = UUID.randomUUID();
    JedisCluster jedisCluster = mock(JedisCluster.class);
    String invocationBlacklistKey = config.getInvocationBlacklistPrefix() + ":" + toolInvocationId;
    when(jedisCluster.exists(invocationBlacklistKey)).thenReturn(true);
    when(mockJedisClusterFactory.get()).thenReturn(jedisCluster);
    backplane =
        new RedisShardBackplane(
            config, "invocation-blacklist-test", o -> o, o -> o, mockJedisClusterFactory);
    backplane.start("startTime/test:0000");

    assertThat(
            backplane.isBlacklisted(
                RequestMetadata.newBuilder()
                    .setToolInvocationId(toolInvocationId.toString())
                    .build()))
        .isTrue();

    verify(mockJedisClusterFactory, times(1)).get();
    verify(jedisCluster, times(1)).exists(invocationBlacklistKey);
  }
}
