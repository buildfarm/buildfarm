// Copyright 2018 The Buildfarm Authors. All rights reserved.
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.common.config.Queue;
import build.buildfarm.common.redis.BalancedRedisQueue;
import build.buildfarm.common.redis.Cluster;
import build.buildfarm.common.redis.ClusterPipeline;
import build.buildfarm.common.redis.RedisClient;
import build.buildfarm.common.redis.RedisHashMap;
import build.buildfarm.common.redis.RedisMap;
import build.buildfarm.instance.shard.ExecutionQueue.ExecutionQueueEntry;
import build.buildfarm.v1test.Digest;
import build.buildfarm.v1test.DispatchedOperation;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.OperationChange;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.ShardWorker;
import build.buildfarm.v1test.WorkerChange;
import build.buildfarm.worker.resources.LocalResourceSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import redis.clients.jedis.AbstractPipeline;
import redis.clients.jedis.UnifiedJedis;

@RunWith(JUnit4.class)
public class RedisShardBackplaneTest {
  private BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  @Mock Supplier<UnifiedJedis> mockJedisClusterFactory;

  @Before
  public void setUp() throws IOException {
    configs.getBackplane().setOperationExpire(10);
    configs.getBackplane().setQueues(new Queue[] {});
    MockitoAnnotations.initMocks(this);
  }

  public RedisShardBackplane createBackplane(String name) {
    return new RedisShardBackplane(
        name,
        /* subscribeToBackplane= */ false,
        /* runFailsafeOperation= */ false,
        o -> o,
        mockJedisClusterFactory);
  }

  @Test
  public void workersWithInvalidProtobufAreRemoved() throws IOException {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedis);
    when(jedis.hgetAll(configs.getBackplane().getWorkersHashName() + "_storage"))
        .thenReturn(ImmutableMap.of("foo", "foo"));
    when(jedis.hdel(configs.getBackplane().getWorkersHashName() + "_storage", "foo"))
        .thenReturn(1L);
    RedisShardBackplane backplane = createBackplane("invalid-protobuf-worker-removed-test");
    backplane.start("startTime/test:0000", name -> {});
    assertThat(backplane.getStorageWorkers()).isEmpty();
    verify(jedis, times(1)).hdel(configs.getBackplane().getWorkersHashName() + "_storage", "foo");
    ArgumentCaptor<String> changeCaptor = ArgumentCaptor.forClass(String.class);
    verify(jedis, times(1))
        .publish(eq(configs.getBackplane().getWorkerChannel()), changeCaptor.capture());
    String json = changeCaptor.getValue();
    WorkerChange.Builder builder = WorkerChange.newBuilder();
    JsonFormat.parser().merge(json, builder);
    WorkerChange workerChange = builder.build();
    assertThat(workerChange.getName()).isEqualTo("foo");
    assertThat(workerChange.getTypeCase()).isEqualTo(WorkerChange.TypeCase.REMOVE);
  }

  OperationChange verifyChangePublished(String channel, UnifiedJedis jedis) throws IOException {
    ArgumentCaptor<String> changeCaptor = ArgumentCaptor.forClass(String.class);
    verify(jedis, times(1)).publish(eq(channel), changeCaptor.capture());
    return parseOperationChange(changeCaptor.getValue());
  }

  OperationChange verifyChangePublished(String channel, AbstractPipeline pipeline)
      throws IOException {
    ArgumentCaptor<String> changeCaptor = ArgumentCaptor.forClass(String.class);
    verify(pipeline, times(1)).publish(eq(channel), changeCaptor.capture());
    return parseOperationChange(changeCaptor.getValue());
  }

  String operationName(String name) {
    return "Operation:" + name;
  }

  @Test
  public void prequeueUpdatesOperationPrequeuesAndPublishes() throws IOException {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    RedisClient client = new RedisClient(jedis);
    DistributedState state = new DistributedState();
    state.executions = mock(Executions.class);
    state.prequeue = mock(BalancedRedisQueue.class);
    RedisShardBackplane backplane = createBackplane("prequeue-operation-test");
    backplane.start(client, state, "startTime/test:0000", name -> {});
    DigestUtil digestUtil = new DigestUtil(HashFunction.SHA256);
    ByteString content = ByteString.copyFromUtf8("Action");
    Digest actionDigest = digestUtil.compute(content);

    final String opName = "op";
    ExecuteEntry executeEntry =
        ExecuteEntry.newBuilder().setActionDigest(actionDigest).setOperationName(opName).build();
    Operation op = Operation.newBuilder().setName(opName).build();
    when(state.executions.create(
            eq(jedis),
            eq(DigestUtil.asActionKey(actionDigest).toString()),
            eq(opName),
            eq(RedisShardBackplane.operationPrinter.print(op))))
        .thenReturn(true);

    assertThat(backplane.prequeue(executeEntry, op, /* ignoreMerge= */ false)).isTrue();

    verify(state.executions, times(1))
        .create(
            eq(jedis),
            eq(DigestUtil.asActionKey(actionDigest).toString()),
            eq(opName),
            eq(RedisShardBackplane.operationPrinter.print(op)));
    verifyNoMoreInteractions(state.executions);
    OperationChange opChange = verifyChangePublished(backplane.executionChannel(opName), jedis);
    assertThat(opChange.hasReset()).isTrue();
    assertThat(opChange.getReset().getOperation().getName()).isEqualTo(opName);
  }

  @Test
  public void queuingPublishes() throws IOException {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedis);
    RedisShardBackplane backplane = createBackplane("requeue-operation-test");
    backplane.start("startTime/test:0000", name -> {});
    final String opName = "op";
    backplane.queueing(opName);

    verify(mockJedisClusterFactory, times(1)).get();
    OperationChange opChange = verifyChangePublished(backplane.executionChannel(opName), jedis);
    assertThat(opChange.hasReset()).isTrue();
    assertThat(opChange.getReset().getOperation().getName()).isEqualTo(opName);
  }

  @Test
  public void requeueDispatchedExecutionQueuesAndPublishes() throws IOException {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    RedisClient client = new RedisClient(jedis);
    DistributedState state = new DistributedState();
    state.dispatchedExecutions = mock(RedisHashMap.class);
    state.executionQueue = mock(ExecutionQueue.class);
    RedisShardBackplane backplane = createBackplane("requeue-operation-test");
    backplane.start(client, state, "startTime/test:0000", name -> {});
    final String opName = "op";
    QueueEntry queueEntry =
        QueueEntry.newBuilder()
            .setExecuteEntry(ExecuteEntry.newBuilder().setOperationName(opName).build())
            .build();
    backplane.requeueDispatchedExecution(queueEntry);

    verify(state.dispatchedExecutions, times(1)).remove(jedis, opName);
    verifyNoMoreInteractions(state.dispatchedExecutions);
    verify(state.executionQueue, times(1))
        .push(
            jedis,
            queueEntry.getPlatform().getPropertiesList(),
            JsonFormat.printer().print(queueEntry),
            queueEntry.getExecuteEntry().getExecutionPolicy().getPriority());
    verifyNoMoreInteractions(state.executionQueue);
    OperationChange opChange = verifyChangePublished(backplane.executionChannel(opName), jedis);
    assertThat(opChange.hasReset()).isTrue();
    assertThat(opChange.getReset().getOperation().getName()).isEqualTo(opName);
  }

  @Test
  public void dispatchedOperationsShowProperRequeueAmount0to1()
      throws IOException, InterruptedException {
    // ARRANGE
    int STARTING_REQUEUE_AMOUNT = 0;
    int REQUEUE_AMOUNT_WHEN_DISPATCHED = 0;
    int REQUEUE_AMOUNT_WHEN_READY_TO_REQUEUE = 1;

    // create a backplane
    ClusterPipeline pipeline = mock(ClusterPipeline.class);
    Cluster jedis = mock(Cluster.class);
    when(jedis.pipelined(any(Executor.class))).thenReturn(pipeline);
    RedisClient client = new RedisClient(jedis);
    DistributedState state = new DistributedState();
    state.dispatchedExecutions = mock(RedisHashMap.class);
    state.dispatchingExecutions = mock(RedisMap.class);
    state.executionQueue = mock(ExecutionQueue.class);
    RedisShardBackplane backplane = createBackplane("requeue-operation-test");
    backplane.start(client, state, "startTime/test:0000", name -> {});
    // ARRANGE
    // Assume the operation queue is already populated with a first-time operation.
    // this means the operation's requeue amount will be 0.
    // The jedis cluser is also mocked to assume success on other operations.
    final String opName = "op";
    QueueEntry queueEntry =
        QueueEntry.newBuilder()
            .setExecuteEntry(ExecuteEntry.newBuilder().setOperationName(opName).build())
            .setRequeueAttempts(STARTING_REQUEUE_AMOUNT)
            .build();
    BalancedRedisQueue subQueue = mock(BalancedRedisQueue.class);
    ExecutionQueueEntry executionQueueEntry =
        new ExecutionQueueEntry(subQueue, /* balancedQueueEntry= */ null, queueEntry);
    when(state.executionQueue.dequeue(
            eq(jedis), any(List.class), any(LocalResourceSet.class), any(ExecutorService.class)))
        .thenReturn(executionQueueEntry);
    when(subQueue.removeFromDequeue(jedis, null)).thenReturn(true);
    // PRE-ASSERT
    when(state.dispatchedExecutions.insertIfMissing(eq(jedis), eq(opName), any(String.class)))
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

              return true;
            });

    // ACT
    // dispatch the operation and test properties of the QueueEntry and internal jedis calls.
    QueueEntry readyForRequeue =
        backplane.dispatchOperation(ImmutableList.of(), new LocalResourceSet());

    // ASSERT
    verify(jedis, times(1)).pipelined(any(Executor.class));
    OperationChange opChange = verifyChangePublished(backplane.executionChannel(opName), pipeline);
    assertThat(opChange.hasReset()).isTrue();
    assertThat(opChange.getReset().getOperation().getName()).isEqualTo(opName);
    verify(pipeline, times(1)).close(); // should happen *after* the change published...
    verifyNoMoreInteractions(pipeline);
    assertThat(readyForRequeue.getRequeueAttempts())
        .isEqualTo(REQUEUE_AMOUNT_WHEN_READY_TO_REQUEUE);
    verify(state.executionQueue, times(1))
        .dequeue(
            eq(jedis), any(List.class), any(LocalResourceSet.class), any(ExecutorService.class));
    verifyNoMoreInteractions(state.executionQueue);
    verify(subQueue, times(1)).removeFromDequeue(pipeline, null);
    verifyNoMoreInteractions(subQueue);
    verify(state.dispatchedExecutions, times(1))
        .insertIfMissing(
            eq(pipeline), eq(queueEntry.getExecuteEntry().getOperationName()), any(String.class));
    verifyNoMoreInteractions(state.dispatchedExecutions);
    verify(state.dispatchingExecutions, times(1))
        .remove(pipeline, queueEntry.getExecuteEntry().getOperationName());
    verifyNoMoreInteractions(state.dispatchingExecutions);
  }

  @Test
  public void dispatchedOperationsShowProperRequeueAmount1to2()
      throws IOException, InterruptedException {
    // ARRANGE
    int STARTING_REQUEUE_AMOUNT = 1;
    int REQUEUE_AMOUNT_WHEN_DISPATCHED = 1;
    int REQUEUE_AMOUNT_WHEN_READY_TO_REQUEUE = 2;

    // create a backplane
    ClusterPipeline pipeline = mock(ClusterPipeline.class);
    Cluster jedis = mock(Cluster.class);
    when(jedis.pipelined(any(Executor.class))).thenReturn(pipeline);
    RedisClient client = new RedisClient(jedis);
    DistributedState state = new DistributedState();
    state.dispatchedExecutions = mock(RedisHashMap.class);
    state.dispatchingExecutions = mock(RedisMap.class);
    state.executionQueue = mock(ExecutionQueue.class);
    RedisShardBackplane backplane = createBackplane("requeue-operation-test");
    backplane.start(client, state, "startTime/test:0000", name -> {});
    // ARRANGE
    // Assume the operation queue is already populated from a first re-queue.
    // this means the operation's requeue amount will be 1.
    // The jedis cluser is also mocked to assume success on other operations.
    final String opName = "op";
    QueueEntry queueEntry =
        QueueEntry.newBuilder()
            .setExecuteEntry(ExecuteEntry.newBuilder().setOperationName(opName).build())
            .setRequeueAttempts(STARTING_REQUEUE_AMOUNT)
            .build();
    BalancedRedisQueue subQueue = mock(BalancedRedisQueue.class);
    ExecutionQueueEntry executionQueueEntry =
        new ExecutionQueueEntry(subQueue, /* balancedQueueEntry= */ null, queueEntry);
    when(state.executionQueue.dequeue(
            eq(jedis), any(List.class), any(LocalResourceSet.class), any(ExecutorService.class)))
        .thenReturn(executionQueueEntry);
    when(state.executionQueue.removeFromDequeue(jedis, executionQueueEntry)).thenReturn(true);
    // PRE-ASSERT
    when(state.dispatchedExecutions.insertIfMissing(eq(jedis), eq(opName), any(String.class)))
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

              return true;
            });

    // ACT
    // dispatch the operation and test properties of the QueueEntry and internal jedis calls.
    QueueEntry readyForRequeue =
        backplane.dispatchOperation(ImmutableList.of(), new LocalResourceSet());

    // ASSERT
    verify(jedis, times(1)).pipelined(any(Executor.class));
    OperationChange opChange = verifyChangePublished(backplane.executionChannel(opName), pipeline);
    assertThat(opChange.hasReset()).isTrue();
    assertThat(opChange.getReset().getOperation().getName()).isEqualTo(opName);
    verify(pipeline, times(1)).close(); // should happen *after* the change published...
    verifyNoMoreInteractions(pipeline);
    assertThat(readyForRequeue.getRequeueAttempts())
        .isEqualTo(REQUEUE_AMOUNT_WHEN_READY_TO_REQUEUE);
    verify(state.executionQueue, times(1))
        .dequeue(
            eq(jedis), any(List.class), any(LocalResourceSet.class), any(ExecutorService.class));
    verifyNoMoreInteractions(state.executionQueue);
    verify(subQueue, times(1)).removeFromDequeue(pipeline, null);
    verifyNoMoreInteractions(subQueue);
    verify(state.dispatchedExecutions, times(1))
        .insertIfMissing(
            eq(pipeline), eq(queueEntry.getExecuteEntry().getOperationName()), any(String.class));
    verifyNoMoreInteractions(state.dispatchedExecutions);
    verify(state.dispatchingExecutions, times(1))
        .remove(pipeline, queueEntry.getExecuteEntry().getOperationName());
    verifyNoMoreInteractions(state.dispatchingExecutions);
  }

  @Test
  public void completeOperationUndispatches() throws IOException {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedis);
    RedisShardBackplane backplane = createBackplane("complete-operation-test");
    backplane.start("startTime/test:0000", name -> {});
    final String opName = "op";

    backplane.completeOperation(opName);

    verify(mockJedisClusterFactory, times(1)).get();
    verify(jedis, times(1)).hdel(configs.getBackplane().getDispatchedOperationsHashName(), opName);
  }

  @Test
  @Ignore
  public void deleteOperationDeletesAndPublishes() throws IOException {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedis);
    RedisShardBackplane backplane = createBackplane("delete-operation-test");
    backplane.start("startTime/test:0000", name -> {});
    final String opName = "op";

    backplane.deleteOperation(opName);

    verify(mockJedisClusterFactory, times(1)).get();
    verify(jedis, times(1)).hdel(configs.getBackplane().getDispatchedOperationsHashName(), opName);
    verify(jedis, times(1)).del(operationName(opName));
    OperationChange opChange = verifyChangePublished(backplane.executionChannel(opName), jedis);
    assertThat(opChange.hasReset()).isTrue();
    assertThat(opChange.getReset().getOperation().getName()).isEqualTo(opName);
  }

  @Test
  public void invocationsCanBeBlacklisted() throws IOException {
    UUID toolInvocationId = UUID.randomUUID();
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    String invocationBlacklistKey =
        configs.getBackplane().getInvocationBlacklistPrefix() + ":" + toolInvocationId;
    when(jedis.exists(invocationBlacklistKey)).thenReturn(true);
    when(mockJedisClusterFactory.get()).thenReturn(jedis);
    RedisShardBackplane backplane = createBackplane("invocation-blacklist-test");
    backplane.start("startTime/test:0000", name -> {});
    assertThat(
            backplane.isBlacklisted(
                RequestMetadata.newBuilder()
                    .setToolInvocationId(toolInvocationId.toString())
                    .build()))
        .isTrue();

    verify(mockJedisClusterFactory, times(1)).get();
    verify(jedis, times(1)).exists(invocationBlacklistKey);
  }

  @Test
  public void testGetWorkersStartTime() throws IOException {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedis);
    RedisShardBackplane backplane = createBackplane("workers-starttime-test");
    backplane.start("startTime/test:0000", name -> {});
    Set<String> workerNames = ImmutableSet.of("worker1", "worker2", "missing_worker");

    String storageWorkerKey = configs.getBackplane().getWorkersHashName() + "_storage";
    Map<String, String> workersJson =
        Map.of(
            "worker1",
                "{\"endpoint\": \"worker1\", \"expireAt\": \"9999999999999\", \"workerType\": 3,"
                    + " \"firstRegisteredAt\": \"1685292624000\"}",
            "worker2",
                "{\"endpoint\": \"worker2\", \"expireAt\": \"9999999999999\", \"workerType\": 3,"
                    + " \"firstRegisteredAt\": \"1685282624000\"}");
    when(jedis.hgetAll(storageWorkerKey)).thenReturn(workersJson);
    Map<String, Long> workersStartTime = backplane.getWorkersStartTimeInEpochSecs(workerNames);
    assertThat(workersStartTime.size()).isEqualTo(2);
    assertThat(workersStartTime.get("worker1")).isEqualTo(1685292624L);
    assertThat(workersStartTime.get("worker2")).isEqualTo(1685282624L);
    assertThat(workersStartTime.get("missing_worker")).isNull();
  }

  @Test
  public void getDigestInsertTime() throws IOException {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedis);
    RedisShardBackplane backplane = createBackplane("digest-inserttime-test");
    backplane.start("startTime/test:0000", name -> {});
    long ttl = 3600L;
    long expirationInSecs = configs.getBackplane().getCasExpire();
    when(jedis.ttl("ContentAddressableStorage:abc/0")).thenReturn(ttl);

    Digest digest = Digest.newBuilder().setHash("abc").build();

    Long insertTimeInSecs = backplane.getDigestInsertTime(digest);

    // Assuming there could be at most 2s delay in execution of both
    // `Instant.now().getEpochSecond()` call.
    assertThat(insertTimeInSecs)
        .isGreaterThan(Instant.now().getEpochSecond() - expirationInSecs + ttl - 2);
    assertThat(insertTimeInSecs).isAtMost(Instant.now().getEpochSecond() - expirationInSecs + ttl);
  }

  @Test
  public void testAddWorker() throws IOException {
    ShardWorker shardWorker =
        ShardWorker.newBuilder().setWorkerType(3).setFirstRegisteredAt(1703065913000L).build();
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedis);
    when(jedis.hset(anyString(), anyString(), anyString())).thenReturn(1L);
    RedisShardBackplane backplane = createBackplane("add-worker-test");
    backplane.start("addWorker/test:0000", name -> {});
    backplane.addWorker(shardWorker);
    verify(jedis, times(1))
        .hset(
            configs.getBackplane().getWorkersHashName() + "_storage",
            "",
            JsonFormat.printer().print(shardWorker));
    verify(jedis, times(1))
        .hset(
            configs.getBackplane().getWorkersHashName() + "_execute",
            "",
            JsonFormat.printer().print(shardWorker));
    verify(jedis, times(1)).publish(anyString(), anyString());
  }
}
