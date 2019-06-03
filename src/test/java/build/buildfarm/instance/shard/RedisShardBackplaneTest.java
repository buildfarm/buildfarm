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

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import build.buildfarm.v1test.RedisShardBackplaneConfig;
import com.google.common.collect.ImmutableMap;
import io.grpc.Status;
import io.grpc.Status.Code;
import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisClusterPipeline;
import redis.clients.jedis.exceptions.JedisConnectionException;

@RunWith(JUnit4.class)
public class RedisShardBackplaneTest {
  private RedisShardBackplane backplane;

  @Mock
  Supplier<JedisCluster> mockJedisClusterFactory;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void withBackplaneExceptionEndOfStreamIsUnavailable() throws IOException, InterruptedException {
    when(mockJedisClusterFactory.get()).thenReturn(mock(JedisCluster.class));
    backplane = new RedisShardBackplane(
        RedisShardBackplaneConfig.getDefaultInstance(),
        "end-of-stream-unavailable-test",
        (o) -> o,
        (o) -> o,
        (o) -> false,
        (o) -> false,
        mockJedisClusterFactory);
    backplane.start();
    Status status = Status.UNKNOWN;
    try {
      backplane.withVoidBackplaneException((jedis) -> { throw new JedisConnectionException("Unexpected end of stream."); });
    } catch (IOException e) {
      status = Status.fromThrowable(e);
    }
    assertThat(status.getCode()).isEqualTo(Code.UNAVAILABLE);
  }

  @Test
  public void withBackplaneExceptionConnectionResetIsUnavailable() throws IOException, InterruptedException {
    when(mockJedisClusterFactory.get()).thenReturn(mock(JedisCluster.class));
    backplane = new RedisShardBackplane(
        RedisShardBackplaneConfig.getDefaultInstance(),
        "connection-reset-unavailable-test",
        (o) -> o,
        (o) -> o,
        (o) -> false,
        (o) -> false,
        mockJedisClusterFactory);
    backplane.start();
    Status status = Status.UNKNOWN;
    try {
      backplane.withVoidBackplaneException((jedis) -> { throw new JedisConnectionException(new SocketException("Connection reset")); });
    } catch (IOException e) {
      status = Status.fromThrowable(e);
    }
    assertThat(status.getCode()).isEqualTo(Code.UNAVAILABLE);
  }

  @Test
  public void withBackplaneExceptionSocketTimeoutExceptionIsDeadlineExceeded()
      throws IOException, InterruptedException {
    when(mockJedisClusterFactory.get()).thenReturn(mock(JedisCluster.class));
    backplane = new RedisShardBackplane(
        RedisShardBackplaneConfig.getDefaultInstance(),
        "socket-timeout-exception-is-deadline-exceeded-test",
        (o) -> o,
        (o) -> o,
        (o) -> false,
        (o) -> false,
        mockJedisClusterFactory);
    backplane.start();
    Status status = Status.UNKNOWN;
    try {
      backplane.withVoidBackplaneException((jedis) -> { throw new JedisConnectionException(new SocketTimeoutException()); });
    } catch (IOException e) {
      status = Status.fromThrowable(e);
    }
    assertThat(status.getCode()).isEqualTo(Code.DEADLINE_EXCEEDED);
  }

  @Test
  public void workersWithInvalidProtobufAreRemoved() throws IOException {
    RedisShardBackplaneConfig config = RedisShardBackplaneConfig.newBuilder()
        .setWorkersHashName("Workers")
        .build();
    JedisCluster jedisCluster = mock(JedisCluster.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedisCluster);
    when(jedisCluster.hgetAll(config.getWorkersHashName())).thenReturn(ImmutableMap.of("foo", "foo"));
    JedisClusterPipeline pipeline = mock(JedisClusterPipeline.class);
    when(jedisCluster.pipelined()).thenReturn(pipeline);
    backplane = new RedisShardBackplane(
        config,
        "invalid-protobuf-worker-removed-test",
        (o) -> o,
        (o) -> o,
        (o) -> false,
        (o) -> false,
        mockJedisClusterFactory);
    backplane.start();

    assertThat(backplane.getWorkers()).isEmpty();
    verify(jedisCluster, times(1)).pipelined();
    verify(pipeline, times(1)).hdel(config.getWorkersHashName(), "foo");
    verify(pipeline, times(1)).sync();
  }
}
