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

package build.buildfarm.common.redis;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;

import io.grpc.Status;
import io.grpc.Status.Code;
import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisConnectionException;

@RunWith(JUnit4.class)
public class RedisClientTest {
  @Test
  public void runExceptionEndOfStreamIsUnavailable() throws IOException, InterruptedException {
    RedisClient client = new RedisClient(mock(JedisCluster.class));
    Status status = Status.UNKNOWN;
    try {
      client.run(
          jedis -> {
            throw new JedisConnectionException("Unexpected end of stream.");
          });
    } catch (IOException e) {
      status = Status.fromThrowable(e);
    }
    assertThat(status.getCode()).isEqualTo(Code.UNAVAILABLE);
  }

  @Test
  public void runExceptionConnectionResetIsUnavailable() throws IOException, InterruptedException {
    RedisClient client = new RedisClient(mock(JedisCluster.class));
    Status status = Status.UNKNOWN;
    try {
      client.run(
          jedis -> {
            throw new JedisConnectionException(new SocketException("Connection reset"));
          });
    } catch (IOException e) {
      status = Status.fromThrowable(e);
    }
    assertThat(status.getCode()).isEqualTo(Code.UNAVAILABLE);
  }

  @Test
  public void runExceptionSocketTimeoutExceptionIsDeadlineExceeded()
      throws IOException, InterruptedException {
    RedisClient client = new RedisClient(mock(JedisCluster.class));
    Status status = Status.UNKNOWN;
    try {
      client.run(
          jedis -> {
            throw new JedisConnectionException(new SocketTimeoutException());
          });
    } catch (IOException e) {
      status = Status.fromThrowable(e);
    }
    assertThat(status.getCode()).isEqualTo(Code.DEADLINE_EXCEEDED);
  }
}
