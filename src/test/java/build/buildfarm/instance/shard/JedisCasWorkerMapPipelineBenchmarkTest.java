// Copyright 2024 The Buildfarm Authors. All rights reserved.
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

import build.buildfarm.common.DigestUtil;
import build.buildfarm.v1test.Digest;
import com.github.fppt.jedismock.RedisServer;
import com.github.fppt.jedismock.server.ServiceOptions;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import redis.clients.jedis.AbstractPipeline;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.UnifiedJedis;

/**
 * Benchmark comparing non-pipelined (sequential) vs pipelined Redis operations for batch workloads.
 *
 * <p>This test compares two strategies for performing N Redis operations:
 *
 * <ul>
 *   <li><b>Sequential</b>: Issues each Redis command individually (N round-trips)
 *   <li><b>Pipelined</b>: Batches all commands into a single pipeline (1 round-trip)
 * </ul>
 *
 * <p>Uses jedis-mock (in-process), so absolute timings are smaller than production. With real
 * network latency (e.g. 0.1-1ms per round-trip), the speedup is dramatically larger.
 */
@RunWith(JUnit4.class)
public class JedisCasWorkerMapPipelineBenchmarkTest {
  private static final Logger log =
      Logger.getLogger(JedisCasWorkerMapPipelineBenchmarkTest.class.getName());

  private static final String CAS_PREFIX = "ContentAddressableStorage";
  private static final int KEY_EXPIRATION_S = 3600;
  private static final int DIGEST_COUNT = 200;
  private static final int WORKERS_PER_DIGEST = 3;
  private static final int WARMUP_ITERATIONS = 2;
  private static final int MEASURED_ITERATIONS = 3;

  private RedisServer redisServer;
  private JedisCluster jedis;

  @Before
  public void setup() throws IOException {
    redisServer =
        RedisServer.newRedisServer(0, InetAddress.getByName("localhost"))
            .setOptions(ServiceOptions.defaultOptions().withClusterModeEnabled())
            .start();
    jedis =
        new JedisCluster(
            Collections.singleton(
                new HostAndPort(redisServer.getHost(), redisServer.getBindPort())));
  }

  @After
  public void tearDown() throws IOException {
    if (jedis != null) {
      jedis.close();
    }
    if (redisServer != null) {
      redisServer.stop();
    }
  }

  // ---- Helpers ----

  private List<Digest> generateDigests(int count) {
    List<Digest> digests = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      digests.add(Digest.newBuilder().setHash("hash_" + i).setSize(i + 1).build());
    }
    return digests;
  }

  private String casKey(Digest d) {
    return CAS_PREFIX + ":" + DigestUtil.toString(d);
  }

  private void seedData(List<Digest> digests) {
    try (AbstractPipeline p = jedis.pipelined()) {
      for (Digest d : digests) {
        String key = casKey(d);
        for (int w = 0; w < WORKERS_PER_DIGEST; w++) {
          p.sadd(key, "worker_" + w);
        }
        p.expire(key, KEY_EXPIRATION_S);
      }
    }
  }

  private void flushData(List<Digest> digests) {
    try (AbstractPipeline p = jedis.pipelined()) {
      for (Digest d : digests) {
        p.del(casKey(d));
      }
    }
  }

  // ---- Sequential (old) batch implementations ----

  /** Old addAll: one SADD + EXPIRE per digest, sequential. */
  private void sequentialAddAll(UnifiedJedis jedis, Iterable<Digest> digests, String workerName) {
    for (Digest d : digests) {
      String key = casKey(d);
      jedis.sadd(key, workerName);
      jedis.expire(key, KEY_EXPIRATION_S);
    }
  }

  /** Old removeAll: one SREM per digest, sequential. */
  private void sequentialRemoveAll(
      UnifiedJedis jedis, Iterable<Digest> digests, String workerName) {
    for (Digest d : digests) {
      jedis.srem(casKey(d), workerName);
    }
  }

  /** Old getMap: one SMEMBERS per digest, sequential. */
  private Map<Digest, Set<String>> sequentialGetMap(UnifiedJedis jedis, Iterable<Digest> digests) {
    Map<Digest, Set<String>> result = new HashMap<>();
    for (Digest d : digests) {
      Set<String> workers = jedis.smembers(casKey(d));
      if (!workers.isEmpty()) {
        result.put(d, workers);
      }
    }
    return result;
  }

  /** Old setExpire: one EXPIRE per digest, sequential. */
  private void sequentialSetExpire(UnifiedJedis jedis, Iterable<Digest> digests) {
    for (Digest d : digests) {
      jedis.expire(casKey(d), KEY_EXPIRATION_S);
    }
  }

  // ---- Benchmark harness ----

  @FunctionalInterface
  private interface BenchmarkAction {
    void run();
  }

  private long benchmark(String label, BenchmarkAction action) {
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      action.run();
    }
    long totalNs = 0;
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      long start = System.nanoTime();
      action.run();
      totalNs += System.nanoTime() - start;
    }
    long avgNs = totalNs / MEASURED_ITERATIONS;
    log.info(String.format("  %-40s avg %,12d ns  (%,.2f ms)", label, avgNs, avgNs / 1_000_000.0));
    return avgNs;
  }

  private void logSpeedup(String operation, long seqNs, long pipNs) {
    double speedup = (double) seqNs / pipNs;
    log.info(
        String.format(
            "  >> %-12s speedup: %5.2fx  (seq: %,.2f ms, pip: %,.2f ms)",
            operation, speedup, seqNs / 1_000_000.0, pipNs / 1_000_000.0));
    log.info("");
  }

  // ---- Benchmark tests ----

  @Test
  public void addAllBatchBenchmark() {
    List<Digest> digests = generateDigests(DIGEST_COUNT);
    JedisCasWorkerMap pipelinedMap = new JedisCasWorkerMap(jedis, CAS_PREFIX, KEY_EXPIRATION_S);

    log.info("=== addAll() batch benchmark — " + DIGEST_COUNT + " digests ===");

    long seqNs =
        benchmark(
            "sequential (SADD+EXPIRE × N)", () -> sequentialAddAll(jedis, digests, "bench_worker"));

    long pipNs =
        benchmark(
            "pipelined (single pipeline)", () -> pipelinedMap.addAll(digests, "bench_worker"));

    logSpeedup("addAll", seqNs, pipNs);

    // Verify correctness
    for (Digest d : digests.subList(0, 5)) {
      assertThat(jedis.sismember(casKey(d), "bench_worker")).isTrue();
    }
    flushData(digests);
  }

  @Test
  public void removeAllBatchBenchmark() {
    List<Digest> digests = generateDigests(DIGEST_COUNT);
    JedisCasWorkerMap pipelinedMap = new JedisCasWorkerMap(jedis, CAS_PREFIX, KEY_EXPIRATION_S);

    log.info("=== removeAll() batch benchmark — " + DIGEST_COUNT + " digests ===");

    // Seed and benchmark sequential
    long seqNs =
        benchmark(
            "sequential (SREM × N)",
            () -> {
              seedData(digests);
              sequentialRemoveAll(jedis, digests, "worker_0");
            });

    // Seed and benchmark pipelined
    long pipNs =
        benchmark(
            "pipelined (single pipeline)",
            () -> {
              seedData(digests);
              pipelinedMap.removeAll(digests, "worker_0");
            });

    logSpeedup("removeAll", seqNs, pipNs);
    flushData(digests);
  }

  @Test
  public void getMapBatchBenchmark() {
    List<Digest> digests = generateDigests(DIGEST_COUNT);
    seedData(digests);
    JedisCasWorkerMap pipelinedMap = new JedisCasWorkerMap(jedis, CAS_PREFIX, KEY_EXPIRATION_S);

    log.info("=== getMap() batch benchmark — " + DIGEST_COUNT + " digests ===");

    long seqNs = benchmark("sequential (SMEMBERS × N)", () -> sequentialGetMap(jedis, digests));

    long pipNs = benchmark("pipelined (single pipeline)", () -> pipelinedMap.getMap(digests));

    logSpeedup("getMap", seqNs, pipNs);

    // Verify correctness: both return same results
    Map<Digest, Set<String>> seqResult = sequentialGetMap(jedis, digests);
    Map<Digest, Set<String>> pipResult = pipelinedMap.getMap(digests);
    assertThat(pipResult).isEqualTo(seqResult);
    assertThat(pipResult).hasSize(DIGEST_COUNT);

    flushData(digests);
  }

  @Test
  public void setExpireBatchBenchmark() {
    List<Digest> digests = generateDigests(DIGEST_COUNT);
    seedData(digests);
    JedisCasWorkerMap pipelinedMap = new JedisCasWorkerMap(jedis, CAS_PREFIX, KEY_EXPIRATION_S);

    log.info("=== setExpire() batch benchmark — " + DIGEST_COUNT + " digests ===");

    long seqNs = benchmark("sequential (EXPIRE × N)", () -> sequentialSetExpire(jedis, digests));

    long pipNs = benchmark("pipelined (single pipeline)", () -> pipelinedMap.setExpire(digests));

    logSpeedup("setExpire", seqNs, pipNs);
    flushData(digests);
  }

  /**
   * Combined benchmark showing total time for a realistic workload: add blobs, query locations, and
   * refresh TTLs.
   */
  @Test
  public void combinedWorkloadBenchmark() {
    List<Digest> digests = generateDigests(DIGEST_COUNT);
    JedisCasWorkerMap pipelinedMap = new JedisCasWorkerMap(jedis, CAS_PREFIX, KEY_EXPIRATION_S);

    log.info("=== Combined workload benchmark — " + DIGEST_COUNT + " digests ===");
    log.info("  Workload: addAll → getMap → setExpire");

    long seqNs =
        benchmark(
            "sequential (all operations)",
            () -> {
              sequentialAddAll(jedis, digests, "combined_worker");
              sequentialGetMap(jedis, digests);
              sequentialSetExpire(jedis, digests);
            });

    long pipNs =
        benchmark(
            "pipelined (all operations)",
            () -> {
              pipelinedMap.addAll(digests, "combined_worker");
              pipelinedMap.getMap(digests);
              pipelinedMap.setExpire(digests);
            });

    logSpeedup("combined", seqNs, pipNs);

    // Log the round-trip reduction
    int seqCommands =
        (DIGEST_COUNT * 2) + DIGEST_COUNT + DIGEST_COUNT; // add(2×N) + getMap(N) + expire(N)
    int pipCommands = 3; // 3 pipeline flushes
    log.info(
        String.format(
            "  Round-trips reduced: %d sequential commands → %d pipeline flushes (%.0f%%"
                + " reduction)",
            seqCommands, pipCommands, (1.0 - (double) pipCommands / seqCommands) * 100));

    flushData(digests);
  }
}
