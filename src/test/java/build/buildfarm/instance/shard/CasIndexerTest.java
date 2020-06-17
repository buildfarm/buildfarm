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

package build.buildfarm.common.redis;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import build.buildfarm.instance.shard.CasIndexer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

@RunWith(JUnit4.class)
public class CasIndexerTest {

  @Mock private JedisCluster redis;
  @Mock private ScanResult scanResult;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void reindexWithoutException() throws Exception {

    // test that we can call reindexing with a mocked redis
    // and not see any exceptions.
    CasIndexer.reindexWorkers(redis);
  }

  @Test
  public void exhaustScanResults() throws Exception {

    // test that indexing terminates when all results are scanned
    when(redis.scan(any(String.class), any(ScanParams.class))).thenReturn(scanResult);
    when(scanResult.getCursor()).thenReturn("1").thenReturn("1").thenReturn("0");
    CasIndexer.reindexWorkers(redis);
  }

  @Test
  public void storeActiveWorkers() throws Exception {

    Set<String> workerSet = new HashSet<>(Arrays.asList("worker1", "worker2", "worker3"));
    when(redis.scan(any(String.class), any(ScanParams.class))).thenReturn(scanResult);
    when(scanResult.getCursor()).thenReturn("0");
    when(redis.hkeys("Workers")).thenReturn(workerSet);
    CasIndexer.reindexWorkers(redis);

    // ensure workers are added to an intersection set
    verify(redis, times(1))
        .sadd("{aSM}:intersecting-workers", workerSet.stream().toArray(String[]::new));
    verify(redis, times(1))
        .sadd("{aSN}:intersecting-workers", workerSet.stream().toArray(String[]::new));
    verify(redis, times(1))
        .sadd("{aSO}:intersecting-workers", workerSet.stream().toArray(String[]::new));
  }

  @Test
  public void removedThroughInterStore() throws Exception {

    when(redis.scan(any(String.class), any(ScanParams.class))).thenReturn(scanResult);
    when(scanResult.getCursor()).thenReturn("0");
    when(scanResult.getResult()).thenReturn(Arrays.asList("key_1", "key_2", "key_3"));
    CasIndexer.reindexWorkers(redis);

    // ensure intersections are made on the keys
    verify(redis, times(1)).sinterstore("key_1", "{aSM}:intersecting-workers", "key_1");
    verify(redis, times(1)).sinterstore("key_2", "{aSN}:intersecting-workers", "key_2");
    verify(redis, times(1)).sinterstore("key_3", "{aSO}:intersecting-workers", "key_3");
  }
}
