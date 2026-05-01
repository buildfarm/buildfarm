// Copyright 2026 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.actioncache;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.OutputFile;
import build.buildfarm.backplane.Backplane;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.v1test.Digest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class ShardActionCacheTest {
  private static final ActionKey KEY_A = key("a");
  private static final ActionResult RESULT_A = result("a");

  @Mock private Backplane backplane;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void get_cacheMiss_loadsFromBackplaneAndCachesResult() throws Exception {
    when(backplane.getActionResult(KEY_A)).thenReturn(RESULT_A);
    ShardActionCache cache = newCache(/* maxEntries= */ 10L);

    // First get reaches the backplane and caches; second get for the same key uses the cache.
    assertThat(cache.get(KEY_A).get()).isEqualTo(RESULT_A);
    assertThat(cache.get(KEY_A).get()).isEqualTo(RESULT_A);

    verify(backplane, times(1)).getActionResult(KEY_A);
  }

  @Test
  public void invalidate_removesEntry_forcesReloadFromBackplane() throws Exception {
    when(backplane.getActionResult(KEY_A)).thenReturn(RESULT_A);
    ShardActionCache cache = newCache(/* maxEntries= */ 10L);

    // First get reaches backplane, then invalidate, then get again reaches backplane again.
    cache.get(KEY_A).get();
    cache.invalidate(KEY_A);
    cache.get(KEY_A).get();

    verify(backplane, times(2)).getActionResult(KEY_A);
  }

  @Test
  public void readThrough_writesLocalCacheOnly_neverCallsBackplane() throws Exception {
    ShardActionCache cache = newCache(/* maxEntries= */ 10L);

    // Puts the key in the cache and doesn't contact the backplane.
    cache.readThrough(KEY_A, RESULT_A);
    assertThat(cache.get(KEY_A).get()).isEqualTo(RESULT_A);

    verify(backplane, never()).putActionResult(any(), any());
    verify(backplane, never()).getActionResult(any());
  }

  @Test
  public void readThrough_capExceeded_evictsOldEntriesAndReloadsFromBackplane() throws Exception {
    when(backplane.getActionResult(KEY_A)).thenReturn(RESULT_A);
    ShardActionCache cache = newCache(/* maxEntries= */ 10L);

    cache.readThrough(KEY_A, RESULT_A);
    for (int i = 0; i < 100; i++) {
      cache.readThrough(key("filler-" + i), result("filler-" + i));
    }

    cache.get(KEY_A).get();

    // Make sure the key is fetched from the backplane after being evicted due to cache size.
    verify(backplane, times(1)).getActionResult(KEY_A);
  }

  private ShardActionCache newCache(long maxEntries) {
    return new ShardActionCache(maxEntries, backplane, newDirectExecutorService());
  }

  private static ActionKey key(String hash) {
    return DigestUtil.asActionKey(Digest.newBuilder().setHash(hash).setSize(1).build());
  }

  private static ActionResult result(String tag) {
    return ActionResult.newBuilder()
        .addOutputFiles(OutputFile.newBuilder().setPath(tag).build())
        .build();
  }
}
