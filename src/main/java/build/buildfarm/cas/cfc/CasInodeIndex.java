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

package build.buildfarm.cas.cfc;

import build.buildfarm.cas.cfc.CASFileCache.Entry;
import io.prometheus.client.Counter;
import java.util.concurrent.ConcurrentHashMap;
import org.jspecify.annotations.Nullable;

/**
 * Owns the inode-to-{@link Entry} map and the bookkeeping that keeps {@code
 * Entry.casDirectoryHardlinkCount} synchronized with that map. {@code DirectoryEntryCFC} hardlinks
 * CAS files into CAS-internal directory trees; the directory tree walker that runs at parent-dir
 * eviction does not have the source CAS key — only the destination file's inode — so it must look
 * up the source {@code Entry} by inode identity ({@code BasicFileAttributes.fileKey()}) to
 * decrement.
 *
 * <p>The two methods below are the only sanctioned way to mutate {@code
 * Entry.casDirectoryHardlinkCount}. They pair the atomic counter update with the conditional map
 * insert/remove using a re-read-inside-{@code compute} pattern so concurrent 0&harr;1 transitions
 * resolve deterministically. Callers must never touch the underlying counter or map directly —
 * reasoning about correctness lives here, not at every call site.
 *
 * <p>The map's lifecycle belongs here rather than on {@code Entry}: {@code Entry} is a CAS data
 * record, unaware of the indexing strategy.
 */
final class CasInodeIndex {
  // Accounting-bug canary. decrement() already fails fast on underflow (throws below), but that is
  // only visible in logs/crashes; this counter makes it dashboard-visible. Nonzero ⇒ a double-
  // decrement / decrement-without-increment / double-visit ⇒ either a leak or premature eviction of
  // a live hardlinked file — exactly the failure mode Phase 3's accounting exists to prevent.
  private static final Counter hardlinkCountUnderflowTotal =
      Counter.build()
          .name("cas_hardlink_count_underflow_total")
          .help("casDirectoryHardlinkCount decrement underflows (accounting-bug canary; expect 0).")
          .register();

  // Keyed by Object because BasicFileAttributes.fileKey() returns Object — the JDK does not provide
  // a shared interface across UnixFileKey (Linux/macOS), WindowsFileKey, and other providers'
  // implementations. Each concrete class implements equals() and hashCode() on the underlying inode
  // identity, so Object works correctly as the map key; we just can't declare a more specific type.
  private final ConcurrentHashMap<Object, Entry> map = new ConcurrentHashMap<>();

  /**
   * Atomically increments {@code entry.casDirectoryHardlinkCount} and, on the 0&rarr;1 transition,
   * inserts ({@code fileKey} &rarr; {@code entry}) into the index. The compute callback re-reads
   * the counter so a concurrent decrement that already drove the count back to 0 does not leave a
   * stale mapping behind.
   *
   * <p>The insert overwrites any existing mapping for {@code fileKey}, which is intentional and
   * safe under inode reuse: the OS can hand {@code fileKey}'s inode to a different file only after
   * the prior occupant's last hardlink is gone — i.e. after its count already reached 0 (removing
   * its mapping) and it was evicted (freeing the inode). So at most one {@code Entry} ever holds
   * {@code count > 0} for a given {@code fileKey} at a time, and the overwrite only ever replaces a
   * logically-dead mapping.
   */
  void increment(Entry entry, Object fileKey) {
    int old = entry.getAndAddCasDirectoryHardlinkCount(1);
    if (old == 0) {
      map.compute(
          fileKey, (k, existing) -> entry.casDirectoryHardlinkCount() > 0 ? entry : existing);
    }
  }

  /**
   * Atomically decrements {@code entry.casDirectoryHardlinkCount} and, on the 1&rarr;0 transition,
   * removes the ({@code fileKey} &rarr; {@code entry}) mapping from the index.
   *
   * <p>Throws {@link IllegalStateException} on underflow (caller bug — double-decrement, decrement
   * without a prior increment, or a walker visiting a file twice), matching the codebase convention
   * of failing fast on refcount-discipline violations. Recovery (e.g. floor-at-zero) would silently
   * leak index entries and produce permanently-unevictable Entries — a worse failure mode than a
   * fail-fast crash.
   *
   * @return the count this decrement produced (the value the atomic transitioned to, ignoring
   *     concurrent mutations). A return of {@code 0} means this call performed the 1&rarr;0
   *     transition and owns waking the source's evictor. Deciding the wake off this returned value
   *     rather than a separate re-read of the counter closes a last-decrementer race: if every
   *     decrementer that drives the count to 0 instead observed a transient {@code > 0} from an
   *     interleaved increment, no one would issue the wake and the now-evictable source would sit
   *     skipped until the idle heartbeat.
   */
  int decrement(Entry entry, Object fileKey) {
    int old = entry.getAndAddCasDirectoryHardlinkCount(-1);
    if (old <= 0) {
      entry.getAndAddCasDirectoryHardlinkCount(1);
      hardlinkCountUnderflowTotal.inc();
      throw new IllegalStateException(
          "entry " + entry.key + " casDirectoryHardlinkCount underflow (was " + old + ")");
    }
    if (old == 1) {
      map.compute(
          fileKey,
          (k, existing) ->
              existing == entry && entry.casDirectoryHardlinkCount() <= 0 ? null : existing);
    }
    return old - 1;
  }

  @Nullable Entry get(Object fileKey) {
    return map.get(fileKey);
  }

  int size() {
    return map.size();
  }
}
