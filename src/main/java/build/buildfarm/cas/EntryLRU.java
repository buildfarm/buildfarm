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

package build.buildfarm.cas;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

import build.buildfarm.cas.CASFileCache.Entry;
import build.buildfarm.cas.CASFileCache.SentinelEntry;
import com.google.common.collect.Maps;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.concurrent.GuardedBy;

public class EntryLRU implements LRU {
  private static final Logger logger = Logger.getLogger(EntryLRU.class.getName());

  private transient AtomicLong sizeInBytes = new AtomicLong(0);
  private final long maxSizeInBytes;

  private final ConcurrentMap<String, Entry> entryStorage = Maps.newConcurrentMap();

  @GuardedBy("this")
  private final transient Entry header = SentinelEntry.getSingletonHeader();

  public EntryLRU(long maxSizeInBytes) {
    this.maxSizeInBytes = maxSizeInBytes;
    header.after = header.before = header;
  }

  @Override
  public synchronized void put(Entry e) {
    checkNotNull(e);
    entryStorage.put(e.key, e);
    if (e.referenceCount == 0) {
      e.addBefore(header);
    }
    sizeInBytes.addAndGet(e.size);
  }

  @Override
  public synchronized Entry get(String key) {
    return entryStorage.get(key);
  }

  @Override
  public Entry remove(String key) {
    Entry e = entryStorage.remove(key);
    if (e != null && e.isLinked()) {
      e.unlink();
    }
    return e;
  }

  @Override
  public int decrementInputReferences(Iterable<String> inputFiles) {
    int entriesDereferenced = 0;
    for (String input : inputFiles) {
      checkNotNull(input);
      Entry e = entryStorage.get(input);
      if (e == null) {
        throw new IllegalStateException(input + " has been removed with references");
      }
      if (!e.key.equals(input)) {
        throw new RuntimeException("ERROR: entry retrieved: " + e.key + " != " + input);
      }
      synchronized (this) {
        e.decrementReference(header);
      }
      if (e.referenceCount == 0) {
        entriesDereferenced++;
      }
    }
    return entriesDereferenced;
  }

  @GuardedBy("this")
  public Entry expireEntry(
      long blobSizeInBytes,
      ExecutorService service,
      Predicate<Entry> delegateInterrupted) // TODO: lambda name should be changed
      throws InterruptedException {
    for (Entry e = waitForLastUnreferencedEntry(); e != null; e = waitForLastUnreferencedEntry()) {
      if (e.referenceCount != 0) {
        throw new IllegalStateException(
            "ERROR: Reference counts lru ordering has not been maintained correctly, attempting to expire referenced (or negatively counted) content "
                + e.key
                + " with "
                + e.referenceCount
                + " references");
      }
      boolean interrupted = delegateInterrupted.test(e);
      Entry removedEntry = remove(e.key); // ?
      // reference compare on purpose
      if (removedEntry == e) {
        discharge(e.size);
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
        return e;
      }
      if (removedEntry == null) {
        logger.log(Level.SEVERE, format("entry %s was already removed during expiration", e.key));
        if (e.isLinked()) {
          logger.log(Level.SEVERE, format("removing spuriously non-existent entry %s", e.key));
          e.unlink();
        } else {
          logger.log(
              Level.SEVERE,
              format(
                  "spuriously non-existent entry %s was somehow unlinked, should not appear again",
                  e.key));
        }
      } else {
        logger.log(
            Level.SEVERE,
            "removed entry %s did not match last unreferenced entry, restoring it",
            e.key);
        put(removedEntry); // ?
      }
      // possibly delegated, but no removal, if we're interrupted, abort loop
      if (interrupted || Thread.currentThread().isInterrupted()) {
        throw new InterruptedException();
      }
    }
    return null;
  }

  @GuardedBy("this")
  private Entry waitForLastUnreferencedEntry() throws InterruptedException {
    while (header.after == header) {
      logger.log(
          Level.INFO,
          format(
              "CASFileCache::expireEntry() unreferenced list is empty, %d bytes, %d keys.",
              getStorageSizeInBytes(), totalEntryCount()));
      wait();
      if (getStorageSizeInBytes() <= maxSizeInBytes) {
        return null;
      }
    }
    return header.after;
  }

  @Override
  public synchronized Iterable<Entry> expire(long size) {
    return null;
  }

  @Override
  public long getStorageSizeInBytes() {
    return sizeInBytes.get();
  }

  @Override
  public boolean containsKey(String key) {
    return entryStorage.containsKey(key);
  }

  @Override
  public long totalEntryCount() {
    return entryStorage.size();
  }

  @Override
  public long unreferencedEntryCount() {
    return Entry.entryCount.get();
  }

  @Override
  public void discharge(long size) {
    sizeInBytes.addAndGet(-size);
  }
}
