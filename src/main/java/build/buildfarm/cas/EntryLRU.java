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

import build.buildfarm.cas.CASFileCache.Entry;
import build.buildfarm.cas.CASFileCache.SentinelEntry;
import com.google.common.collect.Maps;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public class EntryLRU implements LRU {
  private static final Logger logger = Logger.getLogger(EntryLRU.class.getName());

  private transient AtomicLong sizeInBytes = new AtomicLong(0);

  private final ConcurrentMap<String, Entry> entryStorage = Maps.newConcurrentMap();
  private final transient Entry header = new SentinelEntry();

  public EntryLRU() {
    header.after = header.before = header;
  }

  @Override
  public synchronized void put(Entry e) {
    checkNotNull(e);
    entryStorage.put(e.key, e);
    e.addBefore(header);
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
