// Copyright 2017 The Bazel Authors. All rights reserved.
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

package build.buildfarm.instance.memory;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.ContentAddressableStorage;
import com.google.devtools.remoteexecution.v1test.Digest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class MemoryLRUContentAddressableStorage implements ContentAddressableStorage {
  private final long maxSizeInBytes;
  private Consumer<Digest> onPut;

  private final Map<Digest, Entry> storage = new HashMap<>();
  private final Map<Digest, Object> mutexes = new HashMap<>();
  private final Entry header = new SentinelEntry();
  private transient long sizeInBytes = 0;

  public MemoryLRUContentAddressableStorage(long maxSizeInBytes) {
    this(maxSizeInBytes, (digest) -> {});
  }

  public MemoryLRUContentAddressableStorage(long maxSizeInBytes, Consumer<Digest> onPut) {
    this.maxSizeInBytes = maxSizeInBytes;
    this.onPut = onPut;
  }

  @Override
  public boolean contains(Digest digest) {
    // incur access use of the digest
    return get(digest) != null;
  }

  @Override
  public synchronized Blob get(Digest digest) {
    if (digest.getSizeBytes() == 0) {
      throw new IllegalArgumentException("Cannot fetch empty blob");
    }

    Entry e = storage.get(digest);
    if (e == null) {
      return null;
    }
    e.recordAccess(header);
    return e.value;
  }

  @Override
  public void put(Blob blob) {
    put(blob, null);
  }

  @Override
  public synchronized void put(Blob blob, Runnable onExpiration) {
    if (blob.getDigest().getSizeBytes() == 0) {
      throw new IllegalArgumentException("Cannot put empty blob");
    }

    Entry e = storage.get(blob.getDigest());
    if (e != null) {
      if (onExpiration != null) {
        e.addOnExpiration(onExpiration);
      }
      e.recordAccess(header);
      return;
    }

    sizeInBytes += blob.size();

    while (sizeInBytes > maxSizeInBytes && header.after != header) {
      expireEntry(header.after);
    }

    createEntry(blob, onExpiration);

    storage.put(blob.getDigest(), header.before);
  }

  private void createEntry(Blob blob, Runnable onExpiration) {
    Entry e = new Entry(blob);
    if (onExpiration != null) {
      e.addOnExpiration(onExpiration);
    }
    e.addBefore(header);
  }

  private void expireEntry(Entry e) {
    System.out.println("MemoryLRUCAS: expiring " + DigestUtil.toString(e.key));
    storage.remove(e.key);
    e.expire();
    sizeInBytes -= e.value.size();
  }

  private static class Entry {
    Entry before, after;
    final Digest key;
    final Blob value;
    private List<Runnable> onExpirations;

    /** implemented only for sentinel */
    private Entry() {
      key = null;
      value = null;
      onExpirations = null;
    }

    public Entry(Blob blob) {
      key = blob.getDigest();
      value = blob;
      onExpirations = null;
    }

    public void addOnExpiration(Runnable onExpiration) {
      if (onExpirations == null) {
        onExpirations = new ArrayList<>(1);
      }
      onExpirations.add(onExpiration);
    }

    public void remove() {
      before.after = after;
      after.before = before;
    }

    public void expire() {
      remove();
      if (onExpirations != null) {
        for (Runnable r : onExpirations) {
          r.run();
        }
      }
    }

    public void addBefore(Entry existingEntry) {
      after = existingEntry;
      before = existingEntry.before;
      before.after = this;
      after.before = this;
    }

    public void recordAccess(Entry header) {
      remove();
      addBefore(header);
    }
  }

  class SentinelEntry extends Entry {
    SentinelEntry() {
      super();
      before = after = this;
    }

    @Override
    public void addOnExpiration(Runnable onExpiration) {
      throw new UnsupportedOperationException("cannot add expiration to sentinal");
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("cannot remove sentinel");
    }

    @Override
    public void expire() {
      throw new UnsupportedOperationException("cannot expire sentinel");
    }

    @Override
    public void addBefore(Entry existingEntry) {
      throw new UnsupportedOperationException("cannot add sentinel");
    }

    @Override
    public void recordAccess(Entry header) {
      throw new UnsupportedOperationException("cannot record sentinel access");
    }
  }
}
