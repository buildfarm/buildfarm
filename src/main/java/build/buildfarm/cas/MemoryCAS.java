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

package build.buildfarm.cas;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.DigestUtil;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class MemoryCAS implements ContentAddressableStorage {
  private static final Logger logger = Logger.getLogger(MemoryCAS.class.getName());

  private final long maxSizeInBytes;
  private Consumer<Digest> onPut;

  private final Map<Digest, Entry> storage = new HashMap<>();
  private final Map<Digest, Object> mutexes = new HashMap<>();
  private final Entry header = new SentinelEntry();
  private transient long sizeInBytes = 0;

  public MemoryCAS(long maxSizeInBytes) {
    this(maxSizeInBytes, (digest) -> {});
  }

  public MemoryCAS(long maxSizeInBytes, Consumer<Digest> onPut) {
    this.maxSizeInBytes = maxSizeInBytes;
    this.onPut = onPut;
  }

  @Override
  public synchronized Iterable<Digest> findMissingBlobs(Iterable<Digest> digests) {
    ImmutableList.Builder<Digest> missing = ImmutableList.builder();
    // incur access use of the digest
    for (Digest digest : digests) {
      if (digest.getSizeBytes() != 0 && get(digest) == null) {
        missing.add(digest);
      }
    }
    return missing.build();
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
  public InputStream newInput(Digest digest, long offset) throws IOException {
    // implicit int bounds compare against size bytes
    if (offset < 0 || offset > digest.getSizeBytes()) {
      throw new IndexOutOfBoundsException(
          String.format(
              "%d is out of bounds for blob %s",
              offset,
              DigestUtil.toString(digest)));
    }
    Blob blob = get(digest);
    if (blob == null) {
      throw new NoSuchFileException(DigestUtil.toString(digest));
    }
    return blob.getData().substring((int) offset).newInput();
  }

  @Override
  public OutputStream newOutput(Digest digest) throws IOException {
    if (digest.getSizeBytes() == 0 || digest.getSizeBytes() > Integer.MAX_VALUE) {
      return null;
    }

    return new ByteArrayOutputStream((int) digest.getSizeBytes()) {
      boolean hasPut = false;

      @Override
      public void close() throws IOException {
        if (count != digest.getSizeBytes()) {
          throw new IOException(
              String.format(
                  "content size was %d, expected %d",
                  count,
                  digest.getSizeBytes()));
        }
        if (!hasPut) {
          put(new Blob(ByteString.copyFrom(buf, 0, count), digest));
          hasPut = true;
        }
      }
    };
  }

  private long size() {
    Entry e = header.before;
    long count = 0;
    while (e != header) {
      count++;
      e = e.before;
    }
    return count;
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

    if (sizeInBytes > maxSizeInBytes) {
      logger.warning(
          String.format(
              "Out of nodes to remove, sizeInBytes = %d, maxSizeInBytes = %d, storage = %d, list = %d",
              sizeInBytes,
              maxSizeInBytes,
              storage.size(),
              size()));
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
    logger.info("MemoryLRUCAS: expiring " + DigestUtil.toString(e.key));
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
