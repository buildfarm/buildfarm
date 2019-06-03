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

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.logging.Level.SEVERE;

import build.bazel.remote.execution.v2.BatchReadBlobsResponse.Response;
import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.Write;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.protobuf.StatusProto;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.logging.Logger;
import javax.annotation.concurrent.GuardedBy;

class MemoryCAS implements ContentAddressableStorage {
  private static final Logger logger = Logger.getLogger(MemoryCAS.class.getName());

  static final Status OK = Status.newBuilder()
      .setCode(Code.OK.getNumber())
      .build();

  static final Status NOT_FOUND = Status.newBuilder()
      .setCode(Code.NOT_FOUND.getNumber())
      .build();

  private final Writes writes = new Writes(this);
  private final long maxSizeInBytes;

  @GuardedBy("this")
  private final Map<Digest, Entry> storage;

  @GuardedBy("this")
  private final Entry header;

  @GuardedBy("this")
  private long sizeInBytes;

  public MemoryCAS(long maxSizeInBytes) {
    this.maxSizeInBytes = maxSizeInBytes;
    sizeInBytes = 0;
    header = new SentinelEntry();
    header.before = header.after = header;
    storage = Maps.newHashMap();
  }

  @Override
  public synchronized boolean contains(Digest digest) {
    return get(digest) != null;
  }

  @Override
  public synchronized Iterable<Digest> findMissingBlobs(Iterable<Digest> digests) {
    ImmutableList.Builder<Digest> missing = ImmutableList.builder();
    // incur access use of the digest
    for (Digest digest : digests) {
      if (digest.getSizeBytes() != 0 && !contains(digest)) {
        missing.add(digest);
      }
    }
    return missing.build();
  }

  @Override
  public synchronized InputStream newInput(Digest digest, long offset) throws IOException {
    Entry e = storage.get(digest);
    if (e == null) {
      throw new NoSuchFileException(digest.getHash());
    }
    e.recordAccess(header);
    InputStream in = e.value.getData().newInput();
    in.skip(offset);
    return in;
  }

  @Override
  public ListenableFuture<Iterable<Response>> getAllFuture(Iterable<Digest> digests) {
    return immediateFuture(getAll(digests));
  }

  synchronized Iterable<Response> getAll(Iterable<Digest> digests) {
    return getAll(digests, (digest) -> {
      Blob blob = get(digest);
      if (blob == null) {
        return null;
      }
      return blob.getData();
    });
  }

  public static Iterable<Response> getAll(
      Iterable<Digest> digests,
      Function<Digest, ByteString> blobGetter) {
    ImmutableList.Builder<Response> responses =
        ImmutableList.builder();
    for (Digest digest : digests) {
      responses.add(getResponse(digest, blobGetter));
    }
    return responses.build();
  }

  private static Status statusFromThrowable(Throwable t) {
    Status status = StatusProto.fromThrowable(t);
    if (status == null) {
      status = Status.newBuilder()
          .setCode(io.grpc.Status.fromThrowable(t).getCode().value())
          .build();
    }
    return status;
  }

  public static Response getResponse(Digest digest, Function<Digest, ByteString> blobGetter) {
    Response.Builder response = Response.newBuilder()
        .setDigest(digest);
    try {
      ByteString blob = blobGetter.apply(digest);
      if (blob == null) {
        response.setStatus(NOT_FOUND);
      } else {
        response
            .setData(blob)
            .setStatus(OK);
      }
    } catch (Throwable t) {
      logger.log(SEVERE, "error getting " + DigestUtil.toString(digest), t);
      response.setStatus(statusFromThrowable(t));
    }
    return response.build();
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

  @GuardedBy("this")
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
  public Write getWrite(Digest digest, UUID uuid) {
    return writes.get(digest, uuid);
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

    writes.getFuture(blob.getDigest()).set(blob.getData());
  }

  @GuardedBy("this")
  private void createEntry(Blob blob, Runnable onExpiration) {
    Entry e = new Entry(blob);
    if (onExpiration != null) {
      e.addOnExpiration(onExpiration);
    }
    e.addBefore(header);
  }

  @GuardedBy("this")
  private void expireEntry(Entry e) {
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
