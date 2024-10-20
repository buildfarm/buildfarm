// Copyright 2017 The Buildfarm Authors. All rights reserved.
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.concurrent.TimeUnit.MINUTES;

import build.bazel.remote.execution.v2.BatchReadBlobsResponse.Response;
import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.Write;
import build.buildfarm.v1test.Digest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.rpc.Status;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.ServerCallStreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import javax.annotation.concurrent.GuardedBy;
import lombok.extern.java.Log;

@Log
public class MemoryCAS implements ContentAddressableStorage {
  private final long maxSizeInBytes;
  private final Consumer<Digest> onPut;

  @GuardedBy("this")
  private final Map<String, Entry> storage;

  @GuardedBy("this")
  private final Entry header = new SentinelEntry();

  @GuardedBy("this")
  private long sizeInBytes;

  private final ContentAddressableStorage delegate;
  private final Writes writes = new Writes(this);

  public MemoryCAS(long maxSizeInBytes) {
    this(maxSizeInBytes, (digest) -> {}, /* delegate= */ null);
  }

  public MemoryCAS(
      long maxSizeInBytes, Consumer<Digest> onPut, ContentAddressableStorage delegate) {
    this.maxSizeInBytes = maxSizeInBytes;
    this.onPut = onPut;
    this.delegate = delegate;
    sizeInBytes = 0;
    header.before = header.after = header;
    storage = Maps.newHashMap();
  }

  @Override
  public synchronized boolean contains(
      Digest digest, build.bazel.remote.execution.v2.Digest.Builder result) {
    Entry entry = getEntry(digest);
    if (entry != null) {
      result.setHash(entry.key).setSizeBytes(entry.value.size());
      return true;
    }
    return delegate != null && delegate.contains(digest, result);
  }

  @Override
  public Iterable<build.bazel.remote.execution.v2.Digest> findMissingBlobs(
      Iterable<build.bazel.remote.execution.v2.Digest> digests, DigestFunction.Value digestFunction)
      throws InterruptedException {
    ImmutableList.Builder<build.bazel.remote.execution.v2.Digest> builder = ImmutableList.builder();
    synchronized (this) {
      // incur access use of the digest
      for (build.bazel.remote.execution.v2.Digest digest : digests) {
        if (digest.getSizeBytes() != 0
            && !contains(
                DigestUtil.fromDigest(digest, digestFunction),
                build.bazel.remote.execution.v2.Digest.newBuilder())) {
          builder.add(digest);
        }
      }
    }
    ImmutableList<build.bazel.remote.execution.v2.Digest> missing = builder.build();
    if (delegate != null && !missing.isEmpty()) {
      return delegate.findMissingBlobs(missing, digestFunction);
    }
    return missing;
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Override
  public synchronized InputStream newInput(Compressor.Value compressor, Digest digest, long offset)
      throws IOException {
    checkArgument(compressor == Compressor.Value.IDENTITY);
    // implicit int bounds compare against size bytes
    if (offset < 0 || offset > digest.getSize()) {
      throw new IndexOutOfBoundsException(
          String.format("%d is out of bounds for blob %s", offset, DigestUtil.toString(digest)));
    }
    Blob blob = get(digest);
    if (blob == null) {
      if (delegate != null) {
        // FIXME change this to a read-through input stream
        return delegate.newInput(Compressor.Value.IDENTITY, digest, offset);
      }
      throw new NoSuchFileException(DigestUtil.toString(digest));
    }
    InputStream in = blob.getData().newInput();
    in.skip(offset);
    return in;
  }

  @Override
  public void get(
      Compressor.Value compressor,
      Digest digest,
      long offset,
      long limit,
      ServerCallStreamObserver<ByteString> blobObserver,
      RequestMetadata requestMetadata) {
    checkArgument(compressor == Compressor.Value.IDENTITY);
    Blob blob = get(digest);
    if (blob == null) {
      if (delegate != null) {
        // FIXME change this to a read-through get
        delegate.get(compressor, digest, offset, limit, blobObserver, requestMetadata);
      } else {
        blobObserver.onError(io.grpc.Status.NOT_FOUND.asException());
      }
    } else {
      blobObserver.onNext(blob.getData());
      blobObserver.onCompleted();
    }
  }

  @Override
  public ListenableFuture<List<Response>> getAllFuture(
      Iterable<build.bazel.remote.execution.v2.Digest> digests,
      DigestFunction.Value digestFunction) {
    return immediateFuture(getAll(digests, digestFunction));
  }

  synchronized List<Response> getAll(
      Iterable<build.bazel.remote.execution.v2.Digest> digests,
      DigestFunction.Value digestFunction) {
    return getAll(
        digests,
        digestFunction,
        (digest) -> {
          Blob blob = get(DigestUtil.fromDigest(digest, digestFunction));
          if (blob == null) {
            return null;
          }
          return blob.getData();
        });
  }

  public static List<Response> getAll(
      Iterable<build.bazel.remote.execution.v2.Digest> digests,
      DigestFunction.Value digestFunction,
      Function<build.bazel.remote.execution.v2.Digest, ByteString> blobGetter) {
    ImmutableList.Builder<Response> responses = ImmutableList.builder();
    for (build.bazel.remote.execution.v2.Digest digest : digests) {
      responses.add(getResponse(digest, digestFunction, blobGetter));
    }
    return responses.build();
  }

  private static Status statusFromThrowable(Throwable t) {
    Status status = StatusProto.fromThrowable(t);
    if (status == null) {
      status =
          Status.newBuilder().setCode(io.grpc.Status.fromThrowable(t).getCode().value()).build();
    }
    return status;
  }

  public static Response getResponse(
      build.bazel.remote.execution.v2.Digest digest,
      DigestFunction.Value digestFunction,
      Function<build.bazel.remote.execution.v2.Digest, ByteString> blobGetter) {
    Response.Builder response = Response.newBuilder().setDigest(digest);
    try {
      ByteString blob = blobGetter.apply(digest);
      if (blob == null) {
        response.setStatus(NOT_FOUND);
      } else {
        response.setData(blob).setStatus(OK);
      }
    } catch (Throwable t) {
      log.log(
          Level.SEVERE,
          "error getting " + DigestUtil.toString(DigestUtil.fromDigest(digest, digestFunction)),
          t);
      response.setStatus(statusFromThrowable(t));
    }
    return response.build();
  }

  @Override
  public Blob get(Digest digest) {
    if (digest.getSize() == 0) {
      throw new IllegalArgumentException("Cannot fetch empty blob");
    }

    Entry e = getEntry(digest);
    if (e == null) {
      if (delegate != null) {
        return delegate.get(digest);
      }
      return null;
    }
    return e.value;
  }

  private synchronized Entry getEntry(Digest digest) {
    Entry e = storage.get(DigestUtil.toString(digest));
    if (e == null) {
      return null;
    }
    e.recordAccess(header);
    return e;
  }

  @GuardedBy("this")
  @SuppressWarnings("PMD.CompareObjectsWithEquals")
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
  public Write getWrite(
      Compressor.Value compressor, Digest digest, UUID uuid, RequestMetadata requestMetadata) {
    return writes.get(compressor, digest, uuid);
  }

  @Override
  public void put(Blob blob) {
    put(blob, null);
    onPut.accept(blob.getDigest());
  }

  @Override
  public void put(Blob blob, Runnable onExpiration) {
    if (blob.getDigest().getSize() == 0) {
      throw new IllegalArgumentException("Cannot put empty blob");
    }

    if (add(blob, onExpiration)) {
      writes.getFuture(blob.getDigest()).set(blob.getData());
      onPut.accept(blob.getDigest());
    }
  }

  @SuppressWarnings("PMD.CompareObjectsWithEquals")
  private synchronized boolean add(Blob blob, Runnable onExpiration) {
    Entry e = storage.get(blob.getDigest().getHash());
    if (e != null) {
      if (onExpiration != null) {
        e.addOnExpiration(onExpiration);
      }
      e.recordAccess(header);
      return false;
    }

    sizeInBytes += blob.size();

    while (sizeInBytes > maxSizeInBytes && header.after != header) {
      expireEntry(header.after);
    }

    if (sizeInBytes > maxSizeInBytes) {
      log.log(
          Level.WARNING,
          String.format(
              "Out of nodes to remove, sizeInBytes = %d, maxSizeInBytes = %d, storage = %d, list ="
                  + " %d",
              sizeInBytes, maxSizeInBytes, storage.size(), size()));
    }

    createEntry(blob, onExpiration);

    storage.put(blob.getDigest().getHash(), header.before);

    return true;
  }

  @Override
  public long maxEntrySize() {
    return UNLIMITED_ENTRY_SIZE_MAX;
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
    Digest digest =
        DigestUtil.buildDigest(e.key, e.value.size(), e.value.getDigest().getDigestFunction());
    log.log(Level.INFO, "MemoryLRUCAS: expiring " + DigestUtil.toString(digest));
    if (delegate != null) {
      try {
        Write write =
            delegate.getWrite(
                Compressor.Value.IDENTITY,
                digest,
                UUID.randomUUID(),
                RequestMetadata.getDefaultInstance());
        try (OutputStream out = write.getOutput(1, MINUTES, () -> {})) {
          e.value.getData().writeTo(out);
        }
      } catch (IOException ioEx) {
        log.log(
            Level.SEVERE, String.format("error delegating %s", DigestUtil.toString(digest)), ioEx);
      }
    }
    storage.remove(e.key);
    e.expire();
    sizeInBytes -= digest.getSize();
  }

  private static class Entry {
    Entry before;
    Entry after;
    final String key;
    final Blob value;
    private List<Runnable> onExpirations;

    /** implemented only for sentinel */
    private Entry() {
      key = null;
      value = null;
      onExpirations = null;
    }

    public Entry(Blob blob) {
      key = blob.getDigest().getHash();
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
