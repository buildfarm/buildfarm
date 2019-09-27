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

package build.buildfarm.worker;

import static build.buildfarm.worker.Utils.readdir;
import static build.buildfarm.worker.Utils.removeDirectory;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.io.ByteStreams.nullOutputStream;
import static com.google.common.util.concurrent.Futures.catchingAsync;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.successfulAsList;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.Futures.whenAllComplete;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.lang.String.format;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.logging.Level.WARNING;
import static java.util.logging.Level.SEVERE;

import build.bazel.remote.execution.v2.BatchReadBlobsResponse.Response;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.DigestMismatchException;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.Write;
import build.buildfarm.common.Write.CompleteWrite;
import build.buildfarm.common.io.FeedbackOutputStream;
import build.buildfarm.v1test.BlobWriteKey;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.HashingOutputStream;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.protobuf.ByteString;
import io.grpc.Deadline;
import io.grpc.StatusRuntimeException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

public abstract class CASFileCache implements ContentAddressableStorage {
  private static final Logger logger = Logger.getLogger(CASFileCache.class.getName());

  private final Path root;
  private final long maxSizeInBytes;
  private final long maxEntrySizeInBytes;
  private final DigestUtil digestUtil;
  private final ConcurrentMap<Path, Entry> storage;
  private final Map<Digest, DirectoryEntry> directoryStorage = Maps.newHashMap();
  private final LockMap locks = new LockMap();
  private final Consumer<Digest> onPut;
  private final Consumer<Iterable<Digest>> onExpire;
  private final Executor accessRecorder;
  private final ExecutorService expireService;
  @Nullable private final ContentAddressableStorage delegate;
  private final LoadingCache<BlobWriteKey, Write> writes = CacheBuilder.newBuilder()
      .expireAfterAccess(1, HOURS)
      .removalListener(new RemovalListener<BlobWriteKey, Write>() {
        @Override
        public void onRemoval(RemovalNotification<BlobWriteKey, Write> notification) {
          notification.getValue().reset();
        }
      })
      .build(new CacheLoader<BlobWriteKey, Write>() {
        @Override
        public Write load(BlobWriteKey key) {
          return newWrite(key);
        }
      });
  private final LoadingCache<Digest, SettableFuture<Long>> writesInProgress = CacheBuilder.newBuilder()
      .expireAfterAccess(1, HOURS)
      .removalListener(new RemovalListener<Digest, SettableFuture<Long>>() {
        @Override
        public void onRemoval(RemovalNotification<Digest, SettableFuture<Long>> notification) {
          // no effect if already done
          notification.getValue().setException(new IOException("write cancelled"));
        }
      })
      .build(new CacheLoader<Digest, SettableFuture<Long>>() {
        @Override
        public SettableFuture<Long> load(Digest digest) {
          SettableFuture<Long> future = SettableFuture.create();
          if (containsLocal(digest, (key) -> {})) {
            future.set(digest.getSizeBytes());
          }
          return future;
        }
      });

  private ListenableFuture<Path> lastPutDirectory = immediateFuture(null);

  private transient long sizeInBytes = 0;
  private transient Entry header = new SentinelEntry();

  synchronized long size() {
    return sizeInBytes;
  }

  public static class IncompleteBlobException extends IOException {
    private final Path writePath;
    private final Path key;
    private final long committed;
    private final long expected;

    IncompleteBlobException(Path writePath, Path key, long committed, long expected) {
      super(
          format(
              "blob %s => %s: committed %d, expected %d",
              writePath, key, committed, expected));
      this.writePath = writePath;
      this.key = key;
      this.committed = committed;
      this.expected = expected;
    }
  }

  public CASFileCache(
      Path root,
      long maxSizeInBytes,
      long maxEntrySizeInBytes,
      DigestUtil digestUtil,
      ExecutorService expireService,
      Executor accessRecorder) {
    this(
        root,
        maxSizeInBytes,
        maxEntrySizeInBytes,
        digestUtil,
        expireService,
        accessRecorder,
        /* storage=*/ Maps.newConcurrentMap(),
        /* onPut=*/ (digest) -> {},
        /* onExpire=*/ (digests) -> {},
        /* delegate=*/ null);
  }

  public CASFileCache(
      Path root,
      long maxSizeInBytes,
      long maxEntrySizeInBytes,
      DigestUtil digestUtil,
      ExecutorService expireService,
      Executor accessRecorder,
      ConcurrentMap<Path, Entry> storage,
      Consumer<Digest> onPut,
      Consumer<Iterable<Digest>> onExpire,
      @Nullable ContentAddressableStorage delegate) {
    this.root = root;
    this.maxSizeInBytes = maxSizeInBytes;
    this.maxEntrySizeInBytes = maxEntrySizeInBytes;
    this.digestUtil = digestUtil;
    this.expireService = expireService;
    this.accessRecorder = accessRecorder;
    this.storage = storage;
    this.onPut = onPut;
    this.onExpire = onExpire;
    this.delegate = delegate;

    header.before = header.after = header;
  }

  public static <T> T getInterruptiblyOrIOException(ListenableFuture<T> future) throws IOException, InterruptedException {
    try {
      return future.get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      if (e.getCause() instanceof InterruptedException) {
        throw (InterruptedException) e.getCause();
      }
      throw new UncheckedExecutionException(e.getCause());
    }
  }

  public static <T> T getOrIOException(ListenableFuture<T> future) throws IOException {
    boolean interrupted = false;
    for (;;) {
      try {
        T t = future.get();
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
        return t;
      } catch (ExecutionException e) {
        if (e.getCause() instanceof IOException) {
          throw (IOException) e.getCause();
        }
        if (e.getCause() instanceof InterruptedException) {
          Thread.interrupted();
          interrupted = true;
        }
        throw new UncheckedExecutionException(e.getCause());
      } catch (InterruptedException e) {
        Thread.interrupted();
        interrupted = true;
      }
    }
  }

  private static Digest keyToDigest(Path key, DigestUtil digestUtil) throws NumberFormatException {
    String fileName = key.getFileName().toString();
    String[] components = fileName.split("_");

    String hashComponent = components[0];
    String sizeComponent = components[1];
    long parsedSizeComponent = Long.parseLong(sizeComponent);

    return digestUtil.build(hashComponent, parsedSizeComponent);
  }

  /**
   * Parses the given fileName and invokes the onKey method if successful
   *
   * if size > 0, consider the filename invalid if it does not match
   */
  private static <T> T parseFileEntryKey(String fileName, long size, DigestUtil digestUtil, BiFunction<Digest, Boolean, T> onKey) {
    String[] components = fileName.split("_");
    if (components.length < 2 || components.length > 3) {
      return null;
    }

    boolean isExecutable = false;
    Digest digest;
    try {
      String sizeComponent = components[1];
      long parsedSizeComponent = Long.parseLong(sizeComponent);

      if (size > 0 && parsedSizeComponent != size) {
        return null;
      }

      String hashComponent = components[0];
      digest = digestUtil.build(hashComponent, parsedSizeComponent);
      if (components.length == 3) {
        if (components[2].equals("exec")) {
          isExecutable = true;
        } else {
          return null;
        }
      }
    } catch (NumberFormatException e) {
      return null;
    }

    return onKey.apply(digest, isExecutable);
  }

  private FileEntryKey parseFileEntryKey(String fileName) {
    return parseFileEntryKey(fileName, /* size=*/ -1);
  }

  private FileEntryKey parseFileEntryKey(String fileName, long size) {
    return parseFileEntryKey(
        fileName,
        size,
        digestUtil,
        (digest, isExecutable) -> new FileEntryKey(getKey(digest, isExecutable), isExecutable, digest));
  }

  private boolean contains(Digest digest, boolean isExecutable, Consumer<Path> onContains) {
    Path key = getKey(digest, isExecutable);
    if (!storage.containsKey(key)) {
      return false;
    }
    onContains.accept(key);
    return true;
  }

  private void accessed(Iterable<Path> keys) {
    /* could also bucket these */
    try {
      accessRecorder.execute(() -> recordAccess(keys));
    } catch (RejectedExecutionException e) {
      logger.log(SEVERE, format("could not record access for %d keys", Iterables.size(keys)), e);
    }
  }

  private synchronized void recordAccess(Iterable<Path> keys) {
    for (Path key : keys) {
      Entry e = storage.get(key);
      if (e != null) {
        e.recordAccess(header);
      }
    }
  }

  private boolean entryExists(Entry e) {
    if (!e.existsDeadline.isExpired()) {
      return true;
    }

    if (Files.exists(e.key)) {
      e.existsDeadline = Deadline.after(10, SECONDS);
      return true;
    }
    return false;
  }

  boolean containsLocal(Digest digest, Consumer<Path> onContains) {
    /* maybe swap the order here if we're higher in ratio on one side */
    return contains(digest, false, onContains) || contains(digest, true, onContains);
  }

  @Override
  public Iterable<Digest> findMissingBlobs(Iterable<Digest> digests) throws InterruptedException {
    ImmutableList.Builder<Digest> builder = ImmutableList.builder();
    ImmutableList.Builder<Path> found = ImmutableList.builder();
    for (Digest digest : digests) {
      if (!containsLocal(digest, found::add)) {
        builder.add(digest);
      }
    }
    List<Path> foundDigests = found.build();
    if (!foundDigests.isEmpty()) {
      accessed(foundDigests);
    }
    ImmutableList<Digest> missingDigests = builder.build();
    if (delegate != null && !missingDigests.isEmpty()) {
      return delegate.findMissingBlobs(missingDigests);
    }
    return missingDigests;
  }

  @Override
  public boolean contains(Digest digest) {
    return containsLocal(digest, (key) -> accessed(ImmutableList.of(key))) || (delegate != null && delegate.contains(digest));
  }

  @Override
  public ListenableFuture<Iterable<Response>> getAllFuture(Iterable<Digest> digests) {
    throw new UnsupportedOperationException();
  }

  protected InputStream newTransparentInput(Digest digest, long offset) throws IOException {
    try {
      return newLocalInput(digest, offset);
    } catch (NoSuchFileException e) {
      if (delegate == null) {
        throw e;
      }
    }
    return delegate.newInput(digest, offset);
  }

  InputStream newLocalInput(Digest digest, long offset) throws IOException {
    logger.finer(format("getting input stream for %s", DigestUtil.toString(digest)));
    boolean isExecutable = false;
    do {
      Path key = getKey(digest, isExecutable);
      Entry e = storage.get(key);
      if (e != null) {
        InputStream input = null;
        try {
          input = Files.newInputStream(key);
          input.skip(offset);
        } catch (NoSuchFileException eNoEnt) {
          boolean removed = false;
          synchronized (this) {
            Entry removedEntry = storage.remove(key);
            if (removedEntry == e) {
              unlinkEntry(removedEntry);
              removed = true;
            } else if (removedEntry != null) {
              logger.severe(format("nonexistent entry %s did not match last unreferenced entry, restoring it", key));
              storage.put(key, removedEntry);
            }
          }
          if (removed && isExecutable) {
            onExpire.accept(ImmutableList.of(digest));
          }
          continue;
        }
        accessed(ImmutableList.of(key));
        return input;
      }
      isExecutable = !isExecutable;
    } while (isExecutable != false);
    throw new NoSuchFileException(DigestUtil.toString(digest));
  }

  @Override
  public InputStream newInput(Digest digest, long offset) throws IOException {
    try {
      return newLocalInput(digest, offset);
    } catch (NoSuchFileException e) {
      if (delegate == null) {
        throw e;
      }
    }
    if (digest.getSizeBytes() > maxEntrySizeInBytes) {
      return delegate.newInput(digest, offset);
    }
    Write write = getWrite(digest, UUID.randomUUID(), RequestMetadata.getDefaultInstance());
    return newReadThroughInput(
        digest,
        offset,
        write);
  }

  ReadThroughInputStream newReadThroughInput(Digest digest, long offset, Write write) throws IOException {
    return new ReadThroughInputStream(delegate.newInput(digest, 0), digest, offset, write);
  }

  @Override
  public Blob get(Digest digest) {
    throw new UnsupportedOperationException();
  }

  boolean completeWrite(Digest digest) {
    try {
      onPut.accept(digest);
    } catch (RuntimeException e) {
      logger.log(SEVERE, "error during write completion onPut for " + DigestUtil.toString(digest), e);
      /* ignore error, writes must complete */
    }
    try {
      return writesInProgress.get(digest).set(digest.getSizeBytes());
    } catch (ExecutionException e) {
      logger.log(SEVERE, "error getting write in progress future for " + DigestUtil.toString(digest), e);
      return false;
    }
  }

  void invalidateWrite(Digest digest) {
    writesInProgress.invalidate(digest);
  }

  @Override
  public void put(Blob blob) throws InterruptedException {
    Path blobPath = getKey(blob.getDigest(), false);
    try {
      logger.finer(format("put: %s", blobPath.getFileName()));
      OutputStream out = putImpl(
          blobPath,
          UUID.randomUUID(),
          () -> completeWrite(blob.getDigest()),
          blob.getDigest().getSizeBytes(),
          /* isExecutable=*/ false,
          /* containingDirectory=*/ null,
          () -> invalidateWrite(blob.getDigest()));
      boolean referenced = out == null;
      try {
        if (out != null) {
          try {
            blob.getData().writeTo(out);
          } finally {
            out.close();
            referenced = true;
          }
        }
      } finally {
        if (referenced) {
          decrementReference(blobPath);
        }
      }
    } catch (IOException e) {
      logger.log(SEVERE, "error putting " + DigestUtil.toString(blob.getDigest()), e);
    }
  }

  @Override
  public Write getWrite(Digest digest, UUID uuid, RequestMetadata requestMetadata) {
    if (digest.getSizeBytes() == 0) {
      return new CompleteWrite(0);
    }
    try {
      return writes.get(BlobWriteKey.newBuilder()
          .setDigest(digest)
          .setIdentifier(uuid.toString())
          .build());
    } catch (ExecutionException e) {
      logger.log(SEVERE, "error getting write for " + DigestUtil.toString(digest) + ":" + uuid, e);
      throw new IllegalStateException("write create must not fail", e.getCause());
    }
  }

  class ReadThroughInputStream extends InputStream {
    private InputStream in;
    private final Write write;
    private final OutputStream out;
    private final Digest digest;

    @GuardedBy("this")
    private boolean local = false;

    @GuardedBy("this")
    private long localOffset;

    @GuardedBy("this")
    private long skip;

    @GuardedBy("this")
    private long remaining;

    @GuardedBy("this")
    private IOException exception = null;

    ReadThroughInputStream(InputStream in, Digest digest, long offset, Write write) throws IOException {
      this.in = in;
      this.localOffset = offset;
      this.digest = digest;
      skip = offset;
      remaining = digest.getSizeBytes();
      this.write = write;
      write.addListener(
          this::switchToLocal,
          directExecutor());
      out = write.getOutput(1, MINUTES, () -> {});
    }

    private synchronized void switchToLocal() {
      if (!local && localOffset < digest.getSizeBytes()) {
        local = true;
        try {
          in.close();
        } catch (IOException e) {
          // ignore
        }
        try {
          in = newTransparentInput(digest, localOffset);
        } catch (IOException e) {
          in = null;
          exception = e;
        }
        notify(); // wake up a writer
      }
    }

    @GuardedBy("this")
    private void readToSkip() throws IOException {
      while (!local && skip > 0) {
        byte[] buf = new byte[8192];

        int len = (int) Math.min(buf.length, skip);
        int n = in.read(buf, 0, len);
        if (n > 0) {
          out.write(buf, 0, n);
          skip -= n;
          remaining -= n;
          localOffset += n;
        } else if (n < 0) {
          throw new IOException("premature EOF for delegate");
        }
      }
    }

    @Override
    public int available() throws IOException {
      return in.available();
    }

    @Override
    public synchronized int read() throws IOException {
      if (local) {
        if (exception != null) {
          throw exception;
        }
        return in.read();
      }
      int b;
      try {
        readToSkip();
        b = in.read();
        if (b != -1) {
          try {
            out.write(b);
          } catch (IOException e) {
            if (!write.isComplete()) {
              throw e;
            }
            // complete writes will switch to local
          }
          remaining--;
          localOffset++;
        } else if (remaining != 0) {
          throw new IOException("premature EOF for delegate");
        }
      } catch (ClosedChannelException e) {
        // if either in or out are closed, it should be due to a local switch
        while (!local) {
          try {
            wait();
          } catch (InterruptedException intEx) {
            throw new IOException(intEx);
          }
        }
        // we reacquire, meaning we should have completed the local switch
        return in.read();
      }
      if (remaining == 0) {
        out.close();
      }
      return b;
    }

    @Override
    public int read(byte[] buf) throws IOException {
      return read(buf, 0, buf.length);
    }

    @Override
    public synchronized int read(byte[] buf, int ofs, int len) throws IOException {
      if (local) {
        if (exception != null) {
          throw exception;
        }
        return in.read(buf, ofs, len);
      }
      int n;
      try {
        readToSkip();
        n = in.read(buf, ofs, len);
        if (n > 0) {
          out.write(buf, ofs, n);
          remaining -= n;
          localOffset += n;
        } else if (remaining != 0) {
          throw new IOException("premature EOF for delegate");
        }
      } catch (ClosedChannelException e) {
        // if either in or out are closed, it should be due to a local switch
        while (!local) {
          try {
            wait();
          } catch (InterruptedException intEx) {
            throw new IOException(intEx);
          }
        }
        // we reacquire, meaning we should have completed the local switch
        return in.read(buf, ofs, len);
      }
      if (remaining == 0) {
        out.close();
      }
      return n;
    }

    @Override
    public synchronized long skip(long n) throws IOException {
      if (local) {
        if (exception != null) {
          throw exception;
        }
        return in.skip(n);
      }
      if (n <= 0) {
        return 0;
      }
      if (skip + n > remaining) {
        n = remaining - skip;
      }
      skip += n;
      localOffset += n;
      return n;
    }

    @Override
    public synchronized void close() throws IOException {
      if (exception != null) {
        throw exception;
      }
      if (!local) {
        if (remaining != 0) {
          write.reset();
        } else {
          try {
            out.close();
          } catch (IOException e) {
            // ignore, may be incomplete
          }
        }
      }
      in.close();
    }
  }

  static class WriteOutputStream extends FeedbackOutputStream {
    protected final OutputStream out;
    private final WriteOutputStream writeOut;

    WriteOutputStream(OutputStream out) {
      this.out = out;
      this.writeOut = null;
    }

    WriteOutputStream(WriteOutputStream writeOut) {
      this.out = writeOut;
      this.writeOut = writeOut;
    }

    @Override
    public void write(int b) throws IOException {
      out.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
      out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
    }

    @Override
    public void close() throws IOException {
      out.close();
    }

    @Override
    public boolean isReady() {
      if (writeOut != null) {
        return writeOut.isReady();
      }
      return true; // fs blocking guarantees readiness
    }

    public Path getPath() {
      if (writeOut == null) {
        throw new UnsupportedOperationException();
      }
      return writeOut.getPath();
    }

    public long getWritten() {
      if (writeOut == null) {
        throw new UnsupportedOperationException();
      }
      return writeOut.getWritten();
    }
  }

  Write newWrite(BlobWriteKey key) {
    Write write = new Write() {
      CancellableOutputStream out = null;
      Path path = null;

      @Override
      public synchronized void reset() {
        try {
          if (out != null) {
            out.cancel();
          } else if (path != null && Files.exists(path)) {
            Files.delete(path);
            path = null;
          }
        } catch (IOException e) {
          logger.log(SEVERE, "could not reset write " + DigestUtil.toString(key.getDigest()) + ":" + key.getIdentifier(), e);
        } finally {
          onClosed();
        }
      }

      @Override
      public synchronized long getCommittedSize() {
        if (isComplete()) {
          return key.getDigest().getSizeBytes();
        }
        if (out == null) {
          Path blobKey = getKey(key.getDigest(), false);
          try {
            return Files.size(blobKey.resolveSibling(blobKey.getFileName() + "." + key.getIdentifier()));
          } catch (IOException e) {
            return 0;
          }
        }
        return out.getWritten();
      }

      @Override
      public synchronized boolean isComplete() {
        return getFuture().isDone() || (out == null && containsLocal(key.getDigest(), (key) -> {}));
      }

      public ListenableFuture<Long> getFuture() {
        try {
          return writesInProgress.get(key.getDigest());
        } catch (ExecutionException e) {
          throw new UncheckedExecutionException(e.getCause());
        }
      }

      public void onClosed() {
        out = null;
      }

      @Override
      public synchronized FeedbackOutputStream getOutput(long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) throws IOException {
        if (out == null) {
          out = newOutput(key.getDigest(), UUID.fromString(key.getIdentifier()), this::onClosed);
          if (out == null) {
            out = new CancellableOutputStream(nullOutputStream());
          } else {
            path = out.getPath();
          }
        }
        return out;
      }

      @Override
      public void addListener(Runnable onCompleted, Executor executor) {
        getFuture().addListener(onCompleted, executor);
      }
    };
    write.addListener(write::reset, directExecutor());
    return write;
  }

  CancellableOutputStream newOutput(Digest digest, UUID uuid, Runnable onClosed) throws IOException {
    Path blobPath = getKey(digest, false);
    final CancellableOutputStream cancellableOut;
    try {
      logger.finer(format("getWrite: %s", blobPath.getFileName()));
      cancellableOut = putImpl(
          blobPath,
          uuid,
          () -> completeWrite(digest),
          digest.getSizeBytes(),
          /* isExecutable=*/ false,
          null,
          () -> invalidateWrite(digest));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
    if (cancellableOut == null) {
      decrementReference(blobPath);
      return null;
    }
    return new CancellableOutputStream(cancellableOut) {
      AtomicBoolean closed = new AtomicBoolean(false);

      @Override
      public void cancel() throws IOException {
        try {
          if (closed.compareAndSet(/* expected=*/ false, /* update=*/ true)) {
            cancellableOut.cancel();
          }
        } finally {
          onClosed.run();
        }
      }

      @Override
      public void close() throws IOException {
        try {
          if (closed.compareAndSet(/* expected=*/ false, /* update=*/ true)) {
            try {
              out.close();
              decrementReference(blobPath);
            } catch (IncompleteBlobException e) {
              // ignore
            }
          }
        } finally {
          onClosed.run();
        }
      }
    };
  }

  @Override
  public void put(Blob blob, Runnable onExpiration) {
    throw new UnsupportedOperationException();
  }

  private static final class SharedLock implements Lock {
    private final AtomicBoolean locked = new AtomicBoolean(false);

    @Override
    public void lock() {
      for (;;) {
        try {
          lockInterruptibly();
          return;
        } catch (InterruptedException e) {
          // ignore
        }
      }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
      // attempt to atomically synchronize
      synchronized (locked) {
        while (!locked.compareAndSet(/* expected=*/ false, /* update=*/ true)) {
          locked.wait();
        }
      }
    }

    @Override
    public Condition newCondition() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryLock() {
      synchronized (locked) {
        return locked.compareAndSet(false, true);
      }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void unlock() {
      if (!locked.compareAndSet(/* expected=*/ true, /* update=*/ false)) {
        throw new IllegalMonitorStateException("the lock was not held");
      }
      synchronized (locked) {
        locked.notify();
      }
    }
  }

  private static final class LockMap {
    private final Map<Path, Lock> mutexes = Maps.newHashMap();

    private synchronized Lock acquire(Path key) {
      Lock mutex = mutexes.get(key);
      if (mutex == null) {
        mutex = new SharedLock();
        mutexes.put(key, mutex);
      }
      return mutex;
    }

    private synchronized void release(Path key) {
      // prevents this lock from being exclusive to other accesses, since it
      // must now be present
      mutexes.remove(key);
    }
  }

  private static final class FileEntryKey {
    private final Path key;
    private final boolean isExecutable;
    private final Digest digest;

    FileEntryKey(Path key, boolean isExecutable, Digest digest) {
      this.key = key;
      this.isExecutable = isExecutable;
      this.digest = digest;
    }

    Path getKey() {
      return key;
    }

    boolean getIsExecutable() {
      return isExecutable;
    }

    Digest getDigest() {
      return digest;
    }
  }

  private Directory computeDirectory(
      Path path,
      Map<Object, Entry> fileKeys,
      ImmutableList.Builder<Path> inputsBuilder) {
    Directory.Builder b = Directory.newBuilder();

    List<Dirent> sortedDirent;
    try {
      sortedDirent = readdir(path, /* followSymlinks= */ false);
    } catch (IOException e) {
      logger.log(SEVERE, "error reading directory " + path.toString(), e);
      return null;
    }
    sortedDirent.sort(Comparator.comparing(Dirent::getName));

    for (Dirent dirent : sortedDirent) {
      String name = dirent.getName();
      Path child = path.resolve(name);
      if (dirent.getType() == Dirent.Type.DIRECTORY) {
        Directory dir = computeDirectory(child, fileKeys, inputsBuilder);
        if (dir == null) {
          return null;
        }
        b.addDirectoriesBuilder().setName(name).setDigest(digestUtil.compute(dir));
      } else if (dirent.getType() == Dirent.Type.FILE) {
        Entry e = fileKeys.get(dirent.getStat().fileKey());
        if (e == null) {
          return null;
        }
        checkNotNull(e.key);
        inputsBuilder.add(e.key);
        Digest digest = CASFileCache.keyToDigest(e.key, digestUtil);
        b.addFilesBuilder().setName(name).setDigest(digest).setIsExecutable(Files.isExecutable(child));
      } else {
        return null;
      }
    }

    return b.build();
  }

  public void start() throws IOException, InterruptedException {
    start(newDirectExecutorService());
  }

  public void start(ExecutorService removeDirectoryService) throws IOException, InterruptedException {
    start(onPut, removeDirectoryService);
  }

  /**
   * initialize the cache for persistent storage and inject any
   * consistent entries which already exist under the root into
   * the storage map. This call will create the root if it does
   * not exist, and will scale in cost with the number of files
   * already present.
   */
  public void start(Consumer<Digest> onPut, ExecutorService removeDirectoryService) throws IOException, InterruptedException {
    Files.createDirectories(root);

    // brutal hax
    if (delegate != null && delegate instanceof CASFileCache) {
      CASFileCache fileCacheDelegate = (CASFileCache) delegate;
      fileCacheDelegate.start(onPut, removeDirectoryService);
    }

    ImmutableList.Builder<Path> directories = new ImmutableList.Builder<>();
    ImmutableMap.Builder<Object, Entry> fileKeysBuilder = new ImmutableMap.Builder<>();
    Files.walkFileTree(root, ImmutableSet.<FileVisitOption>of(), 1, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        if (attrs.isDirectory()) {
          directories.add(file);
          return FileVisitResult.CONTINUE;
        }

        long size = attrs.size();
        if (sizeInBytes + size > maxSizeInBytes || size > maxEntrySizeInBytes) {
          Files.delete(file);
        } else {
          FileEntryKey fileEntryKey = null;
          if (file.getParent().equals(root)) {
            String fileName = file.getFileName().toString();
            fileEntryKey = parseFileEntryKey(fileName, size);
          }
          if (fileEntryKey != null) {
            Path key = fileEntryKey.getKey();
            if (storage.get(key) == null) {
              long now = System.nanoTime();
              Entry e = new Entry(key, size, null, Deadline.after(10, SECONDS));
              fileKeysBuilder.put(attrs.fileKey(), e);
              storage.put(e.key, e);
              onPut.accept(fileEntryKey.getDigest());
              synchronized (CASFileCache.this) {
                e.decrementReference(header);
              }
              sizeInBytes += size;
            }
          } else {
            Files.delete(file);
          }
        }
        return FileVisitResult.CONTINUE;
      }
    });

    ExecutorService pool = Executors.newFixedThreadPool(
        /* nThreads=*/ 32,
        new ThreadFactoryBuilder().setNameFormat("scan-cache-pool-%d").build());

    ImmutableList.Builder<Path> invalidDirectories = new ImmutableList.Builder<>();

    Map<Object, Entry> fileKeys = fileKeysBuilder.build();
    for (Path path : directories.build()) {
      pool.execute(() -> {
        ImmutableList.Builder<Path> inputsBuilder = new ImmutableList.Builder<>();
        Directory directory = computeDirectory(path, fileKeys, inputsBuilder);
        Digest digest = directory == null ? null : digestUtil.compute(directory);
        if (digest != null && getDirectoryPath(digest).equals(path)) {
          DirectoryEntry e = new DirectoryEntry(
              directory,
              inputsBuilder.build(),
              Deadline.after(10, SECONDS));
          synchronized (this) {
            directoryStorage.put(digest, e);
            for (Path input : e.inputs) {
              Entry entry = storage.get(input);
              entry.containingDirectories.add(digest);
            }
          }
        } else {
          synchronized (invalidDirectories) {
            invalidDirectories.add(path);
          }
        }
      });
    }
    pool.shutdown();
    // FIXME exhaustion timeout?
    while (!pool.isTerminated()) {
      logger.info("Waiting for directory population to complete");
      pool.awaitTermination(1, MINUTES);
    }

    for (Path path : invalidDirectories.build()) {
      removeDirectory(path, removeDirectoryService);
    }
  }

  private static String digestFilename(Digest digest) {
    return format("%s_%d", digest.getHash(), digest.getSizeBytes());
  }

  public static String getFileName(Digest digest, boolean isExecutable) {
    return format(
        "%s%s",
        digestFilename(digest),
        (isExecutable ? "_exec" : ""));
  }

  public Path getKey(Digest digest, boolean isExecutable) {
    return getPath(getFileName(digest, isExecutable));
  }

  private void decrementReference(Path inputFile) {
    decrementReferences(ImmutableList.of(inputFile), ImmutableList.of());
  }

  public synchronized void decrementReferences(Iterable<Path> inputFiles, Iterable<Digest> inputDirectories) {
    decrementReferencesSynchronized(inputFiles, inputDirectories);
  }

  private int decrementInputReferences(Iterable<Path> inputFiles) {
    int entriesDereferenced = 0;
    for (Path input : inputFiles) {
      checkNotNull(input);
      Entry e = storage.get(input);
      if (e == null) {
        throw new IllegalStateException(input + " has been removed with references");
      }
      if (!e.key.equals(input)) {
        throw new RuntimeException("ERROR: entry retrieved: " + e.key + " != " + input);
      }
      e.decrementReference(header);
      if (e.referenceCount == 0) {
        entriesDereferenced++;
      }
    }
    return entriesDereferenced;
  }

  private void decrementReferencesSynchronized(Iterable<Path> inputFiles, Iterable<Digest> inputDirectories) {
    // decrement references and notify if any dropped to 0
    // insert after the last 0-reference count entry in list
    int entriesDereferenced = decrementInputReferences(inputFiles);
    for (Digest inputDirectory : inputDirectories) {
      DirectoryEntry dirEntry = directoryStorage.get(inputDirectory);
      if (dirEntry == null) {
        throw new IllegalStateException("inputDirectory " + DigestUtil.toString(inputDirectory) + " is not in directoryStorage");
      }
      entriesDereferenced += decrementInputReferences(dirEntry.inputs);
    }
    if (entriesDereferenced > 0) {
      notify();
    }
  }

  public Path getRoot() {
    return root;
  }

  public Path getPath(String filename) {
    return root.resolve(filename);
  }

  private synchronized void dischargeAndNotify(long size) {
    discharge(size);
    notify();
  }

  @GuardedBy("this")
  private void discharge(long size) {
    sizeInBytes -= size;
  }

  @GuardedBy("this")
  private void unlinkEntry(Entry entry) throws IOException {
    try {
      dischargeEntry(entry, expireService);
    } catch (Exception e) {
      throw new IOException(e);
    }
    // technically we should attempt to remove the file here,
    // but we're only called in contexts where it doesn't exist...
  }

  @VisibleForTesting
  public Path getDirectoryPath(Digest digest) {
    return root.resolve(digestFilename(digest) + "_dir");
  }

  @GuardedBy("this")
  private Entry waitForLastUnreferencedEntry(long blobSizeInBytes) throws InterruptedException {
    while (header.after == header) {
      int references = 0;
      int keys = 0;
      int min = -1, max = 0;
      Path minkey = null, maxkey = null;
      logger.info(format("CASFileCache::expireEntry(%d) header(%s): { after: %s, before: %s }", blobSizeInBytes, header.hashCode(), header.after.hashCode(), header.before.hashCode()));
      // this should be incorporated in the listenable future construction...
      for (Map.Entry<Path, Entry> pe : storage.entrySet()) {
        Path key = pe.getKey();
        Entry e = pe.getValue();
        if (e.referenceCount > max) {
          max = e.referenceCount;
          maxkey = key;
        }
        if (min == -1 || e.referenceCount < min) {
          min = e.referenceCount;
          minkey = key;
        }
        if (e.referenceCount == 0) {
          logger.info(format("CASFileCache::expireEntry(%d) unreferenced entry(%s): { after: %s, before: %s }",
                blobSizeInBytes, e.hashCode(), e.after == null ? null : e.after.hashCode(), e.before == null ? null : e.before.hashCode()));
        }
        references += e.referenceCount;
        keys++;
      }
      if (keys == 0) {
        throw new IllegalStateException("CASFileCache::expireEntry(" + blobSizeInBytes + ") there are no keys to wait for expiration on");
      }
      logger.info(format(
          "CASFileCache::expireEntry(%d) unreferenced list is empty, %d bytes, %d keys with %d references, min(%d, %s), max(%d, %s)",
          blobSizeInBytes, sizeInBytes, keys, references, min, minkey, max, maxkey));
      wait();
      if (sizeInBytes <= maxSizeInBytes) {
        return null;
      }
    }
    return header.after;
  }

  @GuardedBy("this")
  List<ListenableFuture<Void>> unlinkAndExpireDirectories(Entry entry, ExecutorService service) {
    ImmutableList.Builder<ListenableFuture<Void>> builder = ImmutableList.builder();
    for (Digest containingDirectory : entry.containingDirectories) {
      builder.add(expireDirectory(containingDirectory, service));
    }
    entry.unlink();
    if (entry.referenceCount != 0) {
      logger.severe("removed referenced entry " + entry.key);
    }
    return builder.build();
  }

  @GuardedBy("this")
  private ListenableFuture<Path> dischargeEntryFuture(Entry entry, ExecutorService service) {
    List<ListenableFuture<Void>> directoryExpirationFutures = unlinkAndExpireDirectories(entry, service);
    discharge(entry.size);
    return whenAllComplete(directoryExpirationFutures).call(
        () -> {
          Exception expirationException = null;
          for (ListenableFuture<Void> directoryExpirationFuture : directoryExpirationFutures) {
            try {
              directoryExpirationFuture.get();
            } catch (ExecutionException e) {
              Throwable cause = e.getCause();
              if (cause instanceof Exception) {
                expirationException = (Exception) cause;
              } else {
                logger.log(SEVERE, "undeferrable exception during discharge of " + entry.key, cause);
                // errors and the like, avoid any deferrals
                Throwables.throwIfUnchecked(cause);
                throw new RuntimeException(cause);
              }
            } catch (InterruptedException e) {
              // unlikely, all futures must be complete
            }
          }
          if (expirationException != null) {
            throw expirationException;
          }
          return entry.key;
        },
        service);
  }

  @GuardedBy("this")
  private void dischargeEntry(Entry entry, ExecutorService service) throws Exception {
    Exception expirationException = null;
    for (ListenableFuture<Void> directoryExpirationFuture : unlinkAndExpireDirectories(entry, service)) {
      do {
        try {
          directoryExpirationFuture.get();
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          if (cause instanceof Exception) {
            expirationException = (Exception) cause;
          } else {
            logger.log(SEVERE, "undeferrable exception during discharge of " + entry.key, cause);
            // errors and the like, avoid any deferrals
            Throwables.throwIfUnchecked(cause);
            throw new RuntimeException(cause);
          }
        } catch (InterruptedException e) {
          // FIXME add some suppression
          expirationException = e;
        }
      } while (!directoryExpirationFuture.isDone());
    }
    // only discharge after all the directories are gone, or their removal failed
    discharge(entry.size);
    if (expirationException != null) {
      throw expirationException;
    }
  }

  @GuardedBy("this")
  private ListenableFuture<Path> expireEntry(
      long blobSizeInBytes,
      ExecutorService service) throws IOException, InterruptedException {
    for (Entry e = waitForLastUnreferencedEntry(blobSizeInBytes);
        e != null; e = waitForLastUnreferencedEntry(blobSizeInBytes)) {
      if (e.referenceCount != 0) {
        throw new IllegalStateException("ERROR: Reference counts lru ordering has not been maintained correctly, attempting to expire referenced (or negatively counted) content " + e.key.getFileName() + " with " + e.referenceCount + " references");
      }
      Path key = e.key;
      boolean interrupted = false;
      if (delegate != null) {
        FileEntryKey fileEntryKey = parseFileEntryKey(key.getFileName().toString());
        if (fileEntryKey == null) {
          logger.severe(format("error parsing expired key %s", key));
        } else {
          Write write = delegate.getWrite(fileEntryKey.getDigest(), UUID.randomUUID(), RequestMetadata.getDefaultInstance());
          try (OutputStream out = write.getOutput(1, MINUTES, () -> {}); InputStream in = Files.newInputStream(key)) {
            ByteStreams.copy(in, out);
          } catch (IOException ioEx) {
            interrupted = Thread.interrupted() || ioEx.getCause() instanceof InterruptedException || ioEx instanceof ClosedByInterruptException;
            write.reset();
            logger.log(SEVERE, format("error delegating expired entry %s", key), ioEx);
          }
        }
      }
      Entry removedEntry = storage.remove(key);
      // reference compare on purpose
      if (removedEntry == e) {
        ListenableFuture<Path> pathFuture = dischargeEntryFuture(e, service);
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
        return pathFuture;
      }
      if (removedEntry == null) {
        logger.severe(format("entry %s was already removed during expiration", key));
      } else {
        logger.severe(format("removed entry %s did not match last unreferenced entry, restoring it", key));
        storage.put(key, removedEntry);
      }
      // possibly delegated, but no removal, if we're interrupted, abort loop
      if (interrupted || Thread.currentThread().isInterrupted()) {
        throw new InterruptedException();
      }
    }
    return null;
  }

  @GuardedBy("this")
  private void purgeDirectoryFromInputs(Digest digest, Iterable<Path> inputs) {
    for (Path input : inputs) {
      Entry fileEntry = storage.get(input);

      if (fileEntry != null) {
        fileEntry.containingDirectories.remove(digest);
      }
    }
  }

  @GuardedBy("this")
  private ListenableFuture<Void> expireDirectory(Digest digest, ExecutorService service) {
    DirectoryEntry e = directoryStorage.remove(digest);
    if (e == null) {
      logger.severe(format("CASFileCache::expireDirectory(%s) does not exist", DigestUtil.toString(digest)));
      return immediateFuture(null);
    }

    purgeDirectoryFromInputs(digest, e.inputs);
    return removeDirectory(getDirectoryPath(digest), service);
  }

  public Iterable<ListenableFuture<Path>> putFiles(
      Iterable<FileNode> files,
      Path path,
      ImmutableList.Builder<Path> inputsBuilder,
      ExecutorService service) throws IOException, InterruptedException {
    ImmutableList.Builder<ListenableFuture<Path>> putFutures = ImmutableList.builder();
    putDirectoryFiles(files, path, /* containingDirectory=*/ null, inputsBuilder, putFutures, service);
    return putFutures.build();
  }

  private void putDirectoryFiles(
      Iterable<FileNode> files,
      Path path,
      Digest containingDirectory,
      ImmutableList.Builder<Path> inputsBuilder,
      ImmutableList.Builder<ListenableFuture<Path>> putFutures,
      ExecutorService service) throws IOException, InterruptedException {
    for (FileNode fileNode : files) {
      Path filePath = path.resolve(fileNode.getName());
      final ListenableFuture<Path> putFuture;
      if (fileNode.getDigest().getSizeBytes() != 0) {
        Path key = getKey(fileNode.getDigest(), fileNode.getIsExecutable());
        putFuture = transformAsync(
            put(fileNode.getDigest(), fileNode.getIsExecutable(), containingDirectory, service),
            (fileCacheKey) -> {
              // FIXME this can die with 'too many links'... needs some cascading fallout
              Files.createLink(filePath, fileCacheKey);
              // we saw null entries in the built immutable list without synchronization
              synchronized (inputsBuilder) {
                inputsBuilder.add(fileCacheKey);
              }
              return immediateFuture(fileCacheKey);
            },
            service);
      } else {
        putFuture = listeningDecorator(service).submit(() -> {
          Files.createFile(filePath);
          // ignore executable
          return filePath;
        });
      }
      putFutures.add(putFuture);
    }
  }

  private void fetchDirectory(
      Digest containingDirectory,
      Path path,
      Digest digest,
      Map<Digest, Directory> directoriesIndex,
      ImmutableList.Builder<Path> inputsBuilder,
      ImmutableList.Builder<ListenableFuture<Path>> putFutures,
      ExecutorService service) throws IOException, InterruptedException {
    if (Files.exists(path)) {
      if (Files.isDirectory(path)) {
        removeDirectory(path);
      } else {
        Files.delete(path);
      }
    }
    Directory directory;
    if (digest.getSizeBytes() == 0) {
      directory = Directory.getDefaultInstance();
    } else {
      directory = directoriesIndex.get(digest);
    }
    if (directory == null) {
      throw new IOException(
          format(
              "directory not found for %s(%s)",
              path,
              DigestUtil.toString(digest)));
    }
    Files.createDirectory(path);
    putDirectoryFiles(directory.getFilesList(), path, containingDirectory, inputsBuilder, putFutures, service);
    for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
      fetchDirectory(
          containingDirectory,
          path.resolve(directoryNode.getName()),
          directoryNode.getDigest(),
          directoriesIndex,
          inputsBuilder,
          putFutures,
          service);
    }
  }

  public ListenableFuture<Path> putDirectory(
      Digest digest,
      Map<Digest, Directory> directoriesIndex,
      ExecutorService service) {
    Path path = getDirectoryPath(digest);
    Lock l = locks.acquire(path);
    logger.finest(format("locking directory %s", path.getFileName()));
    try {
      l.lockInterruptibly();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return immediateFailedFuture(e);
    }
    logger.finest(format("locked directory %s", path.getFileName()));
    ListenableFuture<Path> putFuture;
    try {
      putFuture = putDirectorySynchronized(path, digest, directoriesIndex, service);
    } catch (IOException|InterruptedException e) {
      putFuture = immediateFailedFuture(e);
    }
    putFuture.addListener(
        () -> {
          l.unlock();
          logger.finest(format("directory %s has been unlocked", path.getFileName()));
        },
        service);
    return putFuture;
  }

  private boolean directoryExists(Path path, Directory directory, Map<Digest, Directory> directoriesIndex) {
    if (!Files.exists(path)) {
      logger.severe(format("directory path %s does not exist", path));
      return false;
    }
    for (FileNode fileNode : directory.getFilesList()) {
      Path filePath = path.resolve(fileNode.getName());
      if (!Files.exists(filePath)) {
        logger.severe(format("directory file entry %s does not exist", filePath));
        return false;
      }
      // additional stat check to ensure that the cache entry exists for hard link inode match?
    }
    for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
      if (!directoryExists(
            path.resolve(directoryNode.getName()),
            directoriesIndex.get(directoryNode.getDigest()),
            directoriesIndex)) {
        return false;
      }
    }
    return true;
  }

  private boolean directoryEntryExists(Path path, DirectoryEntry dirEntry, Map<Digest, Directory> directoriesIndex) {
    if (!dirEntry.existsDeadline.isExpired()) {
      return true;
    }

    if (directoryExists(path, dirEntry.directory, directoriesIndex)) {
      dirEntry.existsDeadline = Deadline.after(10, SECONDS);
      return true;
    }
    return false;
  }

  class PutDirectoryException extends IOException {
    private final Path path;
    private final Digest digest;
    private final List<Throwable> exceptions;

    PutDirectoryException(Path path, Digest digest, List<Throwable> exceptions) {
      super(String.format("%s: %d exceptions", path, exceptions.size()));
      this.path = path;
      this.digest = digest;
      this.exceptions = exceptions;
      for (Throwable exception : exceptions) {
        addSuppressed(exception);
      }
    }

    Path getPath() {
      return path;
    }

    Digest getDigest() {
      return digest;
    }

    List<Throwable> getExceptions() {
      return exceptions;
    }
  }

  private ListenableFuture<Path> putDirectorySynchronized(
      Path path,
      Digest digest,
      Map<Digest, Directory> directoriesIndex,
      ExecutorService service) throws IOException, InterruptedException {
    logger.finest(format("directory %s has been locked", path.getFileName()));
    ListenableFuture<Void> expireFuture;
    synchronized (this) {
      DirectoryEntry e = directoryStorage.get(digest);
      if (e == null) {
        expireFuture = immediateFuture(null);
      } else {
        ImmutableList.Builder<Path> inputsBuilder = new ImmutableList.Builder<>();
        for (Path input : e.inputs) {
          Entry fileEntry = storage.get(input);
          if (fileEntry == null) {
            logger.severe(
                format(
                    "CASFileCache::putDirectory(%s) exists, but input %s does not, purging it with fire and resorting to fetch",
                    DigestUtil.toString(digest),
                    input));
            e = null;
            break;
          }
          fileEntry.incrementReference();
          checkNotNull(input);
          inputsBuilder.add(input);
        }

        if (e != null) {
          logger.finest(format("found existing entry for %s", path.getFileName()));
          if (directoryEntryExists(path, e, directoriesIndex)) {
            return immediateFuture(path);
          }
          logger.severe(format("directory %s does not exist in cache, purging it with fire and resorting to fetch", path.getFileName()));
        }

        decrementReferencesSynchronized(inputsBuilder.build(), ImmutableList.<Digest>of());
        expireFuture = expireDirectory(digest, service);
        logger.finest(format("expiring existing entry for %s", path.getFileName()));
      }
    }

    ImmutableList.Builder<Path> inputsBuilder = ImmutableList.builder();
    ListenableFuture<Void> fetchFuture = transformAsync(
        expireFuture,
        (result) -> {
            logger.finest(format("expiry complete, fetching %s", path.getFileName()));
            ImmutableList.Builder<ListenableFuture<Path>> putFuturesBuilder = ImmutableList.builder();
            fetchDirectory(digest, path, digest, directoriesIndex, inputsBuilder, putFuturesBuilder, service);
            ImmutableList<ListenableFuture<Path>> putFutures = putFuturesBuilder.build();

            // is this better suited for whenAllComplete?

            return transformAsync(
                successfulAsList(putFutures),
                (paths) -> {
                  ImmutableList.Builder<Throwable> failures = ImmutableList.builder();
                  boolean failed = false;
                  for (int i = 0; i < paths.size(); i++) {
                    Path putPath = paths.get(i);
                    if (putPath == null) {
                      failed = true;
                      try {
                        putFutures.get(i).get();
                        // should never get here
                      } catch (Throwable t) {
                        failures.add(t);
                      }
                    }
                  }
                  if (failed) {
                    return immediateFailedFuture(new PutDirectoryException(path, digest, failures.build()));
                  }
                  return immediateFuture(null);
                },
                service);
        },
        service);
    ListenableFuture<Void> rollbackFuture = catchingAsync(
        fetchFuture,
        Throwable.class,
        (e) -> {
          ImmutableList<Path> inputs = inputsBuilder.build();
          synchronized (this) {
            purgeDirectoryFromInputs(digest, inputs);
            decrementReferencesSynchronized(inputs, ImmutableList.<Digest>of());
          }
          try {
            removeDirectory(path);
          } catch (IOException removeException) {
            logger.log(SEVERE, "error during directory removal after fetch failure", removeException);
          }
          return immediateFailedFuture(e);
        },
        service);

    return transform(
        rollbackFuture,
        (results) -> {
          logger.finest(format("directory fetch complete, inserting %s", path.getFileName()));
          DirectoryEntry e = new DirectoryEntry(
              // might want to have this treatment ahead of this
              digest.getSizeBytes() == 0 ? Directory.getDefaultInstance() : directoriesIndex.get(digest),
              inputsBuilder.build(),
              Deadline.after(10, SECONDS));
          synchronized (this) {
            directoryStorage.put(digest, e);
          }
          return path;
        },
        service);
  }

  @VisibleForTesting
  public Path put(Digest digest, boolean isExecutable) throws IOException, InterruptedException {
    checkState(digest.getSizeBytes() > 0, "file entries may not be empty");

    return putAndCopy(digest, isExecutable, /* containingDirectory=*/ null);
  }

  public ListenableFuture<Path> put(
      Digest digest,
      boolean isExecutable,
      Digest containingDirectory,
      Executor executor) {
    checkState(digest.getSizeBytes() > 0, "file entries may not be empty");

    return transformAsync(
        immediateFuture(null),
        (result) -> {
          return immediateFuture(putAndCopy(digest, isExecutable, containingDirectory));
        },
        executor);
  }

  Path putAndCopy(Digest digest, boolean isExecutable, Digest containingDirectory) throws IOException, InterruptedException {
    Path key = getKey(digest, isExecutable);
    CancellableOutputStream out = putImpl(
        key,
        UUID.randomUUID(),
        () -> completeWrite(digest),
        digest.getSizeBytes(),
        isExecutable,
        containingDirectory,
        () -> invalidateWrite(digest));
    if (out != null) {
      boolean complete = false;
      try {
        copyExternalInput(digest, out);
        complete = true;
      } finally {
        try {
          logger.finest(format("closing output stream for %s", DigestUtil.toString(digest)));
          if (complete) {
            out.close();
          } else {
            out.cancel();
          }
          logger.finest(format("output stream closed for %s", DigestUtil.toString(digest)));
        } catch (IOException e) {
          if (Thread.interrupted()) {
            logger.log(SEVERE, format("could not close stream for %s", DigestUtil.toString(digest)), e);
            Throwables.propagateIfInstanceOf(e.getCause(), InterruptedException.class);
            throw new InterruptedException();
          } else {
            logger.log(WARNING, format("failed output stream close for %s", DigestUtil.toString(digest)), e);
          }
          throw e;
        }
      }
    }
    return key;
  }

  private void copyExternalInput(Digest digest, CancellableOutputStream out) throws IOException, InterruptedException {
    logger.finest(format("downloading %s", DigestUtil.toString(digest)));
    boolean complete = false;
    try (InputStream in = newExternalInput(digest, /* offset=*/ 0)) {
      ByteStreams.copy(in, out);
      complete = true;
    } catch (IOException e) {
      out.cancel();
      logger.log(WARNING, format("error downloading %s", DigestUtil.toString(digest)), e); // prevent burial by early end of stream during close
      throw e;
    }
    logger.finest(format("download of %s complete", DigestUtil.toString(digest)));
  }

  @FunctionalInterface
  private static interface IORunnable {
    void run() throws IOException;
  }

  private static class CancellableOutputStream extends WriteOutputStream {
    CancellableOutputStream(OutputStream out) {
      super(out);
    }

    CancellableOutputStream(WriteOutputStream out) {
      super(out);
    }

    void cancel() throws IOException {
    }
  }

  private static final CancellableOutputStream DUPLICATE_OUTPUT_STREAM = new CancellableOutputStream(nullOutputStream()) {
    @Override
    public void write(int b) {
    }
  };

  private CancellableOutputStream putImpl(
      Path key,
      UUID writeId,
      Supplier<Boolean> writeWinner,
      long blobSizeInBytes,
      boolean isExecutable,
      Digest containingDirectory,
      Runnable onInsert) throws IOException, InterruptedException {
    CancellableOutputStream out = putOrReference(
        key,
        writeId,
        writeWinner,
        blobSizeInBytes,
        isExecutable,
        containingDirectory,
        onInsert);
    if (out == DUPLICATE_OUTPUT_STREAM) {
      return null;
    }
    logger.finest(format("entry %s is missing, downloading and populating", key.getFileName()));
    return newCancellableOutputStream(out);
  }

  private CancellableOutputStream newCancellableOutputStream(
      CancellableOutputStream cancellableOut) {
    return new CancellableOutputStream(cancellableOut) {
      boolean terminated = false;

      @Override
      public void cancel() throws IOException {
        withSingleTermination(cancellableOut::cancel);
      }

      @Override
      public void close() throws IOException {
        withSingleTermination(cancellableOut::close);
      }

      private void withSingleTermination(IORunnable runnable) throws IOException {
        if (!terminated) {
          try {
            runnable.run();
          } finally {
            terminated = true;
          }
        }
      }
    };
  }

  private static final class SkipOutputStream extends FilterOutputStream {
    private long skip;

    SkipOutputStream(OutputStream out, long skip) {
      super(out);
      this.skip = skip;
    }

    @Override
    public void write(int b) throws IOException {
      if (skip > 0) {
        skip--;
      } else {
        super.write(b);
      }
    }

    @Override
    public void write(byte[] b) throws IOException {
      write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      if (skip > 0) {
        int skipLen = (int) Math.min(skip, len);
        skip -= skipLen;
        len -= skipLen;
        off += skipLen;
      }
      if (len > 0) {
        super.write(b, off, len);
      }
    }

    boolean isSkipped() {
      return skip == 0;
    }
  }

  private synchronized boolean referenceIfExists(Path key, Digest containingDirectory) throws IOException {
    Entry e = storage.get(key);
    if (e == null) {
      return false;
    }

    if (!entryExists(e)) {
      Entry removedEntry = storage.remove(key);
      if (removedEntry != null) {
        unlinkEntry(removedEntry);
      }
      return false;
    }

    if (containingDirectory != null) {
      e.containingDirectories.add(containingDirectory);
    }
    e.incrementReference();
    return true;
  }

  private CancellableOutputStream putOrReference(
      Path key,
      UUID writeId,
      Supplier<Boolean> writeWinner,
      long blobSizeInBytes,
      boolean isExecutable,
      Digest containingDirectory,
      Runnable onInsert)
      throws IOException, InterruptedException {
    AtomicBoolean requiresDischarge = new AtomicBoolean(false);
    try {
      CancellableOutputStream out = putOrReferenceGuarded(
          key,
          writeId,
          writeWinner,
          blobSizeInBytes,
          isExecutable,
          containingDirectory,
          onInsert,
          requiresDischarge);
      requiresDischarge.set(false); // stream now owns discharge
      return out;
    } finally {
      if (requiresDischarge.get()) {
        dischargeAndNotify(blobSizeInBytes);
      }
    }
  }

  private CancellableOutputStream putOrReferenceGuarded(
      Path key,
      UUID writeId,
      Supplier<Boolean> writeWinner,
      long blobSizeInBytes,
      boolean isExecutable,
      Digest containingDirectory,
      Runnable onInsert,
      AtomicBoolean requiresDischarge)
      throws IOException, InterruptedException {

    if (blobSizeInBytes > maxEntrySizeInBytes) {
      String fileName = key.getFileName().toString();
      FileEntryKey fileEntryKey = parseFileEntryKey(fileName);
      Digest digest;
      if (fileEntryKey == null) {
        logger.log(SEVERE, format("error parsing over limit key %s", key));
        digest = Digest.newBuilder()
            .setHash(fileName)
            .setSizeBytes(blobSizeInBytes)
            .build();
      } else {
        digest = fileEntryKey.getDigest();
      }
      throw new EntryLimitException(digest);
    }

    final ListenableFuture<Set<Digest>> expiredDigestsFuture;

    boolean interrupted = false;
    Iterable<ListenableFuture<Digest>> expiredDigestsFutures;
    synchronized (this) {
      if (referenceIfExists(key, containingDirectory)) {
        return DUPLICATE_OUTPUT_STREAM;
      }
      sizeInBytes += blobSizeInBytes;
      requiresDischarge.set(true);

      ImmutableList.Builder<ListenableFuture<Digest>> builder = ImmutableList.builder();
      try {
        while (!interrupted && sizeInBytes > maxSizeInBytes) {
          ListenableFuture<Path> expiredFuture = expireEntry(blobSizeInBytes, expireService);
          interrupted = Thread.interrupted();
          if (expiredFuture != null) {
            builder.add(
                transformAsync(
                    expiredFuture,
                    (expiredKey) -> {
                      try {
                        Files.delete(expiredKey);
                      } catch (NoSuchFileException eNoEnt) {
                        logger.severe(format("CASFileCache::putImpl: expired key %s did not exist to delete", expiredKey.toString()));
                      }
                      String fileName = expiredKey.getFileName().toString();
                      FileEntryKey fileEntryKey = parseFileEntryKey(fileName);
                      if (fileEntryKey == null) {
                        logger.severe(format("error parsing expired key %s", expiredKey));
                      } else if (storage.containsKey(getKey(fileEntryKey.getDigest(), !fileEntryKey.getIsExecutable()))) {
                        return immediateFuture(null);
                      }
                      return immediateFuture(fileEntryKey.getDigest());
                    },
                    expireService));
          }
        }
      } catch (InterruptedException e) {
        // clear interrupted flag
        Thread.interrupted();
        interrupted = true;
      }
      expiredDigestsFutures = builder.build();
    }

    ImmutableSet.Builder<Digest> builder = ImmutableSet.builder();
    for (ListenableFuture<Digest> expiredDigestFuture : expiredDigestsFutures) {
      Digest digest = getOrIOException(expiredDigestFuture);
      if (Thread.interrupted()) {
        interrupted = true;
      }
      if (digest != null) {
        builder.add(digest);
      }
    }
    Set<Digest> expiredDigests = builder.build();
    if (!expiredDigests.isEmpty()) {
      onExpire.accept(expiredDigests);
    }
    if (interrupted || Thread.currentThread().isInterrupted()) {
      throw new InterruptedException();
    }

    Path writePath = key.resolveSibling(key.getFileName() + "." + writeId);
    final long committedSize;
    final HashingOutputStream hashOut;
    if (Files.exists(writePath)) {
      committedSize = Files.size(writePath);
      try (InputStream in = Files.newInputStream(writePath)) {
        SkipOutputStream skipStream = new SkipOutputStream(
            Files.newOutputStream(writePath, APPEND), committedSize);
        hashOut = digestUtil.newHashingOutputStream(skipStream);
        ByteStreams.copy(in, hashOut);
        checkState(skipStream.isSkipped());
      }
    } else {
      committedSize = 0;
      hashOut = digestUtil.newHashingOutputStream(
          Files.newOutputStream(writePath, CREATE));
    }
    return new CancellableOutputStream(hashOut) {
      long written = committedSize;

      @Override
      public long getWritten() {
        return written;
      }

      @Override
      public Path getPath() {
        return writePath;
      }

      @Override
      public void cancel() throws IOException {
        try {
          hashOut.close();
          Files.delete(writePath);
        } finally {
          dischargeAndNotify(blobSizeInBytes);
        }
      }

      @Override
      public void write(int b) throws IOException {
        hashOut.write(b);
        written++;
      }

      @Override
      public void write(byte[] b) throws IOException {
        hashOut.write(b);
        written += b.length;
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        hashOut.write(b, off, len);
        written += len;
      }

      @Override
      public void close() throws IOException {
        // has some trouble with multiple closes, fortunately we have something above to handle this
        long size = getWritten();
        hashOut.close(); // should probably discharge here as well

        if (size > blobSizeInBytes) {
          String hash = hashOut.hash().toString();
          try {
            Files.delete(writePath);
          } finally {
            dischargeAndNotify(blobSizeInBytes);
          }
          Digest actual = Digest.newBuilder()
              .setHash(hash)
              .setSizeBytes(size)
              .build();
          Digest expected = keyToDigest(key, digestUtil);
          throw new DigestMismatchException(actual, expected);
        }

        if (size != blobSizeInBytes) {
          throw new IncompleteBlobException(writePath, key, size, blobSizeInBytes);
        }

        commit();
      }

      void commit() throws IOException {
        String hash = hashOut.hash().toString();
        String fileName = writePath.getFileName().toString();
        if (!fileName.startsWith(hash)) {
          dischargeAndNotify(blobSizeInBytes);
          Digest actual = Digest.newBuilder()
              .setHash(hash)
              .setSizeBytes(blobSizeInBytes)
              .build();
          Digest expected = keyToDigest(key, digestUtil);
          throw new DigestMismatchException(actual, expected);
        }
        try {
          setPermissions(writePath, isExecutable);
        } catch (IOException e) {
          dischargeAndNotify(blobSizeInBytes);
          throw e;
        }

        Entry entry = new Entry(
            key,
            blobSizeInBytes,
            containingDirectory,
            Deadline.after(10, SECONDS));

        Entry existingEntry = null;
        boolean inserted = false;
        try {
          Files.createLink(key, writePath);
          existingEntry = storage.putIfAbsent(key, entry);
          inserted = existingEntry == null;
        } catch (FileAlreadyExistsException e) {
          logger.finer("file already exists for " + key + ", nonexistent entry will fail");
        } finally {
          Files.delete(writePath);
          if (!inserted) {
            dischargeAndNotify(blobSizeInBytes);
          }
        }

        int attempts = 10;
        if (!inserted) {
          while (existingEntry == null && attempts-- != 0) {
            existingEntry = storage.get(key);
            try {
              MILLISECONDS.sleep(10);
            } catch (InterruptedException intEx) {
              throw new IOException(intEx);
            }
          }

          if (existingEntry == null) {
            throw new IOException("existing entry did not appear for " + key);
          }
        }

        if (existingEntry != null) {
          logger.finer("lost the race to insert " + key);
          if (!referenceIfExists(key, containingDirectory)) {
            // we would lose our accountability and have a presumed reference if we returned
            throw new IllegalStateException("storage conflict with existing key for " + key);
          }
        } else if (writeWinner.get()) {
          logger.finer("won the race to insert " + key);
          try {
            onInsert.run();
          } catch (RuntimeException e) {
            throw new IOException(e);
          }
        } else {
          logger.finer("did not win the race to insert " + key);
        }
      }
    };
  }

  private static void setPermissions(Path path, boolean isExecutable) throws IOException {
    new File(path.toString()).setExecutable(isExecutable, true);
  }

  @VisibleForTesting
  public static class Entry {
    Entry before, after;
    final Path key;
    final long size;
    final Set<Digest> containingDirectories;
    int referenceCount;
    Deadline existsDeadline;

    private Entry() {
      key = null;
      size = -1;
      containingDirectories = null;
      referenceCount = -1;
      existsDeadline = null;
    }

    public Entry(Path key, long size, Digest containingDirectory, Deadline existsDeadline) {
      this.key = key;
      this.size = size;
      referenceCount = 1;
      containingDirectories = Sets.newHashSet();
      if (containingDirectory != null) {
        containingDirectories.add(containingDirectory);
      }
      this.existsDeadline = existsDeadline;
    }

    public void unlink() {
      before.after = after;
      after.before = before;
      before = null;
      after = null;
    }

    protected void addBefore(Entry existingEntry) {
      after = existingEntry;
      before = existingEntry.before;
      before.after = this;
      after.before = this;
    }

    public void incrementReference() {
      if (referenceCount < 0) {
        throw new IllegalStateException("entry " + key + " has " + referenceCount + " references and is being incremented...");
      }
      logger.finest("incrementing references to " + key + " from " + referenceCount + " to " + (referenceCount+1));
      if (referenceCount == 0) {
        if (before == null || after == null) {
          throw new IllegalStateException("entry " + key + " has a broken link (" + before + ", " + after + ") and is being incremented");
        }
        unlink();
      }
      referenceCount++;
    }

    public void decrementReference(Entry header) {
      if (referenceCount == 0) {
        throw new IllegalStateException("entry " + key + " has 0 references and is being decremented...");
      }
      logger.finest("decrementing references to " + key + " from " + referenceCount + " to " + (referenceCount-1));
      referenceCount--;
      if (referenceCount == 0) {
        addBefore(header);
      }
    }

    public void recordAccess(Entry header) {
      if (referenceCount == 0) {
        if (before == null || after == null) {
          throw new IllegalStateException("entry " + key + " has a broken link (" + before + ", " + after + ") and is being recorded");
        }
        unlink();
        addBefore(header);
      }
    }
  }

  private static class SentinelEntry extends Entry {
    @Override
    public void unlink() {
      throw new UnsupportedOperationException("sentinal cannot be unlinked");
    }

    @Override
    protected void addBefore(Entry existingEntry) {
      throw new UnsupportedOperationException("sentinal cannot be added");
    }

    @Override
    public void incrementReference() {
      throw new UnsupportedOperationException("sentinal cannot be referenced");
    }

    @Override
    public void decrementReference(Entry header) {
      throw new UnsupportedOperationException("sentinal cannot be referenced");
    }

    @Override
    public void recordAccess(Entry header) {
      throw new UnsupportedOperationException("sentinal cannot be accessed");
    }
  }

  private static class DirectoryEntry {
    public final Directory directory;
    public final Iterable<Path> inputs;
    Deadline existsDeadline;

    public DirectoryEntry(Directory directory, Iterable<Path> inputs, Deadline existsDeadline) {
      this.directory = directory;
      this.inputs = inputs;
      this.existsDeadline = existsDeadline;
    }
  }

  protected abstract InputStream newExternalInput(Digest digest, long offset) throws IOException, InterruptedException;
}
