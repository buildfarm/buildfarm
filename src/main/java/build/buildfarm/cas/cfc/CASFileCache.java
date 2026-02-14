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

package build.buildfarm.cas.cfc;

import static build.buildfarm.common.DigestUtil.OMITTED_DIGEST_FUNCTIONS;
import static build.buildfarm.common.io.EvenMoreFiles.setReadOnlyPerms;
import static build.buildfarm.common.io.Utils.getOrIOException;
import static build.buildfarm.common.io.Utils.listDir;
import static build.buildfarm.common.io.Utils.stat;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.ByteStreams.nullOutputStream;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.Futures.whenAllComplete;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.lang.String.format;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import build.bazel.remote.execution.v2.BatchReadBlobsResponse.Response;
import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.SymlinkNode;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.DigestMismatchException;
import build.buildfarm.cas.cfc.LRUDB.SizeEntry;
import build.buildfarm.common.BuildfarmExecutors;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.EmptyInputStreamFactory;
import build.buildfarm.common.EntryLimitException;
import build.buildfarm.common.FailoverInputStreamFactory;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.common.Time;
import build.buildfarm.common.Write;
import build.buildfarm.common.Write.CompleteWrite;
import build.buildfarm.common.ZstdCompressingInputStream;
import build.buildfarm.common.ZstdDecompressingOutputStream;
import build.buildfarm.common.ZstdDecompressingOutputStream.FixedBufferPool;
import build.buildfarm.common.grpc.Retrier;
import build.buildfarm.common.grpc.Retrier.Backoff;
import build.buildfarm.common.io.CountingOutputStream;
import build.buildfarm.common.io.Directories;
import build.buildfarm.common.io.FeedbackOutputStream;
import build.buildfarm.common.io.FileStatus;
import build.buildfarm.v1test.BlobWriteKey;
import build.buildfarm.v1test.Digest;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.hash.HashingOutputStream;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import io.grpc.Deadline;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import java.io.BufferedReader;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import lombok.Getter;
import lombok.extern.java.Log;

@Log
public abstract class CASFileCache implements ContentAddressableStorage {
  // Prometheus metrics
  private static final Counter expiredKeyCounter =
      Counter.build().name("expired_key").help("Number of key expirations.").register();
  private static final Gauge casSizeMetric =
      Gauge.build().name("cas_size").help("CAS size.").register();
  private static final Gauge casEntryCountMetric =
      Gauge.build().name("cas_entry_count").help("Number of entries in the CAS.").register();
  private static Histogram casTtl =
      Histogram.build()
          .name("cas_ttl_s")
          .buckets(
              3600, // 1 hour
              21600, // 6 hours
              86400, // 1 day
              345600, // 4 days
              604800, // 1 week
              1210000 // 2 weeks
              )
          .help("The amount of time CAS entries live on L1 storage before expiration (seconds)")
          .register();

  private static final Counter readIOErrors =
      Counter.build().name("read_io_errors").help("Number of IO errors on read.").register();

  @Getter private final Path root;
  protected final EntryPathStrategy entryPathStrategy;
  protected final long maxSizeInBytes;
  protected final long maxEntrySizeInBytes;
  protected final ConcurrentMap<String, Entry> storage;
  private final Consumer<Digest> onPut;
  private final Consumer<Iterable<Digest>> onExpire;
  private final Executor accessRecorder;
  private final ExecutorService expireService;
  private final LRUDB db = new TextLRUDB();
  private volatile Deadline saveLRUAfter = Deadline.after(10, MINUTES);
  private final Path lru;

  private final FixedBufferPool zstdBufferPool;
  @Nullable private final ContentAddressableStorage delegate;
  private final boolean delegateSkipLoad;
  private final InputStreamFactory inputStreamFactory;
  private final LoadingCache<String, Lock> keyLocks =
      CacheBuilder.newBuilder()
          .expireAfterAccess(
              1, MINUTES) // hopefully long enough for any of our file ops to take place and prevent
          // collision
          .build(
              new CacheLoader<>() {
                @Override
                public Lock load(String key) {
                  // should be sufficient for what we're doing
                  return new ReentrantLock();
                }
              });
  private final LoadingCache<BlobWriteKey, Write> writes;

  private final LoadingCache<Digest, SettableFuture<Long>> writesInProgress =
      CacheBuilder.newBuilder()
          .expireAfterAccess(1, HOURS)
          .removalListener(
              (RemovalListener<Digest, SettableFuture<Long>>)
                  notification -> {
                    // no effect if already done
                    notification.getValue().setException(new IOException("write cancelled"));
                  })
          .build(
              new CacheLoader<>() {
                @SuppressWarnings("NullableProblems")
                @Override
                public SettableFuture<Long> load(Digest digest) {
                  SettableFuture<Long> future = SettableFuture.create();
                  if (containsLocal(digest, /* result= */ null, (key) -> {})) {
                    future.set(digest.getSize());
                  }
                  return future;
                }
              });

  protected FileStore fileStore; // bound to root
  protected transient long sizeInBytes = 0;
  protected final transient Entry header = new SentinelEntry();
  protected volatile long unreferencedEntryCount = 0;

  enum State {
    STOPPED,
    STARTING, // files are checked for existence/read
    WRITING, // only writable state
    READ_ONLY,
  }

  @GuardedBy("this")
  private State state = State.STOPPED;

  @GuardedBy("this")
  private long removedEntrySize = 0;

  @GuardedBy("this")
  private int removedEntryCount = 0;

  private Thread prometheusMetricsThread;

  public synchronized long size() {
    return sizeInBytes;
  }

  public long maxSize() {
    return maxSizeInBytes;
  }

  public long entryCount() {
    return storage.size();
  }

  public long unreferencedEntryCount() {
    return unreferencedEntryCount;
  }

  public long directoryStorageCount() {
    return 0;
  }

  public synchronized int getEvictedCount() {
    int count = removedEntryCount;
    removedEntryCount = 0;
    return count;
  }

  public synchronized long getEvictedSize() {
    long size = removedEntrySize;
    removedEntrySize = 0;
    return size;
  }

  public record CacheScanResults(
      List<Path> computeDirs, List<Path> deleteFiles, Map<Object, Entry> fileKeys) {}

  public record CacheLoadResults(
      boolean loadSkipped, CacheScanResults scan, List<Path> invalidDirectories) {}

  public record StartupCacheResults(
      Path cacheDirectory, CacheLoadResults load, Duration startupTime) {}

  public static class IncompleteBlobException extends IOException {
    IncompleteBlobException(Path writePath, String key, long committed, long expected) {
      super(
          format("blob %s => %s: committed %d, expected %d", writePath, key, committed, expected));
    }
  }

  public CASFileCache(
      Path root,
      long maxSizeInBytes,
      long maxEntrySizeInBytes,
      int hexBucketLevels,
      ExecutorService expireService,
      Executor accessRecorder,
      ConcurrentMap<String, Entry> storage,
      FixedBufferPool zstdBufferPool,
      Consumer<Digest> onPut,
      Consumer<Iterable<Digest>> onExpire,
      @Nullable ContentAddressableStorage delegate,
      boolean delegateSkipLoad,
      InputStreamFactory externalInputStreamFactory) {
    this.root = root;
    this.maxSizeInBytes = maxSizeInBytes;
    this.maxEntrySizeInBytes = maxEntrySizeInBytes;
    this.expireService = expireService;
    this.accessRecorder = accessRecorder;
    this.storage = storage;
    this.onPut = onPut;
    this.onExpire = onExpire;
    this.delegate = delegate;
    this.delegateSkipLoad = delegateSkipLoad;
    this.inputStreamFactory =
        new EmptyInputStreamFactory(
            new FailoverInputStreamFactory(this::newTransparentInput, externalInputStreamFactory));
    this.zstdBufferPool = zstdBufferPool;

    lru = root.resolve("lru.txt");

    writes =
        CacheBuilder.newBuilder()
            .expireAfterAccess(1, HOURS)
            .removalListener(
                (RemovalListener<BlobWriteKey, Write>)
                    notification -> expireService.execute(notification.getValue()::reset))
            .build(
                new CacheLoader<>() {
                  @SuppressWarnings("NullableProblems")
                  @Override
                  public Write load(BlobWriteKey key) {
                    return newWrite(key, CASFileCache.this.getFuture(key.getDigest()));
                  }
                });

    entryPathStrategy = new HexBucketEntryPathStrategy(root, hexBucketLevels);

    header.before = header.after = header;
  }

  protected static Digest keyToDigest(String key, long size, DigestUtil digestUtil)
      throws NumberFormatException {
    String[] components = key.split("_");

    int hashIndex = 0;

    if (!OMITTED_DIGEST_FUNCTIONS.contains(digestUtil.getDigestFunction())) {
      hashIndex++;
    }

    String hashComponent = components[hashIndex];

    return digestUtil.build(hashComponent, size);
  }

  protected static @Nullable DigestUtil parseDirectoryDigestUtil(String fileName) {
    String[] components = fileName.split("_");
    if ((components.length != 2 && components.length != 3)
        || !components[components.length - 1].equals("dir")) {
      return null;
    }

    if (components.length == 2) {
      // contains hash and "dir"
      return DigestUtil.parseHash(components[0]);
    }
    // contains expected digest function, hash, and "dir"
    return DigestUtil.forHash(components[0]);
  }

  /** Parses the given fileName into a FileEntryKey or null if parsing failed */
  private static @Nullable FileEntryKey parseFileEntryKey(String fileName, long size) {
    String[] components = fileName.split("_");

    if (components.length > 3) {
      return null;
    }

    // executable if last component of plural is "exec"
    boolean isExecutable =
        components.length > 1 && components[components.length - 1].equals("exec");
    if (!isExecutable && components.length > 2) {
      return null;
    }

    boolean hasDigestFunction = components.length == 3 || (!isExecutable && components.length == 2);
    DigestUtil digestUtil;
    if (hasDigestFunction) {
      digestUtil = DigestUtil.forHash(components[0]);
    } else {
      digestUtil = DigestUtil.parseHash(components[0]);
    }
    if (digestUtil == null) {
      return null;
    }

    try {
      Digest digest = digestUtil.build(components[hasDigestFunction ? 1 : 0], size);
      return new FileEntryKey(getKey(digest, isExecutable), size, isExecutable, digest);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private boolean contains(
      Digest digest,
      boolean isExecutable,
      @Nullable build.bazel.remote.execution.v2.Digest.Builder result,
      Consumer<String> onContains) {
    String key = getKey(digest, isExecutable);
    Entry entry = getEntry(key);
    if (entry != null && entry.referenceCount < 0) {
      try {
        entry = new Entry(key, Files.size(getPath(key)), null);
      } catch (IOException e) {
        return false;
      }
    }
    if (entry != null && (digest.getSize() < 0 || digest.getSize() == entry.size)) {
      if (result != null) {
        result.mergeFrom(DigestUtil.toDigest(digest)).setSizeBytes(entry.size);
      }
      onContains.accept(key);
      return true;
    }
    return false;
  }

  private void saveLRU() {
    List<SizeEntry> list = lruSizeEntryList();
    try {
      synchronized (lru) {
        db.save(list.iterator(), lru);
      }
    } catch (Exception e) {
      log.log(Level.SEVERE, "error saving lru state", e);
    }
  }

  /** Not reentrant, not thread safe */
  private void maybeSaveLRU() {
    if (saveLRUAfter == null || !saveLRUAfter.isExpired()) {
      return;
    }
    saveLRUAfter = null;
    expireService.execute(
        () -> {
          try {
            saveLRU();
          } finally {
            saveLRUAfter = Deadline.after(10, MINUTES);
          }
        });
  }

  private void accessed(Iterable<String> keys) {
    /* could also bucket these */
    try {
      accessRecorder.execute(
          () -> {
            recordAccess(keys);
            maybeSaveLRU();
          });
    } catch (RejectedExecutionException e) {
      log.log(Level.SEVERE, format("could not record access for %d keys", Iterables.size(keys)), e);
    }
  }

  private synchronized void recordAccess(Iterable<String> keys) {
    for (String key : keys) {
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

    if (Files.exists(getPath(e.key))) {
      e.existsDeadline = Deadline.after(10, SECONDS);
      return true;
    }
    return false;
  }

  boolean containsLocal(
      Digest digest,
      @Nullable build.bazel.remote.execution.v2.Digest.Builder result,
      Consumer<String> onContains) {
    /* maybe swap the order here if we're higher in ratio on one side */
    return contains(digest, false, result, onContains)
        || contains(digest, true, result, onContains);
  }

  @Override
  public Iterable<build.bazel.remote.execution.v2.Digest> findMissingBlobs(
      Iterable<build.bazel.remote.execution.v2.Digest> digests, DigestFunction.Value digestFunction)
      throws InterruptedException {
    ImmutableList.Builder<build.bazel.remote.execution.v2.Digest> builder = ImmutableList.builder();
    ImmutableList.Builder<String> found = ImmutableList.builder();
    build.bazel.remote.execution.v2.Digest.Builder result =
        build.bazel.remote.execution.v2.Digest.newBuilder();
    for (build.bazel.remote.execution.v2.Digest digest : digests) {
      if (digest.getSizeBytes() != 0
          && !containsLocal(DigestUtil.fromDigest(digest, digestFunction), result, found::add)) {
        builder.add(digest);
      } else if (digest.getSizeBytes() == -1) {
        // may misbehave with delegate
        builder.add(result.build());
      }
    }
    boolean recordAccess;
    synchronized (this) {
      recordAccess = state == State.WRITING || state == State.READ_ONLY;
    }
    if (recordAccess) {
      List<String> foundDigests = found.build();
      if (!foundDigests.isEmpty()) {
        accessed(foundDigests);
      }
    }
    ImmutableList<build.bazel.remote.execution.v2.Digest> missingDigests = builder.build();
    return CasFallbackDelegate.findMissingBlobs(delegate, missingDigests, digestFunction);
  }

  @Override
  public boolean contains(Digest digest, build.bazel.remote.execution.v2.Digest.Builder result) {
    return containsLocal(digest, result, (key) -> accessed(ImmutableList.of(key)))
        || CasFallbackDelegate.contains(delegate, digest, result);
  }

  @Override
  public ListenableFuture<List<Response>> getAllFuture(
      Iterable<build.bazel.remote.execution.v2.Digest> digests,
      DigestFunction.Value digestFunction) {
    throw new UnsupportedOperationException();
  }

  protected InputStream newTransparentInput(Compressor.Value compressor, Digest digest, long offset)
      throws IOException {
    try {
      return newLocalInput(compressor, digest, offset);
    } catch (NoSuchFileException e) {
      return CasFallbackDelegate.newInput(delegate, e, compressor, digest, offset);
    }
  }

  private InputStream compressorInputStream(Compressor.Value compressor, InputStream identity)
      throws IOException {
    if (compressor == Compressor.Value.IDENTITY) {
      return identity;
    }
    checkArgument(compressor == Compressor.Value.ZSTD);
    return new ZstdCompressingInputStream(identity);
  }

  private Entry getEntry(String key) {
    Entry entry = storage.get(key);
    if (entry != null) {
      return entry;
    }
    synchronized (this) {
      if (state != State.WRITING && state != State.READ_ONLY) {
        return new Entry();
      }
    }
    return null;
  }

  @SuppressWarnings({"ResultOfMethodCallIgnored", "PMD.CompareObjectsWithEquals"})
  InputStream newLocalInput(Compressor.Value compressor, Digest digest, long offset)
      throws IOException {
    // branch here or above for STARTING
    log.log(Level.FINER, format("getting input stream for %s", DigestUtil.toString(digest)));
    boolean isExecutable = false;
    do {
      String key = getKey(digest, isExecutable);
      Entry e = getEntry(key);
      if (e != null) {
        InputStream input = null;
        try {
          input = compressorInputStream(compressor, Files.newInputStream(getPath(key)));
          input.skip(offset);
        } catch (IOException ioEx) {
          if (!(ioEx instanceof NoSuchFileException)) {
            readIOErrors.inc();
            log.log(
                Level.WARNING,
                format("error opening %s at %d", DigestUtil.toString(digest), offset),
                ioEx);
          }

          if (e.referenceCount >= 0) {
            boolean removed = false;
            synchronized (this) {
              invalidateWrite(digest);
              Entry removedEntry = safeStorageRemoval(key);
              if (removedEntry == e) { // Intentional reference comparison
                unlinkEntry(removedEntry);
                removed = true;
              } else if (removedEntry != null) {
                log.severe(
                    format(
                        "nonexistent entry %s did not match last unreferenced entry, restoring it",
                        key));
                storage.put(key, removedEntry);
              }
            }
            if (removed && isExecutable) {
              onExpire.accept(ImmutableList.of(digest));
            }
            e = null;
          }
        }
        if (e != null && e.referenceCount >= 0) {
          accessed(ImmutableList.of(key));
        }
        if (input != null) {
          return input;
        }
      }
      isExecutable = !isExecutable;
    } while (isExecutable);
    throw new NoSuchFileException(DigestUtil.toString(digest));
  }

  @Override
  public InputStream newInput(Compressor.Value compressor, Digest digest, long offset)
      throws IOException {
    try {
      return newLocalInput(compressor, digest, offset);
    } catch (NoSuchFileException e) {
      if (delegate == null) {
        throw e;
      }
    }
    return newInputFallback(compressor, digest, offset);
  }

  @Override
  public Blob get(Digest digest) {
    try (InputStream in = newInput(Compressor.Value.IDENTITY, digest, /* offset= */ 0)) {
      return new Blob(ByteString.readFrom(in), digest);
    } catch (NoSuchFileException e) {
      return null;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static final int CHUNK_SIZE = 128 * 1024;

  private static boolean shouldReadThrough(RequestMetadata requestMetadata) {
    try {
      URI uri = new URI(requestMetadata.getCorrelatedInvocationsId());
      QueryStringDecoder decoder = new QueryStringDecoder(uri);
      return decoder
          .parameters()
          .getOrDefault("THROUGH", ImmutableList.of("false"))
          .getFirst()
          .equals("true");
    } catch (URISyntaxException e) {
      return false;
    }
  }

  @Override
  public void get(
      Compressor.Value compressor,
      Digest digest,
      long offset,
      long count,
      ServerCallStreamObserver<ByteString> blobObserver,
      RequestMetadata requestMetadata) {
    boolean readThrough = shouldReadThrough(requestMetadata);
    InputStream in;
    try {
      if (readThrough && !contains(digest, /* result= */ null)) {
        // really need to be able to reuse/restart the same write over
        // multiple requests - if we get successive read throughs for a single
        // digest, we should pick up from where we were last time
        // Also servers should affinitize
        // And share data, so that they can pick the same worker to pull from
        // if possible.
        Write write = getWrite(compressor, digest, UUID.randomUUID(), requestMetadata);
        blobObserver.setOnCancelHandler(write::reset);
        in =
            new ReadThroughInputStream(
                newExternalInput(compressor, digest, 0),
                localOffset -> newTransparentInput(compressor, digest, localOffset),
                digest.getSize(),
                offset,
                write);
      } else {
        in = newInput(compressor, digest, offset);
      }
    } catch (IOException e) {
      blobObserver.onError(e);
      return;
    }

    blobObserver.setOnCancelHandler(
        () -> {
          try {
            in.close();
          } catch (IOException e) {
            log.log(Level.SEVERE, "error closing input stream on cancel", e);
          }
        });
    byte[] buffer = new byte[CHUNK_SIZE];
    int initialLength;
    try {
      initialLength = in.read(buffer);
    } catch (IOException e) {
      try {
        in.close();
      } catch (IOException ioEx) {
        log.log(Level.SEVERE, "error closing input stream on error", ioEx);
      }
      blobObserver.onError(e);
      return;
    }
    final class ReadOnReadyHandler implements Runnable {
      private boolean wasReady = false;

      private int len = initialLength;

      @Override
      public void run() {
        if (blobObserver.isReady() && !wasReady) {
          wasReady = true;
          try {
            sendBuffer();
          } catch (IOException e) {
            log.log(Level.SEVERE, "error reading from input stream", e);
            try {
              in.close();
            } catch (IOException ioEx) {
              log.log(Level.SEVERE, "error closing input stream on error", ioEx);
            }
            blobObserver.onError(e);
          }
        }
      }

      void sendBuffer() throws IOException {
        while (len >= 0 && wasReady) {
          if (len != 0) {
            blobObserver.onNext(ByteString.copyFrom(buffer, 0, len));
          }
          len = in.read(buffer);
          if (!blobObserver.isReady()) {
            wasReady = false;
          }
        }
        if (len < 0) {
          in.close();
          blobObserver.onCompleted();
        }
      }
    }
    blobObserver.setOnReadyHandler(new ReadOnReadyHandler());
  }

  boolean completeWrite(Digest digest) {
    // this should be traded for an event emission
    try {
      onPut.accept(digest);
    } catch (RuntimeException e) {
      log.log(
          Level.SEVERE,
          "error during write completion onPut for " + DigestUtil.toString(digest),
          e);
      /* ignore error, writes must complete */
    }
    try {
      return getFuture(digest).set(digest.getSize());
    } catch (Exception e) {
      log.log(
          Level.SEVERE,
          "error getting write in progress future for " + DigestUtil.toString(digest),
          e);
      return false;
    }
  }

  void invalidateWrite(Digest digest) {
    writesInProgress.invalidate(digest);
  }

  // TODO stop ignoring onExpiration
  @Override
  public void put(Blob blob, Runnable onExpiration) throws InterruptedException {
    Digest digest = blob.getDigest();
    String key = getKey(digest, false);
    try {
      log.log(Level.FINER, format("put: %s", key));
      OutputStream out =
          putImpl(
              key,
              digest.getDigestFunction(),
              UUID.randomUUID(),
              () -> completeWrite(digest),
              digest.getSize(),
              /* isExecutable= */ false,
              () -> invalidateWrite(digest),
              /* isReset= */ true);
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
          decrementReference(key);
        }
      }
    } catch (IOException e) {
      log.log(Level.SEVERE, "error putting " + DigestUtil.toString(digest), e);
    }
  }

  @Override
  public synchronized boolean isReadOnly() {
    return state != State.WRITING;
  }

  public synchronized boolean setReadOnly(boolean value) {
    if (value && state == State.WRITING) {
      state = State.READ_ONLY;
      return true;
    }
    if (!value && state == State.READ_ONLY) {
      state = State.WRITING;
      notifyAll(); // ensure that we wake up any waits for this state
      return true;
    }
    return false;
  }

  @Override
  public synchronized void waitForWritable(Duration timeout) throws InterruptedException {
    wait(timeout.toMillis(), (int) (timeout.toNanos() % 1000000));
  }

  @Override
  public Write getWrite(
      Compressor.Value compressor, Digest digest, UUID uuid, RequestMetadata requestMetadata)
      throws EntryLimitException {
    if (digest.getSize() == 0) {
      return new CompleteWrite(0);
    }
    if (digest.getSize() > maxEntrySizeInBytes) {
      throw new EntryLimitException(digest.getSize(), maxEntrySizeInBytes);
    }
    try {
      return writes.get(
          BlobWriteKey.newBuilder()
              .setDigest(digest)
              .setIdentifier(uuid.toString())
              .setCompressor(compressor)
              .build());
    } catch (ExecutionException e) {
      String compression = "";
      if (compressor == Compressor.Value.ZSTD) {
        compression = "zstd compressed ";
      }
      log.log(
          Level.SEVERE,
          "error getting " + compression + "write for " + DigestUtil.toString(digest) + ":" + uuid,
          e);
      throw new IllegalStateException("write create must not fail", e.getCause());
    }
  }

  SettableFuture<Long> getFuture(Digest digest) {
    try {
      return writesInProgress.get(digest);
    } catch (ExecutionException e) {
      Throwables.throwIfUnchecked(e.getCause());
      throw new UncheckedExecutionException(e.getCause());
    }
  }

  private static class UniqueWriteOutputStream extends CancellableOutputStream {
    private final CancellableOutputStream out;
    private final Consumer<Boolean> onClosed;
    private final long size;
    private boolean closed = false;

    UniqueWriteOutputStream(CancellableOutputStream out, Consumer<Boolean> onClosed, long size) {
      super(out);
      this.out = out;
      this.onClosed = onClosed;
      this.size = size;
    }

    // available to refer to replicable stream
    CancellableOutputStream delegate() {
      return out;
    }

    @Override
    public void write(int b) throws IOException {
      if (closed) {
        throw new IOException("write output stream is closed");
      }
      super.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
      write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      if (closed) {
        throw new IOException("write output stream is closed");
      }
      super.write(b, off, len);
    }

    @Override
    public void close() throws IOException {
      // disallow any further writes
      closed = true;
      try {
        // we ignore closes below the complete size
        if (out.getWrittenForClose() >= size) {
          super.close();
        }
      } finally {
        onClosed.accept(/* cancelled= */ false);
      }
    }

    @Override
    public void cancel() throws IOException {
      try {
        out.cancel();
      } finally {
        onClosed.accept(/* cancelled= */ true);
      }
    }
  }

  Write newWrite(BlobWriteKey key, SettableFuture<Long> future) {
    Write write =
        new Write() {
          CancellableOutputStream out = null;

          @GuardedBy("this")
          boolean isReset = false;

          @GuardedBy("this")
          SettableFuture<Void> closedFuture = null;

          @GuardedBy("this")
          long fileCommittedSize = -1;

          @Override
          public synchronized void reset() {
            try {
              if (out != null) {
                out.cancel();
                out = null;
              }
            } catch (IOException e) {
              log.log(
                  Level.SEVERE,
                  "could not reset write "
                      + DigestUtil.toString(key.getDigest())
                      + ":"
                      + key.getIdentifier(),
                  e);
            } finally {
              if (closedFuture != null) {
                closedFuture.set(null);
              }
              isReset = true;
            }
          }

          @Override
          public synchronized long getCommittedSize() {
            long committedSize = getCommittedSizeFromOutOrDisk();
            if (committedSize == 0 && out == null) {
              isReset = true;
            }
            return committedSize;
          }

          long getCommittedSizeFromOutOrDisk() {
            if (isComplete()) {
              return key.getDigest().getSize();
            }
            return getCommittedSizeFromOut();
          }

          synchronized long getCommittedSizeFromOut() {
            if (out == null) {
              if (fileCommittedSize < 0) {
                // we need to cache this from disk until an out stream is acquired
                String blobKey = getKey(key.getDigest(), false);
                Path blobKeyPath = getPath(blobKey);
                try {
                  fileCommittedSize =
                      Files.size(blobKeyPath.resolveSibling(blobKey + "." + key.getIdentifier()));
                } catch (IOException e) {
                  fileCommittedSize = 0;
                }
              }
              return fileCommittedSize;
            }
            return out.getWritten();
          }

          @Override
          public synchronized boolean isComplete() {
            return getFuture().isDone()
                || ((closedFuture == null || closedFuture.isDone())
                    && containsLocal(key.getDigest(), /* result= */ null, (key) -> {}));
          }

          @Override
          public synchronized ListenableFuture<FeedbackOutputStream> getOutputFuture(
              long offset,
              long deadlineAfter,
              TimeUnit deadlineAfterUnits,
              Runnable onReadyHandler) {
            if (closedFuture == null || closedFuture.isDone()) {
              try {
                // this isn't great, and will block when there are multiple requesters
                return immediateFuture(
                    getOutput(offset, deadlineAfter, deadlineAfterUnits, onReadyHandler));
              } catch (IOException e) {
                return immediateFailedFuture(e);
              }
            }
            return transformAsync(
                closedFuture,
                result ->
                    getOutputFuture(offset, deadlineAfter, deadlineAfterUnits, onReadyHandler),
                directExecutor());
          }

          private synchronized void syncCancelled() {
            out = null;
            isReset = true;
          }

          @Override
          public synchronized FeedbackOutputStream getOutput(
              long offset, long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler)
              throws IOException {
            // caller will be the exclusive owner of this write stream. all other requests
            // will block until it is returned via a close.
            if (closedFuture != null) {
              try {
                while (!closedFuture.isDone()) {
                  wait();
                }
                closedFuture.get();
              } catch (ExecutionException e) {
                throw new IOException(e.getCause());
              } catch (InterruptedException e) {
                throw new IOException(e);
              }
            }
            if (offset == 0) {
              reset();
            }
            if (isComplete()) {
              if (!future.isDone()) {
                log.log(Level.WARNING, format("%s isComplete but has not completed future", key));
                future.set(key.getDigest().getSize());
              }
              throw new WriteCompleteException();
            }
            checkState(
                getCommittedSize() == offset,
                format(
                    "cannot position stream to %d, committed_size is %d",
                    offset, getCommittedSize()));
            SettableFuture<Void> outClosedFuture = SettableFuture.create();
            Digest digest = key.getDigest();
            UniqueWriteOutputStream uniqueOut =
                createUniqueWriteOutput(
                    out,
                    digest,
                    UUID.fromString(key.getIdentifier()),
                    cancelled -> {
                      if (cancelled) {
                        syncCancelled();
                      }
                      outClosedFuture.set(null);
                    },
                    this::isComplete,
                    isReset);
            if (uniqueOut.getPath() == null) {
              // this is a duplicate output stream and the write is complete
              future.set(key.getDigest().getSize());
              return uniqueOut;
            } else {
              commitOpenState(uniqueOut.delegate(), outClosedFuture);
              switch (key.getCompressor()) {
                case IDENTITY:
                  return uniqueOut;
                case ZSTD:
                  return new ZstdDecompressingOutputStream(uniqueOut, zstdBufferPool);
                default:
                  throw new UnsupportedOperationException(
                      "Unsupported compressor " + key.getCompressor());
              }
            }
          }

          private synchronized void syncNotify() {
            notify();
          }

          private synchronized void commitOpenState(
              CancellableOutputStream out, SettableFuture<Void> closedFuture) {
            // transition the Write to an open state, and modify all internal state required
            // atomically
            // this function must. not. throw.

            this.out = out;
            this.closedFuture = closedFuture;
            closedFuture.addListener(this::syncNotify, directExecutor());
            // they will likely write to this, so we can no longer assume isReset.
            // might want to subscribe to a write event on the stream
            isReset = false;
            // our cached file committed size is now invalid
            fileCommittedSize = -1;
          }

          @Override
          public ListenableFuture<Long> getFuture() {
            return future;
          }
        };
    write.getFuture().addListener(write::reset, expireService);
    return write;
  }

  UniqueWriteOutputStream createUniqueWriteOutput(
      CancellableOutputStream out,
      Digest digest,
      UUID uuid,
      Consumer<Boolean> onClosed,
      BooleanSupplier isComplete,
      boolean isReset)
      throws IOException {
    if (out == null) {
      out = newOutput(digest, uuid, isComplete, isReset);
    }
    if (out == null) {
      // duplicate output stream
      out =
          new CancellableOutputStream(nullOutputStream()) {
            @Override
            public long getWritten() {
              return digest.getSize();
            }

            @Override
            public void cancel() {}

            @Override
            public Path getPath() {
              return null;
            }
          };
    }

    // this stream is uniquely assigned to the consumer, can be closed,
    // and will properly reject any subsequent write activity with an
    // exception. It will not close the underlying stream unless we have
    // reached our digest point (or beyond).
    return new UniqueWriteOutputStream(out, onClosed, digest.getSize());
  }

  CancellableOutputStream newOutput(
      Digest digest, UUID uuid, BooleanSupplier isComplete, boolean isReset) throws IOException {
    String key = getKey(digest, false);
    final CancellableOutputStream cancellableOut;
    try {
      log.log(Level.FINER, format("getWrite: %s", key));
      cancellableOut =
          putImpl(
              key,
              digest.getDigestFunction(),
              uuid,
              () -> completeWrite(digest),
              digest.getSize(),
              /* isExecutable= */ false,
              () -> invalidateWrite(digest),
              isReset);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
    if (cancellableOut == null) {
      decrementReference(key);
      return null;
    }
    return new CancellableOutputStream(cancellableOut) {
      final AtomicBoolean closed = new AtomicBoolean(false);

      @Override
      public void write(int b) throws IOException {
        try {
          super.write(b);
        } catch (ClosedChannelException e) {
          if (!isComplete.getAsBoolean()) {
            throw e;
          }
        }
      }

      @Override
      public void write(byte[] b) throws IOException {
        try {
          super.write(b);
        } catch (ClosedChannelException e) {
          if (!isComplete.getAsBoolean()) {
            throw e;
          }
        }
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        try {
          super.write(b, off, len);
        } catch (ClosedChannelException e) {
          if (!isComplete.getAsBoolean()) {
            throw e;
          }
        }
      }

      @Override
      public void cancel() throws IOException {
        if (closed.compareAndSet(/* expected= */ false, /* update= */ true)) {
          cancellableOut.cancel();
        }
      }

      @Override
      public void close() throws IOException {
        if (closed.compareAndSet(/* expected= */ false, /* update= */ true)) {
          try {
            out.close();
            decrementReference(key);
          } catch (IncompleteBlobException e) {
            // ignore
          }
        }
      }

      @Override
      public long getWrittenForClose() {
        return cancellableOut.getWrittenForClose();
      }
    };
  }

  @Override
  public void put(Blob blob) throws InterruptedException {
    put(blob, /* onExpiration= */ null);
  }

  @Override
  public long maxEntrySize() {
    return maxEntrySizeInBytes;
  }

  private record FileEntryKey(String key, long size, boolean isExecutable, Digest digest) {}

  public void initializeRootDirectory() throws IOException {
    for (Path dir : entryPathStrategy) {
      Files.createDirectories(dir);
    }
    fileStore = Files.getFileStore(root);
  }

  @SuppressWarnings({"PMD.CompareObjectsWithEquals"})
  private synchronized List<SizeEntry> lruSizeEntryList() {
    /**
     * Steps the entries in order from oldest to newest access The order is used here to insert into
     * the lru on load
     */
    List<SizeEntry> list = new ArrayList<>(storage.size());
    for (Entry current = header.after; current != header; current = checkNotNull(current.after)) {
      list.add(new SizeEntry(current.key, current.size));
    }
    return list;
  }

  public synchronized void stop() throws IOException, InterruptedException {
    if (prometheusMetricsThread != null) {
      prometheusMetricsThread.interrupt();
      prometheusMetricsThread.join();
    }
    // lock ordering, [this] -> [lru]
    // path used as lock due to isolation by filename
    saveLRU();
    state = State.STOPPED;
  }

  public ListenableFuture<Void> start(boolean skipLoad) {
    return start(newDirectExecutorService(), skipLoad, /* writable= */ true);
  }

  public ListenableFuture<Void> start(
      ExecutorService removeDirectoryService, boolean skipLoad, boolean writable) {
    return start(onPut, removeDirectoryService, skipLoad, writable);
  }

  public ListenableFuture<Void> start(
      Consumer<Digest> onStartPut,
      ExecutorService removeDirectoryService,
      boolean skipLoad,
      boolean writable) {
    synchronized (this) {
      checkState(state == State.STOPPED);
      state = State.STARTING;
    }
    CasFallbackDelegate.start(
        delegate, onStartPut, removeDirectoryService, delegateSkipLoad, writable);

    return listeningDecorator(expireService)
        .submit(
            () -> {
              startRoutine(onStartPut, removeDirectoryService, skipLoad);
              synchronized (this) {
                saveLRUAfter = Deadline.after(10, MINUTES);
                state = writable ? State.WRITING : State.READ_ONLY;
                if (writable) {
                  notifyAll();
                }
              }
              return null;
            });
  }

  /**
   * initialize the cache for persistent storage and inject any consistent entries which already
   * exist under the root into the storage map. This call will create the root if it does not exist,
   * and will scale in cost with the number of files already present.
   */
  private void startRoutine(
      Consumer<Digest> onStartPut, ExecutorService removeDirectoryService, boolean skipLoad)
      throws IOException, InterruptedException {
    log.log(Level.INFO, "Initializing cache at: " + root);
    Instant startTime = Instant.now();

    // Load the cache
    if (!skipLoad) {
      initializeRootDirectory();
      loadCache(onStartPut, removeDirectoryService);
    } else {
      // Skip loading the cache and ensure it is empty
      fileStore = Files.getFileStore(root);
      Directories.remove(root, fileStore, removeDirectoryService);
      initializeRootDirectory();
    }

    // Calculate Startup time
    Instant endTime = Instant.now();
    Duration startupTime = Duration.between(startTime, endTime);
    log.log(Level.INFO, "Startup Time: " + startupTime.getSeconds() + "s");

    // Start metrics collection thread
    prometheusMetricsThread =
        new Thread(
            () -> {
              while (!Thread.currentThread().isInterrupted()) {
                try {
                  casSizeMetric.set(size());
                  casEntryCountMetric.set(entryCount());
                  MINUTES.sleep(5);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  break;
                } catch (Exception e) {
                  log.log(Level.SEVERE, "Could not update CasFileCache metrics", e);
                }
              }
            },
            "Prometheus CAS Metrics Collector");
    prometheusMetricsThread.start();
  }

  protected CacheLoadResults loadCache(
      Consumer<Digest> onStartPut, ExecutorService removeDirectoryService)
      throws IOException, InterruptedException {
    // Phase 1: Scan
    // build scan cache results by analyzing each file on the root.
    CacheScanResults scan = scanRoot(onStartPut);
    logCacheScanResults(scan);
    deleteInvalidFileContent(scan.deleteFiles, removeDirectoryService);

    // Phase 2: Compute
    // recursively construct all directory structures.
    List<Path> invalidDirectories = computeDirectories(scan);
    logComputeDirectoriesResults(invalidDirectories);
    deleteInvalidFileContent(invalidDirectories, removeDirectoryService);

    return new CacheLoadResults(false, scan, invalidDirectories);
  }

  private void deleteInvalidFileContent(List<Path> files, ExecutorService removeDirectoryService) {
    for (Path path : files) {
      try {
        if (Files.isDirectory(path)) {
          Directories.remove(path, fileStore, removeDirectoryService);
        } else {
          Files.delete(path);
        }
      } catch (Exception e) {
        log.log(Level.SEVERE, "failure to delete CAS content: ", e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void logCacheScanResults(CacheScanResults cacheScanResults) {
    Map<String, Integer> map =
        Map.of(
            "dirs", cacheScanResults.computeDirs.size(),
            "delete", cacheScanResults.deleteFiles.size());
    log.log(Level.INFO, new Gson().toJson(map));
  }

  @SuppressWarnings("unchecked")
  private void logComputeDirectoriesResults(List<Path> invalidDirectories) {
    Map<String, Integer> map = Map.of("invalid dirs", invalidDirectories.size());
    log.log(Level.INFO, new Gson().toJson(map));
  }

  protected boolean shouldDeleteBranchFile(Path branchDir, String name) {
    return !name.matches("[0-9a-f]{2}");
  }

  private CacheScanResults scanRoot(Consumer<Digest> onStartPut)
      throws IOException, InterruptedException {
    // create thread pool
    ExecutorService pool = BuildfarmExecutors.getScanCachePool();

    // collect keys from cache root.
    ImmutableList.Builder<Path> computeDirsBuilder = new ImmutableList.Builder<>();
    ImmutableList.Builder<Path> deleteFilesBuilder = new ImmutableList.Builder<>();

    // TODO invalidate mismatched hash prefix
    Set<Path> files = new HashSet<>();
    for (Path path : entryPathStrategy) {
      files.addAll(listDir(path));
    }

    for (Path branchDir : entryPathStrategy.branchDirectories()) {
      for (Path file : listDir(branchDir)) {
        // allow migration for digest-y names
        String name = file.getFileName().toString();
        if (shouldDeleteBranchFile(branchDir, name)) {
          deleteFilesBuilder.add(file);
        }
      }
    }

    // TODO test for hex bins
    try (BufferedReader br = Files.newBufferedReader(lru)) {
      for (SizeEntry entry : db.entries(br)) {
        // ignore files in the lru that are not present in the directories
        Path path = entryPathStrategy.getPath(entry.key());
        if (files.remove(path)) {
          processRootFile(onStartPut, path, entry, computeDirsBuilder, deleteFilesBuilder);
        }
      }
      // prevent the lru db from being processed -> removed in the purge below
      files.remove(lru);
    } catch (NoSuchFileException e) {
      // ignore - LRU file doesn't exist, will scan all files
    } catch (Exception e) {
      // Handle corrupted LRU file - delete it and fall back to full scan
      log.log(
          Level.WARNING,
          "LRU file is corrupted and cannot be parsed. Deleting corrupted LRU file and falling back"
              + " to full cache scan.",
          e);
      try {
        Files.deleteIfExists(lru);
        log.log(Level.INFO, "Deleted corrupted LRU file: " + lru);
      } catch (IOException deleteEx) {
        log.log(Level.SEVERE, "Failed to delete corrupted LRU file: " + lru, deleteEx);
      }
      // Continue with full scan - all files will be processed in the loop below
    }
    for (Path file : files) {
      String basename = file.getFileName().toString();
      pool.execute(
          () -> {
            try {
              FileStatus stat = stat(file, false, fileStore);
              processRootFile(
                  onStartPut,
                  file,
                  new SizeEntry(basename, stat.getSize()),
                  computeDirsBuilder,
                  deleteFilesBuilder);
            } catch (Exception e) {
              log.log(Level.SEVERE, "error reading file " + file.toString(), e);
            }
          });
    }

    joinThreads(pool, "Scanning Cache Root...");

    // log information from scanning cache root.
    return new CacheScanResults(computeDirsBuilder.build(), deleteFilesBuilder.build(), null);
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  private void processRootFile(
      Consumer<Digest> onStartPut,
      Path path,
      SizeEntry entry,
      ImmutableList.Builder<Path> computeDirs,
      ImmutableList.Builder<Path> deleteFiles)
      throws IOException {
    String basename = entry.key();

    // mark directory for later key compute
    if (basename.endsWith("_dir")) {
      synchronized (computeDirs) {
        computeDirs.add(path);
      }
    } else {
      // if cas is full or entry is oversized or empty, mark file for later deletion.
      long size = entry.size();
      if (sizeInBytes + size > maxSizeInBytes || size > maxEntrySizeInBytes || size == 0) {
        synchronized (deleteFiles) {
          deleteFiles.add(path);
        }
      } else {
        // get the key entry from the file name.
        FileEntryKey fileEntryKey = parseFileEntryKey(basename, size);

        // if key entry file name cannot be parsed, mark file for later deletion.
        if (fileEntryKey == null) {
          synchronized (deleteFiles) {
            deleteFiles.add(path);
          }
        } else {
          String key = fileEntryKey.key();
          // populate key if it is not currently stored.
          Entry e = new Entry(key, size, Deadline.after(10, SECONDS));
          checkState(storage.put(e.key, e) == null, key);
          onStartPut.accept(fileEntryKey.digest());
          synchronized (this) {
            if (e.decrementReference(header)) {
              unreferencedEntryCount++;
            }
          }
          sizeInBytes += size;
        }
      }
    }
  }

  @SuppressWarnings("ConstantConditions")
  protected abstract List<Path> computeDirectories(CacheScanResults cacheScanResults)
      throws InterruptedException;

  @SuppressWarnings("ResultOfMethodCallIgnored")
  protected static void joinThreads(ExecutorService pool, String message)
      throws InterruptedException {
    pool.shutdown();
    while (!pool.isTerminated()) {
      log.log(Level.INFO, message);
      pool.awaitTermination(1, MINUTES);
    }
  }

  static String digestFilename(Digest digest) {
    return optionalDigestFunction(digest.getDigestFunction()) + digest.getHash();
  }

  private static String optionalDigestFunction(DigestFunction.Value digestFunction) {
    if (OMITTED_DIGEST_FUNCTIONS.contains(digestFunction)) {
      return "";
    }
    return digestFunction.toString().toLowerCase() + "_";
  }

  public static String getKey(Digest digest, boolean isExecutable) {
    return digestFilename(digest) + (isExecutable ? "_exec" : "");
  }

  public synchronized void decrementReference(String inputFile) throws IOException {
    if (decrementInputReferences(ImmutableList.of(inputFile)) > 0) {
      notify();
    }
  }

  public abstract void decrementReferences(
      Iterable<String> inputFiles,
      Iterable<build.bazel.remote.execution.v2.Digest> inputDirectories,
      DigestFunction.Value digestFunction)
      throws IOException, InterruptedException;

  @SuppressWarnings("NonAtomicOperationOnVolatileField")
  protected int decrementInputReferences(Iterable<String> inputFiles) {
    int entriesDereferenced = 0;
    for (String input : inputFiles) {
      checkNotNull(input);
      Entry e = storage.get(input);
      if (e == null) {
        throw new IllegalStateException(input + " has been removed with references");
      }
      if (!e.key.equals(input)) {
        throw new RuntimeException("ERROR: entry retrieved: " + e.key + " != " + input);
      }
      if (e.decrementReference(header)) {
        entriesDereferenced++;
        unreferencedEntryCount++;
      }
    }
    return entriesDereferenced;
  }

  public Path getPath(String filename) {
    return entryPathStrategy.getPath(filename);
  }

  protected Path getRemovingPath(String filename) {
    return entryPathStrategy.getPath(filename + "_removed");
  }

  private synchronized void dischargeAndNotify(String key, long size) {
    discharge(key, size);
    notify();
  }

  protected synchronized void discharge(String key, long size) {
    sizeInBytes -= size;
    removedEntryCount++;
    removedEntrySize += size;
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

  protected String getDirectoryKey(Digest digest) {
    return digestFilename(digest) + "_dir";
  }

  @VisibleForTesting
  public Path getDirectoryPath(Digest digest) {
    return getPath(getDirectoryKey(digest));
  }

  @GuardedBy("this")
  @SuppressWarnings("PMD.CompareObjectsWithEquals")
  private Entry waitForLastUnreferencedEntry(long blobSizeInBytes) throws InterruptedException {
    while (header.after == header) { // Intentional reference comparison
      int references = 0;
      int keys = 0;
      int min = -1;
      int max = 0;
      String minkey = null;
      String maxkey = null;
      log.log(
          Level.INFO,
          format(
              "CASFileCache::expireEntry(%d) header(%s): { after: %s, before: %s }",
              blobSizeInBytes,
              header.hashCode(),
              header.after.hashCode(),
              header.before.hashCode()));
      // this should be incorporated in the listenable future construction...
      for (Map.Entry<String, Entry> pe : storage.entrySet()) {
        String key = pe.getKey();
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
          log.log(
              Level.INFO,
              format(
                  "CASFileCache::expireEntry(%d) unreferenced entry(%s): { after: %s, before: %s }",
                  blobSizeInBytes,
                  e.hashCode(),
                  e.after == null ? null : e.after.hashCode(),
                  e.before == null ? null : e.before.hashCode()));
        }
        references += e.referenceCount;
        keys++;
      }
      if (keys == 0) {
        throw new IllegalStateException(
            "CASFileCache::expireEntry("
                + blobSizeInBytes
                + ") there are no keys to wait for expiration on");
      }
      log.log(
          Level.INFO,
          format(
              "CASFileCache::expireEntry(%d) unreferenced list is empty, %d bytes, %d keys with %d"
                  + " references, min(%d, %s), max(%d, %s)",
              blobSizeInBytes, sizeInBytes, keys, references, min, minkey, max, maxkey));
      wait();
      if (sizeInBytes <= maxSizeInBytes) {
        return null;
      }
    }
    return header.after;
  }

  protected abstract List<ListenableFuture<Void>> unlinkAndExpireDirectories(
      Entry entry, ExecutorService service);

  protected ListenableFuture<Entry> dischargeEntryFuture(Entry entry, ExecutorService service) {
    List<ListenableFuture<Void>> directoryExpirationFutures =
        unlinkAndExpireDirectories(entry, service);
    discharge(entry.key, entry.size);
    return whenAllComplete(directoryExpirationFutures)
        .call(
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
                    log.log(
                        Level.SEVERE,
                        "undeferrable exception during discharge of " + entry.key,
                        cause);
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
              return entry;
            },
            service);
  }

  @GuardedBy("this")
  private void dischargeEntry(Entry entry, ExecutorService service) throws Exception {
    Exception expirationException = null;
    for (ListenableFuture<Void> directoryExpirationFuture :
        unlinkAndExpireDirectories(entry, service)) {
      do {
        try {
          directoryExpirationFuture.get();
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          if (cause instanceof Exception) {
            expirationException = (Exception) cause;
          } else {
            log.log(Level.SEVERE, "undeferrable exception during discharge of " + entry.key, cause);
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
    discharge(entry.key, entry.size);
    if (expirationException != null) {
      throw expirationException;
    }
  }

  // clears the interrupted status
  private static boolean causedByInterrupted(Exception e) {
    return Thread.interrupted()
        || e.getCause() instanceof InterruptedException
        || e instanceof ClosedByInterruptException;
  }

  protected Entry safeStorageInsertion(String key, Entry entry) {
    Lock lock;
    try {
      lock = keyLocks.get(key);
    } catch (ExecutionException e) {
      // impossible without exception instantiating lock
      throw new RuntimeException(e.getCause());
    }

    lock.lock();
    try {
      return storage.putIfAbsent(key, entry);
    } finally {
      lock.unlock();
    }
  }

  private Entry safeStorageRemoval(String key) throws IOException {
    Path path = getPath(key);
    Path expiredPath = getRemovingPath(key);
    boolean deleteExpiredPath = false;

    Lock lock;
    try {
      lock = keyLocks.get(key);
    } catch (ExecutionException e) {
      // impossible without exception instantiating lock
      throw new IOException(e);
    }

    Entry entry;
    lock.lock();
    // why are we splitting IO between this and dischargeEntryFuture?
    try {
      if (key.endsWith("_dir")) {
        Files.move(path, expiredPath, ATOMIC_MOVE, REPLACE_EXISTING);
      } else {
        Files.createLink(expiredPath, path);
        deleteExpiredPath = true;
        Files.delete(path);
        deleteExpiredPath = false;
      }
    } catch (NoSuchFileException e) {
      // ignore, already removed
    } finally {
      entry = storage.remove(key);
      if (deleteExpiredPath) {
        // only valid for files with 2-step
        try {
          Files.delete(expiredPath);
        } catch (IOException e) {
          log.log(Level.SEVERE, "error cleaning up after failed safeStorageRemoval", e);
        }
      }
      lock.unlock();
    }
    // the directory remains, guess we should get the service in here??
    return entry;
  }

  @SuppressWarnings({"NonAtomicOperationOnVolatileField", "PMD.CompareObjectsWithEquals"})
  @GuardedBy("this")
  private ListenableFuture<Entry> expireEntry(long blobSizeInBytes, ExecutorService service)
      throws IOException, InterruptedException {
    for (Entry e = waitForLastUnreferencedEntry(blobSizeInBytes);
        e != null;
        e = waitForLastUnreferencedEntry(blobSizeInBytes)) {
      if (e.referenceCount != 0) {
        throw new IllegalStateException(
            "ERROR: Reference counts lru ordering has not been maintained correctly, attempting to"
                + " expire referenced (or negatively counted) content "
                + e.key
                + " with "
                + e.referenceCount
                + " references");
      }
      boolean interrupted = false;
      if (!e.key.endsWith("_dir")) {
        FileEntryKey fileEntryKey = parseFileEntryKey(e.key, e.size);
        if (fileEntryKey == null) {
          log.log(Level.SEVERE, format("error parsing expired key %s", e.key));
        } else {
          try {
            expireEntryFallback(fileEntryKey);
          } catch (IOException ioEx) {
            interrupted = causedByInterrupted(ioEx);
          }
          invalidateWrite(fileEntryKey.digest());
        }
      }
      Entry removedEntry = safeStorageRemoval(e.key);
      // reference compare on purpose
      if (removedEntry == e) {
        ListenableFuture<Entry> entryFuture = dischargeEntryFuture(e, service);
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
        return entryFuture;
      }
      if (removedEntry == null) {
        log.log(Level.SEVERE, format("entry %s was already removed during expiration", e.key));
        if (e.isLinked()) {
          log.log(Level.SEVERE, format("removing spuriously non-existent entry %s", e.key));
          e.unlink();
          unreferencedEntryCount--;
        } else {
          log.log(
              Level.SEVERE,
              format(
                  "spuriously non-existent entry %s was somehow unlinked, should not appear again",
                  e.key));
        }
      } else {
        log.log(
            Level.SEVERE,
            "removed entry %s did not match last unreferenced entry, restoring it",
            e.key);
        storage.put(e.key, removedEntry);
      }
      // possibly delegated, but no removal, if we're interrupted, abort loop
      if (interrupted || Thread.currentThread().isInterrupted()) {
        throw new InterruptedException();
      }
    }
    return null;
  }

  interface FileContent {
    void call(Path filePath, Path cacheFilePath, long size, boolean isExecutable) throws Exception;
  }

  @SuppressWarnings("ConstantConditions")
  private void putDirectoryFiles(
      DigestFunction.Value digestFunction,
      Iterable<FileNode> files,
      Iterable<SymlinkNode> symlinks,
      Path path,
      FileContent onFileContent,
      ImmutableList.Builder<ListenableFuture<Path>> putFutures,
      ExecutorService service) {
    for (FileNode fileNode : files) {
      boolean isExecutable = fileNode.getIsExecutable();
      Path filePath = path.resolve(fileNode.getName());
      final ListenableFuture<Path> putFuture;
      if (fileNode.getDigest().getSizeBytes() != 0) {
        Digest digest = DigestUtil.fromDigest(fileNode.getDigest(), digestFunction);
        putFuture =
            transformAsync(
                put(digest, isExecutable, service),
                cacheFilePath -> {
                  try {
                    onFileContent.call(
                        filePath, cacheFilePath.path(), digest.getSize(), isExecutable);
                  } catch (Exception e) {
                    return immediateFailedFuture(e);
                  }
                  return immediateFuture(filePath);
                },
                service);
      } else {
        putFuture =
            listeningDecorator(service)
                .submit(
                    () -> {
                      Files.createFile(filePath);
                      setReadOnlyPerms(filePath, isExecutable, fileStore);
                      return filePath;
                    });
      }
      putFutures.add(putFuture);
    }
    for (SymlinkNode symlinkNode : symlinks) {
      Path symlinkPath = path.resolve(symlinkNode.getName());
      putFutures.add(
          listeningDecorator(service)
              .submit(
                  () -> {
                    Path relativeTargetPath = root.getFileSystem().getPath(symlinkNode.getTarget());
                    checkState(!relativeTargetPath.isAbsolute());
                    Files.createSymbolicLink(symlinkPath, relativeTargetPath);
                    return symlinkPath;
                  }));
    }
  }

  protected void fetchDirectory(
      Path rootPath,
      Digest digest,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      FileContent onFileContent,
      ImmutableList.Builder<ListenableFuture<Path>> putFutures,
      ExecutorService service)
      throws IOException, InterruptedException {
    Stack<Map.Entry<Path, Directory>> stack = new Stack<>();
    stack.push(
        new AbstractMap.SimpleEntry<>(
            rootPath, getDirectoryFromDigest(directoriesIndex, rootPath, digest)));
    while (!stack.isEmpty()) {
      Map.Entry<Path, Directory> pathDirectoryPair = stack.pop();
      Path path = pathDirectoryPair.getKey();
      Directory directory = pathDirectoryPair.getValue();

      removeFilePath(path);
      Files.createDirectory(path);
      putDirectoryFiles(
          digest.getDigestFunction(),
          directory.getFilesList(),
          directory.getSymlinksList(),
          path,
          onFileContent,
          putFutures,
          service);
      for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
        Path subPath = path.resolve(directoryNode.getName());
        stack.push(
            new AbstractMap.SimpleEntry<>(
                subPath,
                getDirectoryFromDigest(
                    directoriesIndex,
                    subPath,
                    DigestUtil.fromDigest(directoryNode.getDigest(), digest.getDigestFunction()))));
      }
    }
  }

  private void removeFilePath(Path path) throws IOException {
    if (Files.exists(path)) {
      if (Files.isDirectory(path)) {
        log.log(Level.FINER, "removing existing directory " + path + " for fetch");
        Directories.remove(path, fileStore);
      } else {
        Files.delete(path);
      }
    }
  }

  private Directory getDirectoryFromDigest(
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      Path path,
      Digest digest)
      throws IOException {
    Directory directory;
    if (digest.getSize() == 0) {
      directory = Directory.getDefaultInstance();
    } else {
      directory = directoriesIndex.get(DigestUtil.toDigest(digest));
    }
    if (directory == null) {
      throw new IOException(
          format("directory not found for %s(%s)", path, DigestUtil.toString(digest)));
    }
    return directory;
  }

  public record PathResult(Path path, boolean isMissed) {}

  public abstract ListenableFuture<PathResult> putDirectory(
      Digest digest,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      ExecutorService service);

  @VisibleForTesting
  public PathResult put(Digest digest, boolean isExecutable)
      throws IOException, InterruptedException {
    checkState(digest.getSize() > 0, "file entries may not be empty");

    return putAndCopy(digest, isExecutable);
  }

  // This can result in deadlock if called with a direct executor. I'm unsure how to guard
  // against it, until we can get to using a current-download future
  public ListenableFuture<PathResult> put(Digest digest, boolean isExecutable, Executor executor) {
    checkState(digest.getSize() > 0, "file entries may not be empty");

    return transformAsync(
        immediateFuture(null),
        (result) -> immediateFuture(putAndCopy(digest, isExecutable)),
        executor);
  }

  @SuppressWarnings("ThrowFromFinallyBlock")
  PathResult putAndCopy(Digest digest, boolean isExecutable)
      throws IOException, InterruptedException {
    String key = getKey(digest, isExecutable);
    boolean downloadComplete = false;
    CancellableOutputStream out =
        putImpl(
            key,
            digest.getDigestFunction(),
            UUID.randomUUID(),
            () -> completeWrite(digest),
            digest.getSize(),
            isExecutable,
            () -> invalidateWrite(digest),
            /* isReset= */ true);
    if (out != null) {
      try {
        copyExternalInput(digest, out);
        downloadComplete = true;
      } finally {
        try {
          log.log(Level.FINER, format("closing output stream for %s", DigestUtil.toString(digest)));
          if (downloadComplete) {
            out.close();
          } else {
            out.cancel();
          }
          log.log(Level.FINER, format("output stream closed for %s", DigestUtil.toString(digest)));
        } catch (IOException e) {
          if (Thread.interrupted()) {
            log.log(
                Level.SEVERE,
                format("could not close stream for %s", DigestUtil.toString(digest)),
                e);
            //noinspection deprecation
            Throwables.propagateIfInstanceOf(e.getCause(), InterruptedException.class);
            throw new InterruptedException();
          } else {
            log.log(
                Level.FINER,
                format("failed output stream close for %s", DigestUtil.toString(digest)),
                e);
          }
          throw e;
        }
      }
    }
    return new PathResult(getPath(key), downloadComplete);
  }

  private void copyExternalInputProgressive(Digest digest, CancellableOutputStream out)
      throws IOException, InterruptedException {
    try (InputStream in = newExternalInput(Compressor.Value.IDENTITY, digest, out.getWritten())) {
      ByteStreams.copy(in, out);
    }
  }

  private static Exception extractStatusException(IOException e) {
    for (Throwable cause = e.getCause(); cause != null; cause = cause.getCause()) {
      if (cause instanceof StatusException statusException) {
        return statusException;
      } else if (cause instanceof StatusRuntimeException statusRuntimeException) {
        return statusRuntimeException;
      }
    }
    return e;
  }

  private void copyExternalInput(Digest digest, CancellableOutputStream out)
      throws IOException, InterruptedException {
    Retrier retrier = new Retrier(Backoff.sequential(5), Retrier.DEFAULT_IS_RETRIABLE);
    log.log(Level.FINER, format("downloading %s", DigestUtil.toString(digest)));
    try {
      retrier.execute(
          () -> {
            while (out.getWritten() < digest.getSize()) {
              try {
                copyExternalInputProgressive(digest, out);
              } catch (IOException e) {
                throw extractStatusException(e);
              }
            }
            return null;
          });
    } catch (IOException e) {
      out.cancel();
      log.log(
          Level.WARNING,
          format(
              "error downloading %s: %s",
              DigestUtil.toString(digest),
              e.getMessage())); // prevent burial by early end of stream during close
      throw e;
    }
    log.log(Level.FINER, format("download of %s complete", DigestUtil.toString(digest)));
  }

  @FunctionalInterface
  private interface IORunnable {
    void run() throws IOException;
  }

  @VisibleForTesting
  abstract static class CancellableOutputStream extends WriteOutputStream {
    CancellableOutputStream(OutputStream out) {
      super(out);
    }

    CancellableOutputStream(WriteOutputStream out) {
      super(out);
    }

    abstract void cancel() throws IOException;

    long getWrittenForClose() {
      return getWritten();
    }
  }

  private static final CancellableOutputStream DUPLICATE_OUTPUT_STREAM =
      new CancellableOutputStream(nullOutputStream()) {
        @Override
        void cancel() {}
      };

  private CancellableOutputStream putImpl(
      String key,
      DigestFunction.Value digestFunction,
      UUID writeId,
      Supplier<Boolean> writeWinner,
      long blobSizeInBytes,
      boolean isExecutable,
      Runnable onInsert,
      boolean isReset)
      throws IOException, InterruptedException {
    CancellableOutputStream out =
        putOrReference(
            key,
            digestFunction,
            writeId,
            writeWinner,
            blobSizeInBytes,
            isExecutable,
            onInsert,
            isReset);
    if (out == DUPLICATE_OUTPUT_STREAM) {
      return null;
    }
    log.log(Level.FINER, format("entry %s is missing, downloading and populating", key));
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

      @Override
      public long getWrittenForClose() {
        return cancellableOut.getWrittenForClose();
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

  protected synchronized boolean referenceIfExists(String key) throws IOException {
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

    if (e.incrementReference()) {
      unreferencedEntryCount--;
    }
    return true;
  }

  private CancellableOutputStream putOrReference(
      String key,
      DigestFunction.Value digestFunction,
      UUID writeId,
      Supplier<Boolean> writeWinner,
      long blobSizeInBytes,
      boolean isExecutable,
      Runnable onInsert,
      boolean isReset)
      throws IOException, InterruptedException {
    AtomicBoolean requiresDischarge = new AtomicBoolean(false);
    try {
      CancellableOutputStream out =
          putOrReferenceGuarded(
              key,
              digestFunction,
              writeId,
              writeWinner,
              blobSizeInBytes,
              isExecutable,
              onInsert,
              requiresDischarge,
              isReset);
      requiresDischarge.set(false); // stream now owns discharge
      return out;
    } finally {
      if (requiresDischarge.get()) {
        dischargeAndNotify("put:" + key, blobSizeInBytes);
      }
    }
  }

  protected void deleteExpiredKey(String key) throws IOException {
    Path path = getRemovingPath(key);
    long createdTimeMs = Files.getLastModifiedTime(path).to(MILLISECONDS);

    Files.delete(path);

    publishExpirationMetric(createdTimeMs);
  }

  private void publishExpirationMetric(long createdTimeMs) {
    // TODO introduce ttl clock
    long currentTimeMs = new Date().getTime();
    long ttlMs = currentTimeMs - createdTimeMs;
    casTtl.observe(Time.millisecondsToSeconds(ttlMs));
  }

  @SuppressWarnings({"ConstantConditions", "ResultOfMethodCallIgnored"})
  protected boolean charge(String key, long blobSizeInBytes, AtomicBoolean requiresDischarge)
      throws IOException, InterruptedException {
    boolean interrupted = false;
    Iterable<ListenableFuture<Digest>> expiredDigestsFutures;
    synchronized (this) {
      if (referenceIfExists(key)) {
        return false;
      }
      sizeInBytes += blobSizeInBytes;
      requiresDischarge.set(true);

      ImmutableList.Builder<ListenableFuture<Digest>> builder = ImmutableList.builder();
      try {
        while (!interrupted && sizeInBytes > maxSizeInBytes) {
          ListenableFuture<Entry> expiredFuture = expireEntry(blobSizeInBytes, expireService);
          interrupted = Thread.interrupted();
          if (expiredFuture != null) {
            builder.add(
                transformAsync(
                    expiredFuture,
                    (expiredEntry) -> {
                      String expiredKey = expiredEntry.key;
                      try {
                        deleteExpiredKey(expiredKey);
                      } catch (NoSuchFileException eNoEnt) {
                        log.log(
                            Level.SEVERE,
                            format(
                                "CASFileCache::putImpl: expired key %s did not exist to delete",
                                expiredKey),
                            eNoEnt);
                      }
                      FileEntryKey fileEntryKey = parseFileEntryKey(expiredKey, expiredEntry.size);
                      if (fileEntryKey != null
                          && storage.containsKey(
                              getKey(fileEntryKey.digest(), !fileEntryKey.isExecutable()))) {
                        return immediateFuture(null);
                      }
                      expiredKeyCounter.inc();
                      return immediateFuture(fileEntryKey != null ? fileEntryKey.digest() : null);
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
    return true;
  }

  private CancellableOutputStream putOrReferenceGuarded(
      String key,
      DigestFunction.Value digestFunction,
      UUID writeId,
      Supplier<Boolean> writeWinner,
      long blobSizeInBytes,
      boolean isExecutable,
      Runnable onInsert,
      AtomicBoolean requiresDischarge,
      boolean isReset)
      throws IOException, InterruptedException {
    if (blobSizeInBytes > maxEntrySizeInBytes) {
      throw new EntryLimitException(blobSizeInBytes, maxEntrySizeInBytes);
    }

    if (!charge(key, blobSizeInBytes, requiresDischarge)) {
      return DUPLICATE_OUTPUT_STREAM;
    }

    DigestUtil digestUtil = new DigestUtil(HashFunction.get(digestFunction));
    String writeKey = key + "." + writeId;
    Path writePath = getPath(key).resolveSibling(writeKey);
    final long committedSize;
    HashingOutputStream hashOut;
    if (!isReset && Files.exists(writePath)) {
      committedSize = Files.size(writePath);
      try (InputStream in = Files.newInputStream(writePath)) {
        // TODO this might not be completely safe - best to maybe avoid opening the
        // file for write before we're ready to write to it, could do it with a lazy
        // open
        SkipOutputStream skipStream =
            new SkipOutputStream(Files.newOutputStream(writePath, APPEND), committedSize);
        hashOut = digestUtil.newHashingOutputStream(skipStream);
        ByteStreams.copy(in, hashOut);
        in.close();
        checkState(skipStream.isSkipped());
      }
    } else {
      committedSize = 0;
      hashOut = digestUtil.newHashingOutputStream(Files.newOutputStream(writePath, CREATE));
    }
    Supplier<String> hashSupplier = () -> hashOut.hash().toString();
    CountingOutputStream countingOut = new CountingOutputStream(committedSize, hashOut);
    return new CancellableOutputStream(countingOut) {
      long written = committedSize;
      final Digest expectedDigest = keyToDigest(key, blobSizeInBytes, digestUtil);

      @Override
      public long getWritten() {
        return countingOut.written();
      }

      // must report a size that can be considered closeable
      @Override
      public long getWrittenForClose() {
        try {
          out.flush();
        } catch (IOException e) {
          // technically no harm no foul
        }
        return getWritten();
      }

      @Override
      public Path getPath() {
        return writePath;
      }

      @Override
      public void cancel() throws IOException {
        try {
          out.close();
          Files.delete(writePath);
        } finally {
          dischargeAndNotify(writeKey, blobSizeInBytes);
        }
      }

      @Override
      public void write(int b) throws IOException {
        if (getWritten() >= blobSizeInBytes) {
          throw new IOException(
              format("attempted overwrite at %d by 1 byte for %s", getWritten(), writeKey));
        }
        out.write(b);
      }

      @Override
      public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        long written = getWritten();
        if (written + len > blobSizeInBytes) {
          throw new IOException(
              format("attempted overwrite at %d by %d bytes for %s", written, len, writeKey));
        }
        out.write(b, off, len);
        if (getWritten() > blobSizeInBytes) {
          throw new IOException(
              format("overwrite at %d by %d bytes for %s", written, len, writeKey));
        }
      }

      @Override
      public void close() throws IOException {
        out.flush();
        long size = countingOut.written();
        // has some trouble with multiple closes, fortunately we have something above to handle this
        out.close(); // should probably discharge here as well

        if (size > blobSizeInBytes) {
          String hash = hashSupplier.get();
          try {
            Files.delete(writePath);
          } finally {
            dischargeAndNotify(writeKey, blobSizeInBytes);
          }
          Digest actual = digestUtil.build(hash, size);
          throw new DigestMismatchException(actual, expectedDigest);
        }

        if (size != blobSizeInBytes) {
          throw new IncompleteBlobException(writePath, key, size, blobSizeInBytes);
        }

        commit();
      }

      void commit() throws IOException {
        String hash = hashSupplier.get();
        String fileName = writePath.getFileName().toString();
        Digest actual = digestUtil.build(hash, countingOut.written());
        if (!fileName.equals(getKey(actual, isExecutable) + "." + writeId)) {
          dischargeAndNotify(writeKey, blobSizeInBytes);
          throw new DigestMismatchException(actual, expectedDigest);
        }
        try {
          setReadOnlyPerms(writePath, isExecutable, fileStore);
        } catch (IOException e) {
          dischargeAndNotify(writeKey, blobSizeInBytes);
          throw e;
        }

        Entry entry = new Entry(key, blobSizeInBytes, Deadline.after(10, SECONDS));

        Entry existingEntry = null;
        boolean inserted = false;
        try {
          // acquire the key lock
          Files.createLink(CASFileCache.this.getPath(key), writePath);
          existingEntry = safeStorageInsertion(key, entry);
          inserted = existingEntry == null;
        } catch (FileAlreadyExistsException e) {
          log.log(Level.FINER, "file already exists for " + key + ", nonexistent entry will fail");
        } finally {
          Files.delete(writePath);
          if (!inserted) {
            dischargeAndNotify(writeKey, blobSizeInBytes);
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
          log.log(Level.FINER, "lost the race to insert " + key);
          if (!referenceIfExists(key)) {
            // we would lose our accountability and have a presumed reference if we returned
            throw new IllegalStateException("storage conflict with existing key for " + key);
          }
        } else if (writeWinner.get()) {
          log.log(Level.FINER, "won the race to insert " + key);
          try {
            onInsert.run();
          } catch (RuntimeException e) {
            throw new IOException(e);
          }
        } else {
          log.log(Level.FINER, "did not win the race to insert " + key);
        }
      }
    };
  }

  @VisibleForTesting
  public static class Entry {
    Entry before;
    Entry after;
    final String key;
    final long size;
    int referenceCount;
    Deadline existsDeadline;

    private Entry() {
      key = null;
      size = -1;
      referenceCount = -1;
      existsDeadline = null;
    }

    public Entry(String key, long size, Deadline existsDeadline) {
      this.key = key;
      this.size = size;
      referenceCount = 1;
      this.existsDeadline = existsDeadline;
    }

    public boolean isLinked() {
      return before != null && after != null;
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

    // return true iff the entry's state is changed from unreferenced to referenced
    public boolean incrementReference() {
      if (referenceCount < 0) {
        throw new IllegalStateException(
            "entry " + key + " has " + referenceCount + " references and is being incremented...");
      }
      if (referenceCount == 0) {
        if (!isLinked()) {
          throw new IllegalStateException(
              "entry "
                  + key
                  + " has a broken link ("
                  + before
                  + ", "
                  + after
                  + ") and is being incremented");
        }
        unlink();
      }
      return referenceCount++ == 0;
    }

    // return true iff the entry's state is changed from referenced to unreferenced
    public boolean decrementReference(Entry header) {
      if (referenceCount == 0) {
        throw new IllegalStateException(
            "entry " + key + " has 0 references and is being decremented...");
      }
      if (--referenceCount == 0) {
        addBefore(header);
        return true;
      }
      return false;
    }

    public void recordAccess(Entry header) {
      if (referenceCount == 0) {
        if (!isLinked()) {
          throw new IllegalStateException(
              "entry "
                  + key
                  + " has a broken link ("
                  + before
                  + ", "
                  + after
                  + ") and is being recorded");
        }
        unlink();
        addBefore(header);
      }
    }
  }

  private static final class SentinelEntry extends Entry {
    @Override
    public void unlink() {
      throw new UnsupportedOperationException("sentinal cannot be unlinked");
    }

    @Override
    protected void addBefore(Entry existingEntry) {
      throw new UnsupportedOperationException("sentinal cannot be added");
    }

    @Override
    public boolean incrementReference() {
      throw new UnsupportedOperationException("sentinal cannot be referenced");
    }

    @Override
    public boolean decrementReference(Entry header) {
      throw new UnsupportedOperationException("sentinal cannot be referenced");
    }

    @Override
    public void recordAccess(Entry header) {
      throw new UnsupportedOperationException("sentinal cannot be accessed");
    }
  }

  private InputStream newExternalInput(Compressor.Value compressor, Digest digest, long offset)
      throws IOException {
    return inputStreamFactory.newInput(compressor, digest, offset);
  }

  // CAS fallback methods

  private InputStream newInputFallback(Compressor.Value compressor, Digest digest, long offset)
      throws IOException {
    checkNotNull(delegate);

    if (digest.getSize() > maxEntrySizeInBytes) {
      return delegate.newInput(compressor, digest, offset);
    }
    Write write =
        getWrite(compressor, digest, UUID.randomUUID(), RequestMetadata.getDefaultInstance());
    return newReadThroughInput(compressor, digest, offset, write);
  }

  ReadThroughInputStream newReadThroughInput(
      Compressor.Value compressor, Digest digest, long offset, Write write) throws IOException {
    return new ReadThroughInputStream(
        delegate.newInput(compressor, digest, 0),
        localOffset -> newTransparentInput(compressor, digest, localOffset),
        digest.getSize(),
        offset,
        write);
  }

  private void expireEntryFallback(FileEntryKey fileEntryKey) throws IOException {
    if (delegate != null) {
      Write write =
          delegate.getWrite(
              Compressor.Value.IDENTITY,
              fileEntryKey.digest(),
              UUID.randomUUID(),
              RequestMetadata.getDefaultInstance());
      if (write != null) {
        performCopy(write, fileEntryKey.key());
      }
    }
  }

  private void performCopy(Write write, String key) throws IOException {
    try (OutputStream out = write.getOutput(1, MINUTES, () -> {});
        InputStream in = Files.newInputStream(getPath(key))) {
      ByteStreams.copy(in, out);
    } catch (IOException ioEx) {
      boolean interrupted = causedByInterrupted(ioEx);
      if (interrupted || !write.isComplete()) {
        write.reset();
        log.log(Level.SEVERE, format("error delegating expired entry %s", key), ioEx);
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
        throw ioEx;
      }
    }
  }
}
