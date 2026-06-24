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
import static build.buildfarm.common.io.Utils.listDir;
import static build.buildfarm.common.io.Utils.stat;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.ByteStreams.nullOutputStream;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transformAsync;
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
import build.buildfarm.common.Size;
import build.buildfarm.common.Time;
import build.buildfarm.common.Write;
import build.buildfarm.common.Write.CompleteWrite;
import build.buildfarm.common.WritesHelper;
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
import com.google.common.hash.HashingOutputStream;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
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
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.CopyOption;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.stream.Stream;
import javax.annotation.concurrent.GuardedBy;
import lombok.Getter;
import lombok.extern.java.Log;
import org.jspecify.annotations.Nullable;

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

  // --- A/B comparable metrics (baseline + change) for the CAS concurrency project ---
  // cas_charge_seconds: wall time of charge() (admit a write). On the baseline this includes the
  // global-lock stop-the-world inline eviction; on this change branch eviction has moved to an
  // async sharded evictor, so charge() now only delegates to casShards.charge() (which may apply
  // backpressure). Keeping the timer here keeps the admit-latency series comparable across both.
  private static final Histogram casChargeSeconds =
      Histogram.build()
          .name("cas_charge_seconds")
          .buckets(0.0001, 0.001, 0.01, 0.1, 1, 5, 30, 60, 120)
          .help("Wall time of CASFileCache.charge() to admit a write.")
          .register();
  // cas_lookups_total{result}: blob-presence lookups in findMissingBlobs by result. The hit rate
  // (hit/(hit+miss)) proves sharding-by-digest did not hurt CAS dedup effectiveness.
  private static final Counter casLookups =
      Counter.build()
          .name("cas_lookups_total")
          .labelNames("result")
          .help("CAS blob presence lookups in findMissingBlobs (result=hit present / miss absent).")
          .register();

  @Getter private final Path root;
  protected final EntryPathStrategy entryPathStrategy;
  protected final long maxSizeInBytes;
  protected final long maxEntrySizeInBytes;
  protected final ConcurrentMap<String, Entry> storage;
  private final Consumer<Digest> onPut;
  private final Consumer<Iterable<Digest>> onExpire;
  private final ExecutorService expireService;
  private final LRUDB db = new TextLRUDB();

  // Snapshot files from a previous run at a different shard count (or the legacy lru.txt) that the
  // startup scan loaded as recency hints and must rewrite-then-delete once the LRU is fully
  // populated. Set during scanRoot, consumed by migrateSnapshotsIfPending after computeDirectories
  // (so the rewrite captures _dir entries). Empty when no migration is pending. Single-threaded
  // startup access; no synchronization needed.
  private List<Path> pendingMigrationStaleFiles = List.of();
  private Set<String> pendingMigrationOldNLabels = Set.of();

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
                  notification -> cancelWriteFuture(notification.getValue()))
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

  private static final long DEFAULT_BLOCK_SIZE = 4096;
  private static final long ESTIMATED_BYTES_PER_DIRECTORY_ENTRY = 32;

  protected FileStore fileStore; // bound to root
  protected long blockSize = DEFAULT_BLOCK_SIZE;

  /**
   * Indirection over {@link Files#move} so tests can inject {@code Files.move} failure modes (e.g.
   * {@code ENOSPC} / {@code EIO}) at the eviction-rename step. Default wraps the real call.
   */
  @FunctionalInterface
  interface FileMover {
    void move(Path source, Path target, CopyOption... options) throws IOException;
  }

  private volatile FileMover fileMover = Files::move;

  @VisibleForTesting
  void setFileMoverForTesting(FileMover fileMover) {
    this.fileMover = fileMover;
  }

  // One LRU sentinel header per shard, constructed once and handed to CasShards; each shard's
  // evictor thread becomes the sole writer of its own list after start(). The startup scan
  // populates the lists (routed by shard) before start() runs. headers[i] anchors shard i's list.
  private final Entry[] headers;
  protected final CasShards casShards;

  private State state = new State();

  private Thread prometheusMetricsThread;
  private final AtomicLong startupAdmissionBytes = new AtomicLong();

  // Phase 3 startup map population. Set false at the top of scanRoot and true once the standalone
  // file scan has fully admitted every standalone Entry into storage. computeDirectory's
  // CAS-directory-hardlink reconstruction looks up source Entries by inode, so it must not run
  // until the standalone scan is complete (Phase 3 invariant 8). Always-on Preconditions check
  // (not a -ea assert) per codebase convention.
  protected volatile boolean standaloneScanComplete;

  // Inode -> standalone Entry, built during startup so computeDirectory can pin the source Entries
  // that existing on-disk _dir trees hardlink. This is cleared immediately after directory
  // reconstruction; retaining it would duplicate one inode map entry per scanned standalone file
  // for the cache lifetime.
  private ConcurrentMap<Object, Entry> startupFileKeys = new ConcurrentHashMap<>();

  public long size() {
    return casShards.globalTotalBytes();
  }

  public long maxSize() {
    return maxSizeInBytes;
  }

  public long entryCount() {
    return storage.size();
  }

  /**
   * Number of LIVE entries currently linked on the LRU. Most linked entries are unreferenced
   * eviction candidates, but a concurrent acquire can temporarily leave a referenced entry linked;
   * the sweep's refcount recheck still governs eviction. Best-effort operator-visibility metric;
   * not consulted for any admission or capacity decision.
   */
  public long unreferencedEntryCount() {
    return casShards.globalUnreferencedCount();
  }

  public long directoryStorageCount() {
    return 0;
  }

  public int getEvictedCount() {
    return casShards.getEvictedCount();
  }

  public long getEvictedSize() {
    return casShards.getEvictedSize();
  }

  @VisibleForTesting
  EvictorShard evictorForTesting() {
    // Shard 0. Tests run at N=1 (the existing-test-preservation rule), so shard 0 is the only
    // shard and is equivalent to the pre-2.2 single evictor. Multi-shard tests use
    // casShardsForTesting.
    return casShards.shard(0);
  }

  @VisibleForTesting
  CasShards casShardsForTesting() {
    return casShards;
  }

  @VisibleForTesting
  Entry headerForTesting() {
    return headers[0];
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
    this(
        root,
        maxSizeInBytes,
        maxEntrySizeInBytes,
        hexBucketLevels,
        expireService,
        accessRecorder,
        storage,
        zstdBufferPool,
        onPut,
        onExpire,
        delegate,
        delegateSkipLoad,
        externalInputStreamFactory,
        /* lowBytes= */ 0, // sentinel; full-arg constructor fills in defaultLowBytes
        EvictorShard.DEFAULT_WAKE_BUDGET_NANOS,
        EvictorShard.DEFAULT_IDLE_HEARTBEAT_NANOS,
        Clock.systemUTC());
  }

  /**
   * Fallback low-watermark byte budget for direct constructor callers that pass {@code lowBytes <=
   * 0}. Production callers go through {@code Worker.computeLowBytes}, which derives a percent-aware
   * target; this 80% fallback is for tests and programmatic constructions without a {@link Cas}
   * config in hand.
   */
  private static long defaultLowBytes(long maxSizeInBytes) {
    return maxSizeInBytes * 80 / 100;
  }

  /**
   * Role-aware auto-derive of the evictor shard count from vCPU, exposed for {@code Worker} (which
   * is in another package and cannot reach the package-private {@link CasShards}). See {@link
   * CasShards#deriveShardCount}.
   */
  public static int deriveEvictorShardCount(int vCpu, boolean executionRole) {
    return CasShards.deriveShardCount(vCpu, executionRole);
  }

  private static void checkShardCountCanHoldAdmissibleEntry(
      long maxSizeInBytes, long maxEntrySizeInBytes, int shardCount) {
    int maxShardCount = Size.maxShardCountForEntrySize(maxSizeInBytes, maxEntrySizeInBytes);
    checkArgument(
        shardCount <= maxShardCount,
        "shardCount (%s) cannot exceed %s for maxSizeInBytes=%s and maxEntrySizeInBytes=%s",
        shardCount,
        maxShardCount,
        maxSizeInBytes,
        maxEntrySizeInBytes);
  }

  /**
   * Full-arg constructor accepting evictor tunables explicitly. Used by tests that need to exercise
   * specific watermarks / sweep budgets / heartbeats, and by the public constructor which fills in
   * the defaults. {@code lowBytes <= 0} is the sentinel for "use {@link #defaultLowBytes(long)}";
   * callers that have a Cas config with an unset {@code lowWatermarkPercent} pass 0 through to opt
   * into the default. Defaults to a single evictor shard (the pre-2.2 behavior); production callers
   * use the {@code shardCount} overload below.
   */
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
      InputStreamFactory externalInputStreamFactory,
      long lowBytes,
      long wakeBudgetNanos,
      long idleHeartbeatNanos,
      Clock clock) {
    this(
        root,
        maxSizeInBytes,
        maxEntrySizeInBytes,
        hexBucketLevels,
        expireService,
        accessRecorder,
        storage,
        zstdBufferPool,
        onPut,
        onExpire,
        delegate,
        delegateSkipLoad,
        externalInputStreamFactory,
        lowBytes,
        wakeBudgetNanos,
        idleHeartbeatNanos,
        clock,
        /* shardCount= */ 1);
  }

  /**
   * Full-arg constructor with an explicit evictor shard count (Phase 2.2). The CAS is partitioned
   * into {@code shardCount} {@link EvictorShard}s, each with its own LRU list, byte accounting,
   * evictor thread, and snapshot file; keys route by {@code hash(key) & (shardCount - 1)}. Must be
   * a power of two. Production callers pass {@code Worker}'s role-aware {@code
   * CasShards.deriveShardCount} result (or the operator's {@code cas.evictorShards}); tests pass 1
   * via the overload above.
   */
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
      InputStreamFactory externalInputStreamFactory,
      long lowBytes,
      long wakeBudgetNanos,
      long idleHeartbeatNanos,
      Clock clock,
      int shardCount) {
    this.root = root;
    this.maxSizeInBytes = maxSizeInBytes;
    this.maxEntrySizeInBytes = maxEntrySizeInBytes;
    this.expireService = expireService;
    this.storage = storage;
    this.onPut = onPut;
    this.onExpire = onExpire;
    this.delegate = delegate;
    this.delegateSkipLoad = delegateSkipLoad;
    this.inputStreamFactory =
        new EmptyInputStreamFactory(
            new FailoverInputStreamFactory(this::newTransparentInput, externalInputStreamFactory));
    this.zstdBufferPool = zstdBufferPool;
    long effectiveLowBytes = lowBytes > 0 ? lowBytes : defaultLowBytes(maxSizeInBytes);
    checkShardCountCanHoldAdmissibleEntry(maxSizeInBytes, maxEntrySizeInBytes, shardCount);

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

    this.headers = new Entry[shardCount];
    for (int i = 0; i < shardCount; i++) {
      Entry shardHeader = new SentinelEntry();
      shardHeader.before = shardHeader.after = shardHeader;
      headers[i] = shardHeader;
    }

    this.casShards =
        new CasShards(
            new EvictorBackingImpl(),
            shardCount,
            expireService,
            clock,
            headers,
            maxSizeInBytes,
            effectiveLowBytes,
            wakeBudgetNanos,
            idleHeartbeatNanos,
            db,
            root);
  }

  /**
   * Implementation of the {@link CasEvictorBacking} contract this cache provides to its evictor.
   */
  private final class EvictorBackingImpl implements CasEvictorBacking {
    @Override
    public @Nullable Entry lookupEntry(String key) {
      Entry e = storage.get(key);
      // The sentinel placeholder returned by getEntry() when state is STARTING/STOPPED is
      // never published to MPSC queues, but a paranoid drain-side check costs nothing.
      return (e instanceof SentinelEntry) ? null : e;
    }

    @Override
    public @Nullable Entry safeStorageRemoval(String key) throws IOException {
      return CASFileCache.this.safeStorageRemoval(key);
    }

    @Override
    public void deleteExpiredKey(String key) throws IOException {
      CASFileCache.this.deleteExpiredKey(key);
    }

    @Override
    public void expireEntryFallback(String key, long size) {
      FileEntryKey parsed = parseFileEntryKey(key, size);
      if (parsed != null) {
        CASFileCache.this.expireEntryFallback(parsed);
      }
    }

    @Override
    public void invalidateWriteForKey(String key, long size) {
      FileEntryKey parsed = parseFileEntryKey(key, size);
      if (parsed == null) {
        return;
      }
      // Remove from writesInProgress synchronously so a post-eviction write cannot observe the
      // stale completed future. The RemovalListener dispatches only listener completion off-thread.
      invalidateWrite(parsed.digest());
    }

    @Override
    public void restoreEntryAfterRaceLoss(String key, Entry replacement) {
      CASFileCache.this.restoreEntryAfterRaceLoss(key, replacement);
    }

    @Override
    public long onDiskSize(Entry entry) {
      return estimateSizeOnDisk(entry.size, blockSize, /* isHardlink= */ false);
    }

    @Override
    public void onEntryEvicted(Entry entry) {
      // Drive any subclass-level directory cleanup (LegacyDirectoryCFC's directoriesIndex
      // drain). The returned futures are best-effort: we kick them off and don't wait;
      // they're shipped to expireService so the evictor sweep continues.
      List<ListenableFuture<Void>> futures = unlinkAndExpireDirectories(entry, expireService);
      for (ListenableFuture<Void> future : futures) {
        future.addListener(
            () -> {
              try {
                future.get();
              } catch (Exception e) {
                log.log(Level.WARNING, "directory expiration failed for " + entry.key, e);
              }
            },
            expireService);
      }
    }

    @Override
    public void onDigestFullyExpired(String key, long size) {
      FileEntryKey parsed = parseFileEntryKey(key, size);
      if (parsed == null) {
        // Unparseable key — best effort: increment the per-digest expiry counter only.
        expiredKeyCounter.inc();
        return;
      }
      // Duplicate-suppression: if the flipped-executable variant is still in storage, the
      // digest is not yet fully expired and onExpire should NOT fire.
      String otherKey = getKey(parsed.digest(), !parsed.isExecutable());
      if (storage.containsKey(otherKey)) {
        return;
      }
      expiredKeyCounter.inc();
      try {
        onExpire.accept(ImmutableList.of(parsed.digest()));
      } catch (RuntimeException e) {
        log.log(Level.WARNING, "onExpire callback threw for " + parsed.digest(), e);
      }
    }
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
      build.bazel.remote.execution.v2.Digest.@Nullable Builder result,
      Consumer<String> onContains) {
    String key = getKey(digest, isExecutable);
    Entry entry = getEntry(key);
    if (entry == null) {
      return false;
    }
    // getEntry returns a placeholder when storage misses and we're not in a running
    // state — recover by reading the on-disk size directly.
    long size = entry.size;
    if (entry.isPlaceholder()) {
      try {
        size = Files.size(getPath(key));
      } catch (IOException e) {
        return false;
      }
    }
    if (digest.getSize() < 0 || digest.getSize() == size) {
      if (result != null) {
        result.mergeFrom(DigestUtil.toDigest(digest)).setSizeBytes(size);
      }
      onContains.accept(key);
      return true;
    }
    return false;
  }

  /**
   * Forward access notifications to the evictor's lossy {@code accessEvents} queue. The evictor
   * thread is the sole writer of the LRU, so a recordAccess from any other thread is just an MPSC
   * offer. Drop-newest on full degrades recency only, never correctness.
   *
   * <p>The {@code Iterable} signature is permissive, so a mutable caller-owned collection could be
   * iterated mid-mutation. Snapshot to an immutable list at the boundary — cheap on the small lists
   * the call sites pass (a single key for {@code contains}, the found-keys list for {@code
   * findMissingBlobs}).
   */
  private void accessed(Iterable<String> keys) {
    ImmutableList<String> snapshot = ImmutableList.copyOf(keys);
    if (snapshot.isEmpty()) {
      return;
    }
    casShards.recordAccess(snapshot);
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
      build.bazel.remote.execution.v2.Digest.@Nullable Builder result,
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
      boolean missing =
          digest.getSizeBytes() != 0
              && !containsLocal(DigestUtil.fromDigest(digest, digestFunction), result, found::add);
      if (missing) {
        builder.add(digest);
        if (digest.getSizeBytes() > 0) {
          casLookups.labels("miss").inc();
        }
      } else if (digest.getSizeBytes() == -1) {
        // may misbehave with delegate
        builder.add(result.build());
      } else if (digest.getSizeBytes() > 0) {
        casLookups.labels("hit").inc();
      }
    }
    if (state.shouldRecordAccess()) {
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
    if (!state.shouldRecordAccess()) {
      return new Entry();
    }
    return null;
  }

  @SuppressWarnings({"ResultOfMethodCallIgnored", "PMD.CompareObjectsWithEquals"})
  InputStream newLocalInput(Compressor.Value compressor, Digest digest, long offset)
      throws IOException {
    // Validate compressor before taking any refcount. compressorInputStream's checkArgument
    // would otherwise throw IAE from inside the inner try, escaping the outer IOException
    // catch and leaking the refcount taken by e.tryAcquire() in the loop body.
    checkArgument(
        compressor == Compressor.Value.IDENTITY || compressor == Compressor.Value.ZSTD,
        "unsupported compressor %s",
        compressor);
    log.log(Level.FINER, format("getting input stream for %s", DigestUtil.toString(digest)));
    boolean isExecutable = false;
    do {
      String key = getKey(digest, isExecutable);
      Entry e = getEntry(key);
      // Sentinel entries bypass the refcount discipline.
      boolean haveRefcount = false;
      boolean placeholder = e != null && e.isPlaceholder();
      if (e != null && !(e instanceof SentinelEntry) && !placeholder) {
        int previous = e.tryAcquire();
        if (previous < 0) {
          e = null;
        } else {
          haveRefcount = true;
          // The acquirer does not unlink from the LRU; the evictor's sweep observes the
          // bumped refcount via tryEvict's recheck and skips the entry. This preserves the
          // evictor's single-writer ownership of the LRU list.
        }
      }
      if (e != null) {
        InputStream input = null;
        try {
          // Capture the FIS so the wrap can't leak it: if compressorInputStream's ctor
          // throws (e.g. Zstd pipe setup), Files.newInputStream's FD would dangle until GC.
          InputStream fis = Files.newInputStream(getPath(key));
          try {
            input = compressorInputStream(compressor, fis);
            // input.skip is inside the same Throwable handler as the wrap: a non-IOException
            // throw from skip (e.g. a RuntimeException from a compressor wrapper's
            // skip implementation) would otherwise escape the IOException-only outer
            // catch with refcount + stream still held, pinning the entry LIVE forever.
            input.skip(offset);
          } catch (Exception t) {
            try {
              if (input != null) {
                // close the wrapper if compressorInputStream succeeded; the wrapper
                // owns and will close the underlying fis. Null out so the outer
                // IOException catch's input-close block does not double-close.
                input.close();
                input = null;
              } else {
                fis.close();
              }
            } catch (Exception closeEx) {
              t.addSuppressed(closeEx);
            }
            // The outer catch is IOException-only; a non-IOException RuntimeException (e.g.
            // from ZstdCompressingInputStream's JNI ctor or from input.skip) would escape
            // with the refcount taken by tryAcquire still held, pinning the entry LIVE and
            // unevictable. Release before rethrow so the only invariant breach is the
            // unexpected exception itself. An Error (OutOfMemoryError, LinkageError, ...) is
            // deliberately NOT caught here: it propagates immediately without the refcount
            // release or stream close — we make no attempt to clean up after an unrecoverable
            // error, since the JVM state is undefined.
            if (!(t instanceof IOException) && haveRefcount) {
              releaseEntry(e);
              haveRefcount = false;
            }
            throw t;
          }
        } catch (IOException ioEx) {
          if (!(ioEx instanceof NoSuchFileException)) {
            readIOErrors.inc();
            log.log(
                Level.WARNING,
                format("error opening %s at %d", DigestUtil.toString(digest), offset),
                ioEx);
          }

          if (haveRefcount) {
            releaseEntry(e);
            haveRefcount = false;
          }

          if (!(e instanceof SentinelEntry) && !placeholder) {
            // Stale-on-disk recovery: file is gone but entry remains in storage. Drive the
            // entry through the same eviction state machine the evictor uses, then continue.
            // We hold no refcount here (released above), so tryEvict can succeed.
            evictStaleEntry(e);
            e = null;
          }
          // input.skip may have left a partially-read stream we no longer hold a refcount
          // on; close it before returning so the caller doesn't get bare data outside the
          // RefcountedInputStream wrapper.
          if (input != null) {
            try {
              input.close();
            } catch (IOException closeEx) {
              ioEx.addSuppressed(closeEx);
            }
            input = null;
          }
        }
        if (e != null && !(e instanceof SentinelEntry) && !placeholder) {
          accessed(ImmutableList.of(key));
        }
        if (input != null) {
          if (haveRefcount) {
            final Entry refEntry = e;
            return new RefcountedInputStream(input, () -> releaseEntry(refEntry));
          }
          return input;
        }
      }
      if (haveRefcount) {
        releaseEntry(e);
      }
      isExecutable = !isExecutable;
    } while (isExecutable);
    throw new NoSuchFileException(DigestUtil.toString(digest));
  }

  /**
   * Drive an entry whose file is unexpectedly absent through the same eviction state machine the
   * evictor uses. Used by {@link #newLocalInput} when {@code Files.newInputStream} throws {@link
   * NoSuchFileException} on an entry whose existsDeadline has expired. The caller must hold no
   * refcount on {@code e}.
   */
  private void evictStaleEntry(Entry e) {
    try {
      casShards.offerStaleEntry(e);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      log.log(Level.WARNING, "interrupted enqueueing stale entry " + e.key, ie);
    } catch (IOException ioe) {
      log.log(Level.WARNING, "failed enqueueing stale entry " + e.key, ioe);
    }
  }

  /**
   * Drop one refcount on {@code e}; on 1->0 transition, enqueue the entry into the evictor's
   * insertion queue so it lands on the LRU. The caller must hold a real reference to {@code e} —
   * looking up storage.get(key) at release time can return a different Entry inserted after the
   * caller's tryAcquire, causing underflow on the replacement or a leak on the original.
   *
   * <p>The insertion queue is unbounded because this notification is must-not-lose: dropping the
   * final release would leave a charged, unreferenced entry off the LRU.
   */
  protected void releaseEntry(Entry e) {
    if (e.release()) {
      try {
        casShards.offerInsertion(e);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        log.log(
            Level.WARNING,
            "interrupted during entry release for " + e.key + "; entry may delay landing on LRU",
            ie);
      } catch (IOException ioe) {
        log.log(
            Level.SEVERE,
            "failed to enqueue released entry for " + e.key + "; entry may leak from LRU",
            ioe);
      }
    }
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

  private void cancelWriteFuture(SettableFuture<Long> future) {
    Runnable cancel = () -> future.setException(new IOException("write cancelled"));
    try {
      expireService.execute(cancel);
    } catch (RejectedExecutionException e) {
      cancel.run();
    }
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
  public boolean isReadOnly() {
    return !state.isWritable();
  }

  public boolean setReadOnly(boolean value) {
    return state.setReadOnly(value);
  }

  @Override
  public void waitForWritable(Duration timeout) throws InterruptedException {
    synchronized (state) {
      state.wait(timeout.toMillis(), (int) (timeout.toNanos() % 1000000));
    }
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
              } else {
                Files.deleteIfExists(getWritePath());
              }
              fileCommittedSize = 0;
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

          private Path getWritePath() {
            String blobKey = getKey(key.getDigest(), false);
            return getPath(blobKey).resolveSibling(blobKey + "." + key.getIdentifier());
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
                try {
                  fileCommittedSize = Files.size(getWritePath());
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
            long committedSize = getCommittedSize();
            if (offset == 0 && committedSize == key.getDigest().getSize()) {
              if (!future.isDone()) {
                log.log(Level.WARNING, format("%s committed but has not completed future", key));
                future.set(committedSize);
              }
              throw new WriteCompleteException();
            }
            checkState(
                committedSize == offset,
                format(
                    "cannot position stream to %d, committed_size is %d", offset, committedSize));
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
    try {
      blockSize = fileStore.getBlockSize();
    } catch (UnsupportedOperationException | IOException e) {
      // blockSize retains its initial value of DEFAULT_BLOCK_SIZE
    }
  }

  @VisibleForTesting
  void setBlockSizeForTesting(long blockSize) {
    this.blockSize = blockSize;
  }

  @VisibleForTesting
  void setSizeInBytesForTesting(long sizeInBytes) {
    // Tests run at N=1, so shard 0 holds the whole cache; route the test seed there.
    casShards.shard(0).setTotalBytesForTesting(sizeInBytes);
  }

  public void stop() throws IOException, InterruptedException {
    if (prometheusMetricsThread != null) {
      prometheusMetricsThread.interrupt();
      prometheusMetricsThread.join();
    }
    // Final per-shard snapshot + evictor thread drain happens inside each EvictorShard.stop,
    // fanned out by CasShards.stop.
    casShards.stop();
    state.stop();
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
    state.start();
    CasFallbackDelegate.start(
        delegate, onStartPut, removeDirectoryService, delegateSkipLoad, writable);

    return listeningDecorator(expireService)
        .submit(
            () -> {
              // scanRoot mutates the LRU and totalBytes directly; the evictor's
              // single-LRU-writer invariant only kicks in once the evictor is running. If a
              // caller (e.g., a test fixture) eagerly started the evictor, stop it now and
              // restart after the scan. In production start() runs exactly once with the
              // evictor not yet started, and this stop() is a no-op.
              try {
                casShards.stop();
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw ie;
              }
              startRoutine(onStartPut, removeDirectoryService, skipLoad);
              synchronized (this) {
                checkState(state.setReadOnly(!writable));
              }
              casShards.start();
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
                  // 15s, not 5min: the coarse interval is useless for an A/B and hides the
                  // cache-fill dynamics this project is about.
                  Thread.sleep(15_000);
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
    List<Path> invalidDirectories;
    try {
      invalidDirectories = computeDirectories(scan);
    } finally {
      scan.fileKeys().clear();
      startupFileKeys = new ConcurrentHashMap<>();
    }
    logComputeDirectoriesResults(invalidDirectories);
    deleteInvalidFileContent(invalidDirectories, removeDirectoryService);

    // Phase 3: Migrate snapshots if the on-disk shard count changed. Runs here (after computeDirs)
    // so the rewritten current-N snapshots capture _dir entries too, and before the evictor threads
    // start so the LRU walk has no concurrent writer.
    migrateSnapshotsIfPending();

    CacheScanResults retainedScan =
        new CacheScanResults(scan.computeDirs(), scan.deleteFiles(), Map.of());
    return new CacheLoadResults(false, retainedScan, invalidDirectories);
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
    // LRU snapshot files (and their AtomicFileWriter tmp orphans) legitimately live at the root
    // branch directory. Their lifecycle is owned by loadSnapshots / migrateSnapshots — read as
    // recency hints, current-N retained across restarts, stale-N deleted only on a SUCCESSFUL
    // shard-count migration. This generic branch cleanup runs before loadSnapshots, so deleting
    // them here would (a) defeat the migration's retain-stale-on-failure contract and (b)
    // needlessly churn the current-N snapshot (read, delete, rewrite) on every restart when
    // hexBucketLevels > 0.
    if (branchDir.equals(root)
        && (LruSnapshotFiles.isSnapshot(name) || LruSnapshotFiles.isSnapshotTmpOrphan(name))) {
      return false;
    }
    return !name.matches("[0-9a-f]{2}");
  }

  /**
   * Load LRU snapshot files at the cache root as recency hints, routing each listed entry onto its
   * owning shard's list. Snapshots are recency hints plus cached size metadata. A sane cached size
   * keeps the warm-start fast path stat-free; a suspicious size is left in {@code files} so the
   * normal scan can stat and process the path with filesystem-derived size.
   *
   * <p>Files are discovered by name: current-N files ({@code lru_num_shards_<N>_shard_<I>.txt} with
   * {@code N == shardCount}), stale-N files from a prior run at a different count, the legacy
   * {@code lru.txt}, and AtomicFileWriter tmp orphans. Current-N files load first (with a strict
   * shard-ownership check), then stale-N (sorted by {@code (N, I)} for repeatable crash recovery),
   * then legacy; the {@code files} set dedupes a key listed by more than one source. Any stale-N or
   * legacy presence schedules a migration (rewrite current-N + delete stale) that runs after
   * computeDirectories via {@link #migrateSnapshotsIfPending}. All snapshot and tmp-orphan paths
   * are removed from {@code files} so the filesystem scan does not mis-parse them as CAS entries.
   */
  private void loadSnapshots(
      Consumer<Digest> onStartPut,
      Set<Path> files,
      ImmutableList.Builder<Path> computeDirs,
      ImmutableList.Builder<Path> deleteFiles,
      boolean requireFileKeysForDirectoryHardlinks) {
    int shardCount = casShards.shardCount();
    List<SnapshotFile> currentNFiles = new ArrayList<>();
    List<SnapshotFile> staleFiles = new ArrayList<>();
    Path legacyFile = null;
    List<Path> tmpOrphans = new ArrayList<>();
    Set<Path> snapshotFiles = new HashSet<>();

    try (Stream<Path> rootStream = Files.list(root)) {
      for (Path p : (Iterable<Path>) rootStream::iterator) {
        String basename = p.getFileName().toString();
        if (LruSnapshotFiles.isSnapshotTmpOrphan(basename)) {
          tmpOrphans.add(p);
          continue;
        }
        if (basename.equals(LruSnapshotFiles.LEGACY_NAME)) {
          legacyFile = p;
          snapshotFiles.add(p);
          continue;
        }
        LruSnapshotFiles.Parsed parsed = LruSnapshotFiles.parse(basename);
        if (parsed == null) {
          continue; // not a snapshot file
        }
        snapshotFiles.add(p);
        int n = parsed.shardCount();
        // Reject only num_shards_0 (a nonsensical filename). Any other N migrates: migration
        // re-hashes entries by key onto the current shards, so it handles an arbitrary stale N in
        // either direction (a shard-count increase OR decrease). The stale N is never used to size
        // or divide anything — only to classify the file and label the migration metric — and
        // parse() already bounds it to int range, so no upper cap is needed. A bound like
        // "N <= 2 * shardCount" would silently strand the snapshots of a worker that shrank its
        // shard count by more than 2x (e.g. 32 -> 4 on a smaller box), leaving the LRU cold and the
        // stale files orphaned on disk forever.
        if (n <= 0) {
          log.log(
              Level.WARNING,
              format("ignoring snapshot file with nonsensical shard count: %s", basename));
          continue;
        }
        // Classify as current-N only when both N AND the shard index are in range. A file whose
        // name claims the current N but carries an out-of-range index (corruption or a manual file
        // move) is treated as stale: its entries are re-hashed across the live shards (preserving
        // warmth) and the bogus file is cleaned up by the migration, rather than driving every key
        // through a doomed shard-ownership check that drops the hint and logs one WARNING per key.
        if (n == shardCount && parsed.index() >= 0 && parsed.index() < shardCount) {
          currentNFiles.add(new SnapshotFile(p, parsed));
        } else {
          staleFiles.add(new SnapshotFile(p, parsed));
        }
      }
    } catch (NoSuchFileException e) {
      // root not created yet — initializeRootDirectory will create it.
    } catch (IOException e) {
      log.log(Level.WARNING, "failed listing cache root for snapshot files; full scan", e);
    }

    // Keep snapshot files and tmp orphans out of the filesystem scan; queue tmp orphans for delete.
    files.removeAll(snapshotFiles);
    files.removeAll(tmpOrphans);
    deleteFiles.addAll(tmpOrphans);

    // 1. current-N files (strict shard-ownership assertion), in index order.
    currentNFiles.sort(Comparator.comparingInt(f -> f.parsed().index()));
    for (SnapshotFile file : currentNFiles) {
      loadSnapshotFile(
          onStartPut,
          file.path(),
          file.parsed().index(),
          files,
          computeDirs,
          deleteFiles,
          requireFileKeysForDirectoryHardlinks);
    }
    // 2. stale-N files, sorted by (N, I) so overlapping keys resolve the same way on every replay.
    staleFiles.sort(
        Comparator.<SnapshotFile>comparingInt(f -> f.parsed().shardCount())
            .thenComparingInt(f -> f.parsed().index()));
    for (SnapshotFile file : staleFiles) {
      loadSnapshotFile(
          onStartPut,
          file.path(),
          /* expectedIndex= */ -1,
          files,
          computeDirs,
          deleteFiles,
          requireFileKeysForDirectoryHardlinks);
    }
    // 3. legacy single-file snapshot.
    if (legacyFile != null) {
      loadSnapshotFile(
          onStartPut,
          legacyFile,
          -1,
          files,
          computeDirs,
          deleteFiles,
          requireFileKeysForDirectoryHardlinks);
    }

    // Schedule the migration rewrite when any non-current-N snapshot was present.
    if (!staleFiles.isEmpty() || legacyFile != null) {
      List<Path> toDelete = new ArrayList<>();
      Set<String> oldNLabels = new HashSet<>();
      for (SnapshotFile stale : staleFiles) {
        toDelete.add(stale.path());
        oldNLabels.add(Integer.toString(stale.parsed().shardCount()));
      }
      if (legacyFile != null) {
        toDelete.add(legacyFile);
        oldNLabels.add("legacy");
      }
      pendingMigrationStaleFiles = toDelete;
      pendingMigrationOldNLabels = oldNLabels;
    }
  }

  /** A discovered snapshot file paired with its already-parsed {@code (shardCount, index)}. */
  private record SnapshotFile(Path path, LruSnapshotFiles.Parsed parsed) {}

  /**
   * Read one snapshot file, routing each listed (and shard-owned) entry through processRootFile.
   */
  private void loadSnapshotFile(
      Consumer<Digest> onStartPut,
      Path file,
      int expectedIndex,
      Set<Path> files,
      ImmutableList.Builder<Path> computeDirs,
      ImmutableList.Builder<Path> deleteFiles,
      boolean requireFileKeysForDirectoryHardlinks) {
    try (BufferedReader br = Files.newBufferedReader(file)) {
      for (SizeEntry entry : db.entries(br)) {
        String key = entry.key();
        // Shard-ownership assertion for current-N files: a key whose hash doesn't route to this
        // file's index is misplaced (manual file move). Skip the recency hint only — the entry's
        // file, if present, is still admitted by the filesystem scan at the LRU tail.
        if (expectedIndex >= 0 && casShards.shardFor(key).index != expectedIndex) {
          log.log(
              Level.WARNING,
              format(
                  "snapshot %s lists key %s owned by another shard; skipping recency hint",
                  file.getFileName(), key));
          continue;
        }
        Path path = entryPathStrategy.getPath(key);
        if (key.endsWith("_dir")) {
          if (files.remove(path)) {
            processRootFile(onStartPut, path, entry, /* fileKey= */ null, computeDirs, deleteFiles);
          }
          continue;
        }
        if (requireFileKeysForDirectoryHardlinks) {
          if (files.remove(path)) {
            FileStatus stat = stat(path, false, fileStore);
            processRootFile(
                onStartPut,
                path,
                new SizeEntry(key, stat.getSize()),
                stat.fileKey(),
                computeDirs,
                deleteFiles);
          }
          continue;
        }
        SnapshotFastPath fastPath = snapshotFastPath(entry);
        if (fastPath == null) {
          continue;
        }
        long diskSize = fastPath.diskSize();
        if (!tryReserveStartupBytes(diskSize)) {
          continue;
        }
        if (!files.remove(path)) {
          releaseStartupBytes(diskSize);
          continue;
        }
        boolean admitted = false;
        try {
          // Stat-free fast path: no inode available, so this entry is not added to the startup
          // inode index (see startupFileKeys).
          admitFileAtStartup(onStartPut, fastPath.fileEntryKey(), diskSize, /* fileKey= */ null);
          admitted = true;
        } finally {
          if (!admitted) {
            releaseStartupBytes(diskSize);
          }
        }
      }
    } catch (NoSuchFileException e) {
      // Raced away between listing and read — nothing to load.
    } catch (Exception e) {
      // Corrupt/truncated snapshot: its unread entries fall through to the filesystem scan (the
      // admission source of truth). Warn and continue rather than aborting the whole load.
      log.log(
          Level.WARNING,
          "ignoring corrupt LRU snapshot " + file + "; its entries fall back to the scan",
          e);
    }
  }

  /**
   * A snapshot entry that qualifies for the stat-free warm-start path: its parsed key plus the
   * block-aligned on-disk size, both computed once and reused by the caller.
   */
  private record SnapshotFastPath(FileEntryKey fileEntryKey, long diskSize) {}

  /**
   * Return the parsed key and on-disk size for a snapshot entry eligible for the stat-free fast
   * path, or null when the entry is empty, oversized, unparseable, or yields an invalid disk size
   * (such entries fall through to the filesystem scan). Computing the key and size here lets the
   * caller admit the entry without re-parsing.
   */
  private @Nullable SnapshotFastPath snapshotFastPath(SizeEntry entry) {
    long size = entry.size();
    if (size <= 0 || size > maxEntrySizeInBytes) {
      return null;
    }
    FileEntryKey fileEntryKey = parseFileEntryKey(entry.key(), size);
    if (fileEntryKey == null) {
      return null;
    }
    long diskSize;
    try {
      diskSize = estimateSizeOnDisk(size, blockSize, /* isHardlink= */ false);
    } catch (IllegalArgumentException e) {
      return null;
    }
    return new SnapshotFastPath(fileEntryKey, diskSize);
  }

  /**
   * Rewrite every shard's current-N snapshot from the now-fully-populated LRU and delete the
   * stale-N / legacy files those entries were migrated from. Runs after computeDirectories (so
   * {@code _dir} entries are captured) and before the evictor threads start (so the single-writer
   * invariant is not yet in force). No-op when no migration was scheduled in {@link
   * #loadSnapshots}.
   */
  private void migrateSnapshotsIfPending() {
    if (pendingMigrationStaleFiles.isEmpty()) {
      return;
    }
    log.log(
        Level.INFO,
        format(
            "migrating %d stale LRU snapshot file(s) to num_shards=%d",
            pendingMigrationStaleFiles.size(), casShards.shardCount()));
    casShards.migrateSnapshots(pendingMigrationStaleFiles, pendingMigrationOldNLabels);
    pendingMigrationStaleFiles = List.of();
    pendingMigrationOldNLabels = Set.of();
  }

  protected boolean tryReserveStartupBytes(long diskSize) {
    checkArgument(diskSize >= 0, "diskSize (%s) must be non-negative", diskSize);
    while (true) {
      long current = startupAdmissionBytes.get();
      long next = current + diskSize;
      if (next < current || next > maxSizeInBytes) {
        return false;
      }
      if (startupAdmissionBytes.compareAndSet(current, next)) {
        return true;
      }
    }
  }

  protected void releaseStartupBytes(long diskSize) {
    if (diskSize != 0) {
      startupAdmissionBytes.addAndGet(-diskSize);
    }
  }

  private void admitFileAtStartup(
      Consumer<Digest> onStartPut,
      FileEntryKey fileEntryKey,
      long diskSize,
      @Nullable Object fileKey) {
    String key = fileEntryKey.key();
    Entry e = Entry.orphan(key, fileEntryKey.size(), Deadline.after(10, SECONDS));
    checkState(storage.put(e.key, e) == null, key);
    onStartPut.accept(fileEntryKey.digest());
    // Record the inode -> Entry mapping so computeDirectory can pin source Entries that existing
    // _dir trees hardlink (Phase 3 invariant 8). Only the stat-bearing full-scan path supplies a
    // non-null fileKey; the stat-free snapshot fast path passes null and is not indexed here.
    if (fileKey != null) {
      startupFileKeys.put(fileKey, e);
    }
    // Startup runs before the evictor threads start, so the per-shard LRU lists are not yet
    // under their single-writer contract. linkAtStartup routes the entry to its owning shard,
    // links it onto that shard's header under the shard's own lock (the parallel scan pool
    // calls in concurrently), and reserves the bytes against that shard's accounting.
    casShards.linkAtStartup(e, diskSize);
  }

  private CacheScanResults scanRoot(Consumer<Digest> onStartPut)
      throws IOException, InterruptedException {
    // create thread pool
    ExecutorService pool = BuildfarmExecutors.getScanCachePool();
    startupAdmissionBytes.set(0);
    standaloneScanComplete = false;
    startupFileKeys = new ConcurrentHashMap<>();

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

    // Single pass over the scanned files for two startup decisions:
    //  - Sweep any orphan _removed files left behind by JVM crashes between safeStorageRemoval's
    //    rename and the async Files.delete actually running. The basename ends in "_removed" — no
    //    parseFileEntryKey path ever sees this suffix.
    //  - Detect whether any CAS-directory tree (_dir) exists. If so, reconstructing its hardlink
    //    pins needs every standalone file indexed by inode, which forces loadSnapshots to stat each
    //    entry rather than trust the snapshot's recorded size.
    Set<Path> orphansToDelete = null;
    boolean requireFileKeysForDirectoryHardlinks = false;
    for (Path file : files) {
      String basename = file.getFileName().toString();
      if (basename.endsWith("_removed")) {
        if (orphansToDelete == null) {
          orphansToDelete = new HashSet<>();
        }
        orphansToDelete.add(file);
        deleteFilesBuilder.add(file);
      } else if (basename.endsWith("_dir")) {
        requireFileKeysForDirectoryHardlinks = true;
      }
    }
    if (orphansToDelete != null) {
      files.removeAll(orphansToDelete);
      log.log(
          Level.INFO,
          format(
              "scanRoot: reclaiming %d orphan _removed files left by prior crashes",
              orphansToDelete.size()));
    }

    loadSnapshots(
        onStartPut,
        files,
        computeDirsBuilder,
        deleteFilesBuilder,
        requireFileKeysForDirectoryHardlinks);

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
                  stat.fileKey(),
                  computeDirsBuilder,
                  deleteFilesBuilder);
            } catch (Exception e) {
              log.log(Level.SEVERE, "error reading file " + file.toString(), e);
            }
          });
    }

    joinThreads(pool, "Scanning Cache Root...");

    // Every standalone Entry is now in storage and indexed by inode (full-scan path).
    // computeDirectory
    // may now reconstruct CAS-directory-hardlink pins (Phase 3 invariant 8).
    standaloneScanComplete = true;

    // log information from scanning cache root.
    return new CacheScanResults(
        computeDirsBuilder.build(), deleteFilesBuilder.build(), startupFileKeys);
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  private void processRootFile(
      Consumer<Digest> onStartPut,
      Path path,
      SizeEntry entry,
      @Nullable Object fileKey,
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
      if (size > maxEntrySizeInBytes || size <= 0) {
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
          long diskSize = estimateSizeOnDisk(size, blockSize, /* isHardlink= */ false);
          if (!tryReserveStartupBytes(diskSize)) {
            synchronized (deleteFiles) {
              deleteFiles.add(path);
            }
          } else {
            boolean admitted = false;
            try {
              admitFileAtStartup(onStartPut, fileEntryKey, diskSize, fileKey);
              admitted = true;
            } finally {
              if (!admitted) {
                releaseStartupBytes(diskSize);
              }
            }
          }
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

  public void decrementReference(String inputFile) throws IOException {
    decrementInputReferences(ImmutableList.of(inputFile));
  }

  public abstract void decrementReferences(
      Iterable<String> inputFiles,
      Iterable<build.bazel.remote.execution.v2.Digest> inputDirectories,
      DigestFunction.Value digestFunction)
      throws IOException, InterruptedException;

  protected int decrementInputReferences(Iterable<String> inputFiles) {
    int linked = 0;
    for (String input : inputFiles) {
      checkNotNull(input);
      Entry e = storage.get(input);
      if (e == null) {
        throw new IllegalStateException(input + " has been removed with references");
      }
      if (!e.key.equals(input)) {
        throw new RuntimeException("ERROR: entry retrieved: " + e.key + " != " + input);
      }
      if (e.release()) {
        try {
          casShards.offerInsertion(e);
          linked++;
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          log.log(Level.WARNING, "interrupted during decrementInputReferences of " + e.key, ie);
        } catch (IOException ioe) {
          log.log(
              Level.SEVERE,
              "evictor backpressure during decrementInputReferences of " + e.key,
              ioe);
        }
      }
    }
    return linked;
  }

  public Path getPath(String filename) {
    return entryPathStrategy.getPath(filename);
  }

  protected Path getRemovingPath(String filename) {
    return entryPathStrategy.getPath(filename + "_removed");
  }

  /**
   * Discharge a producer reservation that did not progress to a committed entry — e.g. a cancelled
   * write, a digest mismatch, an open-output error. Routes the byte refund through the evictor's
   * totalBytes accounting.
   */
  private void dischargeReservation(String key, long size) {
    long diskSize = estimateSizeOnDisk(size, blockSize, /* isHardlink= */ false);
    // Route the refund to the shard the matching charge(key, ...) reserved against.
    casShards.discharge(key, diskSize);
  }

  protected String getDirectoryKey(Digest digest) {
    return digestFilename(digest) + "_dir";
  }

  @VisibleForTesting
  public Path getDirectoryPath(Digest digest) {
    return getPath(getDirectoryKey(digest));
  }

  /**
   * Hook for subclasses to wire directory-level cleanup into eviction. The contract is now "called
   * by the evictor on a successfully-evicted Entry"; the historical {@code ListenableFuture<Void>}
   * return type is preserved so subclass overrides need no signature change. The Evictor's evictOne
   * calls this synchronously on the evictor thread between safeStorageRemoval and the async {@code
   * deleteExpiredKey} dispatch; an override that needs off-thread work should return a future that
   * completes asynchronously.
   *
   * <p>Default implementation: no extra cleanup beyond the standard delete; returns one
   * already-complete future.
   */
  protected List<ListenableFuture<Void>> unlinkAndExpireDirectories(
      Entry entry, ExecutorService service) {
    return ImmutableList.of(immediateFuture(null));
  }

  // clears the interrupted status
  private static boolean causedByInterrupted(Exception e) {
    return Thread.interrupted()
        || e.getCause() instanceof InterruptedException
        || e instanceof ClosedByInterruptException;
  }

  /**
   * Race-loss recovery: a {@code safeStorageRemoval} on this {@code key} returned a different
   * {@link Entry} than the caller expected (a concurrent writer landed an Entry between the
   * caller's tryEvict and the removal). Restore it under the same per-key lock that {@code
   * safeStorageInsertion} acquires so this is atomic with concurrent insertion. If yet-another
   * writer has already published a newer Entry by the time we get the lock, the replacement is
   * orphaned and we discharge its bytes to avoid a slow totalBytes leak.
   */
  private void restoreEntryAfterRaceLoss(String key, Entry replacement) {
    Lock lock;
    try {
      lock = keyLocks.get(key);
    } catch (ExecutionException e) {
      throw new RuntimeException(e.getCause());
    }
    boolean restored;
    lock.lock();
    try {
      Entry existing = storage.putIfAbsent(key, replacement);
      restored = existing == null;
      if (!restored) {
        log.log(
            Level.SEVERE,
            "race-loss restore for "
                + key
                + " collided with newer entry; discharging orphan bytes");
        casShards.discharge(
            key, estimateSizeOnDisk(replacement.size, blockSize, /* isHardlink= */ false));
      }
    } finally {
      lock.unlock();
    }
    // If the replacement has no holder (refCount==0) and isn't already on the LRU, enqueue it
    // so the evictor's sweep can find it. Without this, the bytes stay in totalBytes but the
    // entry isn't reachable from a sweep — only a subsequent referenceIfExists -> release
    // cycle would re-enqueue it. The drain-side guard in onInsertion rejects already-linked
    // or non-LIVE entries, so this is safe to call unconditionally for refCount==0.
    if (restored && replacement.refCount() == 0) {
      try {
        casShards.offerInsertion(replacement);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        log.log(
            Level.WARNING,
            "interrupted re-enqueueing race-loss replacement for "
                + key
                + "; entry may delay landing on LRU",
            ie);
      } catch (IOException ioe) {
        log.log(
            Level.SEVERE,
            "failed to re-enqueue race-loss replacement for " + key + "; entry may leak from LRU",
            ioe);
      }
    }
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

    Entry entry = null;
    boolean detached = false;
    lock.lock();
    try {
      if (key.endsWith("_dir")) {
        fileMover.move(path, expiredPath, ATOMIC_MOVE, REPLACE_EXISTING);
      } else {
        try {
          Files.createLink(expiredPath, path);
        } catch (FileAlreadyExistsException e) {
          Files.delete(expiredPath);
          Files.createLink(expiredPath, path);
        }
        deleteExpiredPath = true;
        Files.delete(path);
        deleteExpiredPath = false;
      }
      detached = true;
    } catch (NoSuchFileException e) {
      // ignore, already removed
      detached = true;
    } finally {
      if (detached) {
        entry = storage.remove(key);
      }
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

  interface FileContent {
    void call(Path filePath, Path cacheFilePath, Digest digest, long size, boolean isExecutable)
        throws Exception;
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
                        filePath, cacheFilePath.path(), digest, digest.getSize(), isExecutable);
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

  /**
   * Estimates the physical on-disk size of a file by rounding up to the nearest filesystem block
   * boundary.
   *
   * <p>For many files and filesystems this will be accurate. However, for some files on some
   * filesystems this will overestimate the physical size by up to blockSize bytes. To clarify, this
   * likely overestimates for smaller files on filesystems that use inlining, block suballocation,
   * variable block sizes, etc. It can also overestimate for compressed filesystems proportional to
   * the compression ratio.
   *
   * <p>When {@code isHardlink} is {@code true}, the caller is creating (or has verified) a hardlink
   * to an already-resident inode rather than writing a fresh inode; the physical cost is then just
   * a directory entry (~64 bytes on typical filesystems, rounding to 0 under block alignment), so
   * this method returns 0. Callers must only pass {@code true} when a hardlink to an
   * already-accounted inode is being made.
   */
  @VisibleForTesting
  static long estimateSizeOnDisk(long logicalSize, long blockSize, boolean isHardlink) {
    checkArgument(blockSize > 0, "blockSize (%s) must be positive", blockSize);
    checkArgument(logicalSize >= 0, "logicalSize (%s) must be non-negative", logicalSize);
    if (isHardlink) {
      // Hardlinks reuse an existing inode's blocks and add only a directory entry (~64 bytes on
      // typical filesystems, which rounds to 0 under block alignment).
      return 0;
    }
    if (logicalSize == 0) {
      return 0;
    }
    // Use long division to get the ceiling in order to round up to the next largest blocksize.
    // This should return correct answers for sizes that are block aligned as well as those that
    // are not.
    return ((logicalSize + blockSize - 1) / blockSize) * blockSize;
  }

  /**
   * Estimates the on-disk size of a directory's entry table based on its contents. On typical Linux
   * filesystems (ext4, XFS), each directory entry is approximately 8 bytes of header plus the
   * filename length, aligned to 4 bytes. Each directory occupies at least one filesystem block.
   */
  @VisibleForTesting
  static long estimateDirectorySizeOnDisk(Directory directory, long blockSize) {
    checkArgument(blockSize > 0, "blockSize (%s) must be positive", blockSize);
    long entryCount =
        directory.getFilesCount() + directory.getSymlinksCount() + directory.getDirectoriesCount();
    // ~32 bytes per entry is a reasonable average for typical filename lengths
    // on ext4/XFS (8-byte header + ~20-char name + alignment padding)
    long estimatedBytes = entryCount * ESTIMATED_BYTES_PER_DIRECTORY_ENTRY;
    long blocks = Math.max(1, Math.ceilDiv(estimatedBytes, blockSize));
    return blocks * blockSize;
  }

  protected long fetchDirectory(
      Path rootPath,
      Digest digest,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      FileContent onFileContent,
      ImmutableList.Builder<ListenableFuture<Path>> putFutures,
      ExecutorService service)
      throws IOException, InterruptedException {
    long directoryOverhead = 0;
    Stack<Map.Entry<Path, Directory>> stack = new Stack<>();
    stack.push(
        new AbstractMap.SimpleEntry<>(
            rootPath, getDirectoryFromDigest(directoriesIndex, rootPath, digest)));
    while (!stack.isEmpty()) {
      Map.Entry<Path, Directory> pathDirectoryPair = stack.pop();
      Path path = pathDirectoryPair.getKey();
      Directory directory = pathDirectoryPair.getValue();

      validateDirectoryEntryNames(path, directory);
      removeFilePath(path);
      Files.createDirectory(path);
      directoryOverhead += estimateDirectorySizeOnDisk(directory, blockSize);
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
    return directoryOverhead;
  }

  private static void validateDirectoryEntryNames(Path path, Directory directory)
      throws IOException {
    Set<String> names = new HashSet<>();
    for (FileNode fileNode : directory.getFilesList()) {
      checkUniqueDirectoryEntryName(path, names, fileNode.getName());
    }
    for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
      checkUniqueDirectoryEntryName(path, names, directoryNode.getName());
    }
    for (SymlinkNode symlinkNode : directory.getSymlinksList()) {
      checkUniqueDirectoryEntryName(path, names, symlinkNode.getName());
    }
  }

  private static void checkUniqueDirectoryEntryName(Path path, Set<String> names, String name)
      throws IOException {
    if (!names.add(name)) {
      throw new IOException(format("duplicate directory entry %s under %s", name, path));
    }
  }

  protected void removeFilePath(Path path) throws IOException {
    if (Files.exists(path)) {
      if (Files.isDirectory(path)) {
        log.log(Level.FINER, "removing existing directory " + path + " for fetch");
        removeDirectoryForFetch(path);
      } else {
        Files.delete(path);
      }
    }
  }

  protected void removeDirectoryForFetch(Path path) throws IOException {
    Directories.remove(path, fileStore);
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

  @SuppressWarnings("PMD.CompareObjectsWithEquals")
  protected boolean referenceIfExists(String key) throws IOException {
    Entry e = storage.get(key);
    if (e == null) {
      return false;
    }

    if (!entryExists(e)) {
      // Stale-on-disk recovery runs on the evictor thread so LRU unlink and accounting remain
      // single-writer. The caller holds no refcount here; if a concurrent acquire wins before the
      // evictor drains the event, tryEvict's refcount re-check rolls back and a later stale read
      // will re-enqueue it.
      evictStaleEntry(e);
      return false;
    }

    int previous = e.tryAcquire();
    if (previous < 0) {
      return false;
    }
    // Do not unlink from the LRU on 0->1; the evictor's sweep observes the bumped refCount
    // via tryEvict's recheck and skips the entry. The evictor owns LRU mutation.
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
        dischargeReservation(key, blobSizeInBytes);
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
    Histogram.Timer chargeTimer = casChargeSeconds.startTimer();
    try {
      if (referenceIfExists(key)) {
        return false;
      }
      long diskSize = estimateSizeOnDisk(blobSizeInBytes, blockSize, /* isHardlink= */ false);
      casShards.charge(key, diskSize);
      requiresDischarge.set(true);
      return true;
    } finally {
      chargeTimer.observeDuration();
    }
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
          dischargeReservation(key, blobSizeInBytes);
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
            dischargeReservation(key, blobSizeInBytes);
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
          dischargeReservation(key, blobSizeInBytes);
          throw new DigestMismatchException(actual, expectedDigest);
        }
        try {
          setReadOnlyPerms(writePath, isExecutable, fileStore);
        } catch (IOException e) {
          dischargeReservation(key, blobSizeInBytes);
          throw e;
        }

        Entry entry = new Entry(key, blobSizeInBytes, Deadline.after(10, SECONDS));
        Path canonicalPath = CASFileCache.this.getPath(key);

        Entry existingEntry = null;
        boolean inserted = false;
        boolean storageOwnsReservation = false;
        try {
          try {
            // createLink runs before the per-key lock acquired in safeStorageInsertion: a
            // concurrent evictor mid-safeStorageRemoval for the same key still holds the canonical
            // file, so createLink throws FileAlreadyExistsException and we fall through to the
            // "look up the existing entry" polling branch below.
            Files.createLink(canonicalPath, writePath);
            existingEntry = safeStorageInsertion(key, entry);
            inserted = existingEntry == null;
            storageOwnsReservation = inserted;
          } catch (FileAlreadyExistsException e) {
            log.log(Level.FINER, "file already exists for " + key);
          } finally {
            Files.delete(writePath);
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
              verifyCanonicalFileMatches(canonicalPath);
              existingEntry = safeStorageInsertion(key, entry);
              inserted = existingEntry == null;
              storageOwnsReservation = inserted;
              if (inserted) {
                log.log(Level.FINER, "adopted existing canonical file for " + key);
              }
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
        } finally {
          if (!storageOwnsReservation) {
            dischargeReservation(key, blobSizeInBytes);
          }
        }
      }

      private void verifyCanonicalFileMatches(Path canonicalPath) throws IOException {
        long existingSize = Files.size(canonicalPath);
        if (existingSize != blobSizeInBytes) {
          throw new IOException(
              format(
                  "existing file size for %s was %d, expected %d",
                  key, existingSize, blobSizeInBytes));
        }

        HashingOutputStream existingHashOut = digestUtil.newHashingOutputStream(nullOutputStream());
        try (InputStream in = Files.newInputStream(canonicalPath)) {
          ByteStreams.copy(in, existingHashOut);
        }

        Digest existingDigest = digestUtil.build(existingHashOut.hash().toString(), existingSize);
        if (!getKey(existingDigest, isExecutable).equals(key)) {
          throw new DigestMismatchException(existingDigest, expectedDigest);
        }
      }
    };
  }

  @VisibleForTesting
  public static class Entry {
    /**
     * Lifecycle: LIVE -> EVICTING -> EVICTED with the one legal rollback EVICTING -> LIVE (when an
     * evictor's refcount-recheck observes a resurrecting acquire).
     */
    public enum State {
      LIVE,
      EVICTING,
      EVICTED
    }

    private static final VarHandle REFCOUNT;
    private static final VarHandle STATE;
    private static final VarHandle CAS_DIRECTORY_HARDLINK_COUNT;

    static {
      try {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        REFCOUNT = lookup.findVarHandle(Entry.class, "refCount", int.class);
        STATE = lookup.findVarHandle(Entry.class, "state", State.class);
        CAS_DIRECTORY_HARDLINK_COUNT =
            lookup.findVarHandle(Entry.class, "casDirectoryHardlinkCount", int.class);
      } catch (ReflectiveOperationException e) {
        throw new ExceptionInInitializerError(e);
      }
    }

    Entry before;
    Entry after;
    final String key;
    final long size;

    @SuppressWarnings("unused")
    volatile int refCount;

    @SuppressWarnings("unused")
    volatile State state;

    // Counts hardlinks held by CAS-internal directory tree construction — specifically, hardlinks
    // created by DirectoryEntryCFC.linkAndReference. Other hardlinks to this Entry's inode (e.g.,
    // exec-root hardlinks created by CFCLinkExecFileSystem.put) are tracked separately via
    // referenceCount, because their cleanup path has the key in hand and decrements via
    // storage.get(key). This counter exists specifically because the directory tree walker
    // (during parent-dir eviction) does NOT have the source key — only the file's inode — and
    // must look up the source Entry via the CasInodeIndex. Keeping this counter separate from
    // referenceCount lets us clean the index eagerly (on the 1->0 transition); a single combined
    // counter would force lazy cleanup at full eviction and bloat the map at scale. Mutations go
    // through CasInodeIndex (which pairs the atomic update with the inode-keyed map bookkeeping),
    // never directly.
    @SuppressWarnings("unused")
    volatile int casDirectoryHardlinkCount;

    volatile Deadline existsDeadline;

    private Entry(String key, long size, Deadline existsDeadline, int initialRefCount) {
      this.key = key;
      this.size = size;
      this.refCount = initialRefCount;
      this.state = State.LIVE;
      this.existsDeadline = existsDeadline;
    }

    private Entry() {
      this(null, -1, null, -1);
    }

    /**
     * Runtime constructor: caller holds the initial reference. Use {@link #orphan} for startup-scan
     * entries that are not yet referenced.
     */
    public Entry(String key, long size, Deadline existsDeadline) {
      this(key, size, existsDeadline, 1);
    }

    /**
     * Produces an entry in the unreferenced state with {@code before/after = null} so {@link
     * #isLinked()} returns false until the caller publishes it via the evictor. Self-linking here
     * would race the evictor's drain-side guard and corrupt {@code unreferencedCount}.
     */
    public static Entry orphan(String key, long size, Deadline existsDeadline) {
      return new Entry(key, size, existsDeadline, 0);
    }

    public int refCount() {
      return (int) REFCOUNT.getVolatile(this);
    }

    public State state() {
      return (State) STATE.getVolatile(this);
    }

    /**
     * Volatile read of the CAS-directory-hardlink count. {@code > 0} pins the Entry against
     * eviction — the evictor's sweep skips such entries (see {@link EvictorShard}). Read by both
     * the sweep and {@link CasInodeIndex}'s re-read-inside-compute pattern.
     */
    public int casDirectoryHardlinkCount() {
      return (int) CAS_DIRECTORY_HARDLINK_COUNT.getVolatile(this);
    }

    /**
     * Atomic {@code getAndAdd} on the CAS-directory-hardlink count, returning the previous value.
     * Package-private and intended for use only by {@link CasInodeIndex}, which pairs the atomic
     * update with the inode-keyed map bookkeeping. The VarHandle stays encapsulated here, next to
     * the field it guards; the indexing strategy stays in {@code CasInodeIndex}.
     */
    int getAndAddCasDirectoryHardlinkCount(int delta) {
      return (int) CAS_DIRECTORY_HARDLINK_COUNT.getAndAdd(this, delta);
    }

    /**
     * True for the no-arg-constructed dummy returned by {@link CASFileCache#getEntry} when the
     * cache is not running (STARTING / STOPPED) and the digest is not in {@code storage}. Real
     * entries never carry a negative refCount — {@link #release} throws on underflow rather than
     * publishing a transient negative.
     */
    public boolean isPlaceholder() {
      return refCount() < 0;
    }

    @VisibleForTesting
    public boolean isEvictable() {
      return state() == State.LIVE && refCount() == 0 && casDirectoryHardlinkCount() == 0;
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

    /**
     * Atomically increments refCount when the entry is LIVE.
     *
     * @return the previous refcount on success; -1 when the entry is no longer LIVE. The evictor's
     *     sweep observes refCount via tryEvict and skips entries with refCount > 0, so the acquirer
     *     does not need to proactively unlink.
     */
    public int tryAcquire() {
      if (((State) STATE.getVolatile(this)) != State.LIVE) {
        return -1;
      }
      int previous = (int) REFCOUNT.getAndAdd(this, 1);
      if (((State) STATE.getVolatile(this)) != State.LIVE) {
        REFCOUNT.getAndAdd(this, -1);
        return -1;
      }
      return previous;
    }

    /**
     * Atomically decrements refCount; throws IllegalStateException on underflow rather than
     * publishing a transient negative.
     *
     * @return true on the final 1 -> 0 transition (caller must enqueue the entry through {@code
     *     evictor.offerInsertion} so it lands on the LRU).
     */
    public boolean release() {
      for (; ; ) {
        int current = (int) REFCOUNT.getVolatile(this);
        if (current <= 0) {
          throw new IllegalStateException("entry " + key + " release with refCount=" + current);
        }
        if (REFCOUNT.compareAndSet(this, current, current - 1)) {
          return current == 1;
        }
      }
    }

    /**
     * CAS state LIVE -> EVICTING, then re-check both refCount and casDirectoryHardlinkCount under
     * the published EVICTING state. Returns true if the caller now owns the eviction; false
     * otherwise (entry resurrected via a concurrent acquire, or still pinned by a CAS-directory
     * hardlink — the state is rolled back to LIVE in either case).
     */
    public boolean tryEvict() {
      if (!STATE.compareAndSet(this, State.LIVE, State.EVICTING)) {
        return false;
      }
      if ((int) REFCOUNT.getVolatile(this) != 0) {
        // CAS rather than setVolatile so a future EVICTING -> X transition added elsewhere
        // surfaces as a failed checkState rather than silently overwriting state.
        // checkState (not assert) so the invariant holds in production where -ea is off.
        checkState(
            STATE.compareAndSet(this, State.EVICTING, State.LIVE),
            "tryEvict rollback observed unexpected state for %s",
            key);
        return false;
      }
      // A CAS-directory tree still hardlinks this source's inode: evicting it would delete the
      // standalone file and discharge its bytes while the directory hardlink keeps the inode's
      // blocks on disk (an accounting leak) and strands a stale CasInodeIndex mapping. The EVICTING
      // state we just published is the serialization point: a concurrent linkAndReference either
      // still holds src's refCount (caught above) or has already driven the count 0 -> 1 (caught
      // here), and any acquire arriving after the CAS fails in tryAcquire. The sweep's pre-skip on
      // casDirectoryHardlinkCount() > 0 is an optimization; this recheck is authoritative.
      if (casDirectoryHardlinkCount() != 0) {
        checkState(
            STATE.compareAndSet(this, State.EVICTING, State.LIVE),
            "tryEvict rollback observed unexpected state for %s",
            key);
        return false;
      }
      return true;
    }

    /** Retryable transition EVICTING -> LIVE after eviction failed before detaching storage. */
    public void abortEviction() {
      checkState(
          STATE.compareAndSet(this, State.EVICTING, State.LIVE),
          "abortEviction observed unexpected state for %s",
          key);
    }

    /** Terminal transition EVICTING -> EVICTED after the file delete has succeeded. */
    public void completeEviction() {
      // Mirror tryEvict's rollback CAS: a caller that invokes completeEviction without
      // first owning EVICTING (e.g., on a LIVE entry) surfaces a checkState failure
      // rather than silently turning a LIVE entry into EVICTED. checkState (not assert)
      // so the invariant holds in production where -ea is off.
      checkState(
          STATE.compareAndSet(this, State.EVICTING, State.EVICTED),
          "completeEviction observed unexpected state for %s",
          key);
    }
  }

  private static final class SentinelEntry extends Entry {
    @Override
    public void unlink() {
      throw new UnsupportedOperationException("sentinel cannot be unlinked");
    }

    @Override
    protected void addBefore(Entry existingEntry) {
      throw new UnsupportedOperationException("sentinel cannot be added");
    }

    @Override
    public int tryAcquire() {
      throw new UnsupportedOperationException("sentinel cannot be referenced");
    }

    @Override
    public boolean release() {
      throw new UnsupportedOperationException("sentinel cannot be referenced");
    }

    @Override
    public boolean tryEvict() {
      throw new UnsupportedOperationException("sentinel cannot be evicted");
    }

    @Override
    public void completeEviction() {
      throw new UnsupportedOperationException("sentinel cannot be evicted");
    }

    @Override
    public boolean isEvictable() {
      return false;
    }

    @Override
    public boolean isPlaceholder() {
      // SentinelEntry inherits the no-arg Entry() constructor's refCount=-1, which would
      // otherwise satisfy the parent predicate. The header sentinel is the LRU anchor, not
      // the STARTING/STOPPED dummy returned by getEntry(); a future change that exposes
      // the header through getEntry() must not have it pass isPlaceholder().
      return false;
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

  private void expireEntryFallback(FileEntryKey fileEntryKey) {
    if (delegate != null) {
      Write write;
      try {
        write =
            delegate.getWrite(
                Compressor.Value.IDENTITY,
                fileEntryKey.digest(),
                UUID.randomUUID(),
                RequestMetadata.getDefaultInstance());
      } catch (EntryLimitException e) {
        log.log(
            Level.SEVERE,
            format("error opening delegate write for expired entry %s", fileEntryKey.key()),
            e);
        return;
      }
      if (write != null) {
        performCopy(write, fileEntryKey.digest(), fileEntryKey.key());
      }
    }
  }

  /**
   * Kicks off an asynchronous copy. Does not wait for it to complete. That way we don't do IO while
   * holding the lock.
   *
   * <p>Async upload failures (arriving on the gRPC SerializingExecutor after this thread has
   * returned) do NOT propagate interrupts back to the eviction thread. The eviction loop has
   * already moved on. This should be ok because interruption during the async phase is meaningful
   * only at graceful shutdown where higher-level shutdown logic terminates the worker
   * independently.
   *
   * <p>Synchronous setup failures DO restore the interrupt flag. The eviction loop is still on this
   * thread and its end-of-iteration {@code isInterrupted()} check needs to see the flag to
   * propagate {@link InterruptedException} up through {@code charge} to the {@code put} caller.
   */
  private void performCopy(Write write, Digest digest, String key) {
    // We need to synchronously open the InputStream for streamIntoWriteFuture because
    // safeStorageRemoval unlinks the file right after this function returns.
    ListenableFuture<Long> future =
        WritesHelper.streamIntoWriteFuture(
            () -> Files.newInputStream(getPath(key)), write, digest, 1, MINUTES);
    if (future.isDone()) {
      try {
        future.get();
      } catch (ExecutionException e) {
        if (e.getCause() instanceof IOException
            && causedByInterrupted((IOException) e.getCause())) {
          Thread.currentThread().interrupt();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    Futures.addCallback(
        future,
        new FutureCallback<Long>() {
          @Override
          public void onSuccess(Long committedSize) {}

          @Override
          public void onFailure(Throwable t) {
            log.log(Level.SEVERE, format("error delegating expired entry %s", key), t);
          }
        },
        directExecutor());
  }
}
