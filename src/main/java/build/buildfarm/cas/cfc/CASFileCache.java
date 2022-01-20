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

package build.buildfarm.cas.cfc;

import static build.buildfarm.common.io.Directories.disableAllWriteAccess;
import static build.buildfarm.common.io.EvenMoreFiles.isReadOnlyExecutable;
import static build.buildfarm.common.io.EvenMoreFiles.setReadOnlyPerms;
import static build.buildfarm.common.io.Utils.getFileKey;
import static build.buildfarm.common.io.Utils.getOrIOException;
import static build.buildfarm.common.io.Utils.listDir;
import static build.buildfarm.common.io.Utils.listDirentSorted;
import static build.buildfarm.common.io.Utils.stat;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
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
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import build.bazel.remote.execution.v2.BatchReadBlobsResponse.Response;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.SymlinkNode;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.DigestMismatchException;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.EntryLimitException;
import build.buildfarm.common.Write;
import build.buildfarm.common.Write.CompleteWrite;
import build.buildfarm.common.io.Directories;
import build.buildfarm.common.io.FeedbackOutputStream;
import build.buildfarm.common.io.FileStatus;
import build.buildfarm.common.io.NamedFileKey;
import build.buildfarm.v1test.BlobWriteKey;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.hash.HashingOutputStream;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.protobuf.ByteString;
import io.grpc.Deadline;
import io.grpc.stub.ServerCallStreamObserver;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import org.json.simple.JSONObject;

public abstract class CASFileCache implements ContentAddressableStorage {
  private static final Logger logger = Logger.getLogger(CASFileCache.class.getName());
  // Prometheus metrics
  private static final Counter expiredKeyCounter =
      Counter.build().name("expired_key").help("Number of key expirations.").register();
  private static final Gauge casSizeMetric =
      Gauge.build().name("cas_size").help("CAS size.").register();
  private static final Gauge casEntryCountMetric =
      Gauge.build().name("cas_entry_count").help("Number of entries in the CAS.").register();

  protected static final String DEFAULT_DIRECTORIES_INDEX_NAME = "directories.sqlite";
  protected static final String DIRECTORIES_INDEX_NAME_MEMORY = ":memory:";

  private final Path root;
  private final EntryPathStrategy entryPathStrategy;
  private final long maxSizeInBytes;
  private final long maxEntrySizeInBytes;
  private final DigestUtil digestUtil;
  private final ConcurrentMap<String, Entry> storage;
  private final Consumer<Digest> onPut;
  private final Consumer<Iterable<Digest>> onExpire;
  private final Executor accessRecorder;
  private final ExecutorService expireService;
  private Thread prometheusMetricsThread;

  private final Map<Digest, DirectoryEntry> directoryStorage = Maps.newConcurrentMap();
  private final DirectoriesIndex directoriesIndex;
  private final String directoriesIndexDbName;
  private final LockMap locks = new LockMap();
  @Nullable private final ContentAddressableStorage delegate;
  private final LoadingCache<BlobWriteKey, Write> writes =
      CacheBuilder.newBuilder()
          .expireAfterAccess(1, HOURS)
          .removalListener(
              (RemovalListener<BlobWriteKey, Write>)
                  notification -> notification.getValue().reset())
          .build(
              new CacheLoader<BlobWriteKey, Write>() {
                @SuppressWarnings("NullableProblems")
                @Override
                public Write load(BlobWriteKey key) {
                  return newWrite(key, CASFileCache.this.getFuture(key.getDigest()));
                }
              });
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
              new CacheLoader<Digest, SettableFuture<Long>>() {
                @SuppressWarnings("NullableProblems")
                @Override
                public SettableFuture<Long> load(Digest digest) {
                  SettableFuture<Long> future = SettableFuture.create();
                  if (containsLocal(digest, /* result=*/ null, (key) -> {})) {
                    future.set(digest.getSizeBytes());
                  }
                  return future;
                }
              });

  private FileStore fileStore; // bound to root
  private transient long sizeInBytes = 0;
  private final transient Entry header = new SentinelEntry();
  private volatile long unreferencedEntryCount = 0;

  @GuardedBy("this")
  private long removedEntrySize = 0;

  @GuardedBy("this")
  private int removedEntryCount = 0;

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
    return directoryStorage.size();
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

  public static class CacheScanResults {
    public List<Path> computeDirs = Collections.emptyList();
    public List<Path> deleteFiles = Collections.emptyList();
    public Map<Object, Entry> fileKeys = Collections.emptyMap();
  }

  public static class CacheLoadResults {
    public boolean loadSkipped;
    public CacheScanResults scan = new CacheScanResults();
    public List<Path> invalidDirectories = Collections.emptyList();
  }

  public static class StartupCacheResults {
    public Path cacheDirectory;
    public CacheLoadResults load;
    public Duration startupTime;
  }

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
      boolean storeFileDirsIndexInMemory,
      DigestUtil digestUtil,
      ExecutorService expireService,
      Executor accessRecorder) {
    this(
        root,
        maxSizeInBytes,
        maxEntrySizeInBytes,
        hexBucketLevels,
        storeFileDirsIndexInMemory,
        digestUtil,
        expireService,
        accessRecorder,
        /* storage=*/ Maps.newConcurrentMap(),
        /* directoriesIndexDbName=*/ DEFAULT_DIRECTORIES_INDEX_NAME,
        /* onPut=*/ (digest) -> {},
        /* onExpire=*/ (digests) -> {},
        /* delegate=*/ null);
  }

  public CASFileCache(
      Path root,
      long maxSizeInBytes,
      long maxEntrySizeInBytes,
      int hexBucketLevels,
      boolean storeFileDirsIndexInMemory,
      DigestUtil digestUtil,
      ExecutorService expireService,
      Executor accessRecorder,
      ConcurrentMap<String, Entry> storage,
      String directoriesIndexDbName,
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
    this.directoriesIndexDbName = directoriesIndexDbName;

    entryPathStrategy = new HexBucketEntryPathStrategy(root, hexBucketLevels);

    String directoriesIndexUrl = "jdbc:sqlite:";
    if (directoriesIndexDbName.equals(DIRECTORIES_INDEX_NAME_MEMORY)) {
      directoriesIndexUrl += directoriesIndexDbName;
    } else {
      // db is ephemeral for now, no reuse occurs to match it, computation
      // occurs each time anyway, and expected use of put is noop on collision
      Path path = root.resolve(directoriesIndexDbName);
      try {
        if (Files.exists(path)) {
          Files.delete(path);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      directoriesIndexUrl += path.toString();
    }
    this.directoriesIndex =
        storeFileDirsIndexInMemory
            ? new MemoryFileDirectoriesIndex(entryPathStrategy)
            : new SqliteFileDirectoriesIndex(directoriesIndexUrl, entryPathStrategy);
    header.before = header.after = header;
  }

  private static Digest keyToDigest(String key, long size, DigestUtil digestUtil)
      throws NumberFormatException {
    String[] components = key.split("_");

    String hashComponent = components[0];

    return digestUtil.build(hashComponent, size);
  }

  /**
   * Parses the given fileName and invokes the onKey method if successful
   *
   * <p>if size > 0, consider the filename invalid if it does not match
   */
  private FileEntryKey parseFileEntryKey(String fileName, long size, DigestUtil digestUtil) {
    String[] components = fileName.split("_");
    if (components.length > 3) {
      return null;
    }

    boolean isExecutable;
    boolean hasSizeComponent;
    Digest digest;
    try {
      // Can be legacy: <hash>_<size>[_exec]
      // Or new: <hash>[_exec]
      // Streamline when legacy is removed for #677
      String hashComponent = components[0];
      digest = digestUtil.build(hashComponent, size);
      isExecutable = components[components.length - 1].equals("exec");
      // must be executable for 3 fields
      if (!isExecutable && components.length > 2) {
        return null;
      }
      hasSizeComponent = components.length == (isExecutable ? 3 : 2);
      long parsedSizeComponent = hasSizeComponent ? Long.parseLong(components[1]) : size;
      if (size != parsedSizeComponent) {
        return null;
      }
    } catch (NumberFormatException e) {
      return null;
    }

    return new FileEntryKey(
        getKey(digest, isExecutable), size, isExecutable, digest, hasSizeComponent);
  }

  private FileEntryKey parseFileEntryKey(String fileName, long size) {
    return parseFileEntryKey(fileName, size, digestUtil);
  }

  private boolean contains(
      Digest digest,
      boolean isExecutable,
      @Nullable Digest.Builder result,
      Consumer<String> onContains) {
    String key = getKey(digest, isExecutable);
    Entry entry = storage.get(key);
    if (entry != null && (digest.getSizeBytes() < 0 || digest.getSizeBytes() == entry.size)) {
      if (result != null) {
        result.mergeFrom(digest).setSizeBytes(entry.size);
      }
      onContains.accept(key);
      return true;
    }
    return false;
  }

  private void accessed(Iterable<String> keys) {
    /* could also bucket these */
    try {
      accessRecorder.execute(() -> recordAccess(keys));
    } catch (RejectedExecutionException e) {
      logger.log(
          Level.SEVERE, format("could not record access for %d keys", Iterables.size(keys)), e);
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
      Digest digest, @Nullable Digest.Builder result, Consumer<String> onContains) {
    /* maybe swap the order here if we're higher in ratio on one side */
    return contains(digest, false, result, onContains)
        || contains(digest, true, result, onContains);
  }

  @Override
  public Iterable<Digest> findMissingBlobs(Iterable<Digest> digests) throws InterruptedException {
    ImmutableList.Builder<Digest> builder = ImmutableList.builder();
    ImmutableList.Builder<String> found = ImmutableList.builder();
    for (Digest digest : digests) {
      if (digest.getSizeBytes() != 0 && !containsLocal(digest, null, found::add)) {
        builder.add(digest);
      }
    }
    List<String> foundDigests = found.build();
    if (!foundDigests.isEmpty()) {
      accessed(foundDigests);
    }
    ImmutableList<Digest> missingDigests = builder.build();
    return CasFallbackDelegate.findMissingBlobs(delegate, missingDigests);
  }

  @Override
  public boolean contains(Digest digest, Digest.Builder result) {
    return containsLocal(digest, result, (key) -> accessed(ImmutableList.of(key)))
        || CasFallbackDelegate.contains(delegate, digest, result);
  }

  @Override
  public ListenableFuture<Iterable<Response>> getAllFuture(Iterable<Digest> digests) {
    throw new UnsupportedOperationException();
  }

  protected InputStream newTransparentInput(Digest digest, long offset) throws IOException {
    try {
      return newLocalInput(digest, offset);
    } catch (NoSuchFileException e) {
      return CasFallbackDelegate.newInput(delegate, e, digest, offset);
    }
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  InputStream newLocalInput(Digest digest, long offset) throws IOException {
    logger.log(Level.FINE, format("getting input stream for %s", DigestUtil.toString(digest)));
    boolean isExecutable = false;
    do {
      String key = getKey(digest, isExecutable);
      Entry e = storage.get(key);
      if (e != null) {
        InputStream input;
        try {
          input = Files.newInputStream(getPath(key));
          input.skip(offset);
        } catch (NoSuchFileException eNoEnt) {
          boolean removed = false;
          synchronized (this) {
            Entry removedEntry = storage.remove(key);
            if (removedEntry == e) {
              unlinkEntry(removedEntry);
              removed = true;
            } else if (removedEntry != null) {
              logger.log(
                  Level.SEVERE,
                  "nonexistent entry %s did not match last unreferenced entry, restoring it",
                  key);
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
    } while (isExecutable);
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
    return newInputFallback(digest, offset);
  }

  @Override
  public Blob get(Digest digest) {
    try (InputStream in = newInput(digest, /* offset=*/ 0)) {
      return new Blob(ByteString.readFrom(in), digest);
    } catch (NoSuchFileException e) {
      return null;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static final int CHUNK_SIZE = 128 * 1024;

  @Override
  public void get(
      Digest digest,
      long offset,
      long count,
      ServerCallStreamObserver<ByteString> blobObserver,
      RequestMetadata requestMetadata) {
    InputStream in;
    try {
      in = newInput(digest, offset);
    } catch (IOException e) {
      blobObserver.onError(e);
      return;
    }
    blobObserver.setOnCancelHandler(
        () -> {
          try {
            in.close();
          } catch (IOException e) {
            logger.log(Level.SEVERE, "error closing input stream on cancel", e);
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
        logger.log(Level.SEVERE, "error closing input stream on error", ioEx);
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
            logger.log(Level.SEVERE, "error reading from input stream", e);
            try {
              in.close();
            } catch (IOException ioEx) {
              logger.log(Level.SEVERE, "error closing input stream on error", ioEx);
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
      logger.log(
          Level.SEVERE,
          "error during write completion onPut for " + DigestUtil.toString(digest),
          e);
      /* ignore error, writes must complete */
    }
    try {
      return getFuture(digest).set(digest.getSizeBytes());
    } catch (Exception e) {
      logger.log(
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
    String key = getKey(blob.getDigest(), false);
    try {
      logger.log(Level.FINE, format("put: %s", key));
      OutputStream out =
          putImpl(
              key,
              UUID.randomUUID(),
              () -> completeWrite(blob.getDigest()),
              blob.getDigest().getSizeBytes(),
              /* isExecutable=*/ false,
              () -> invalidateWrite(blob.getDigest()),
              /* isReset=*/ true);
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
      logger.log(Level.SEVERE, "error putting " + DigestUtil.toString(blob.getDigest()), e);
    }
  }

  @Override
  public Write getWrite(Digest digest, UUID uuid, RequestMetadata requestMetadata)
      throws EntryLimitException {
    if (digest.getSizeBytes() == 0) {
      return new CompleteWrite(0);
    }
    if (digest.getSizeBytes() > maxEntrySizeInBytes) {
      throw new EntryLimitException(digest.getSizeBytes(), maxEntrySizeInBytes);
    }
    try {
      return writes.get(
          BlobWriteKey.newBuilder().setDigest(digest).setIdentifier(uuid.toString()).build());
    } catch (ExecutionException e) {
      logger.log(
          Level.SEVERE, "error getting write for " + DigestUtil.toString(digest) + ":" + uuid, e);
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
    private final Runnable onClosed;
    private final long size;
    private boolean closed = false;

    UniqueWriteOutputStream(CancellableOutputStream out, Runnable onClosed, long size) {
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
      // we ignore closes below the complete size
      if (out.getWritten() >= size) {
        super.close();
      }
      closed = true;
      onClosed.run();
    }

    @Override
    public void cancel() throws IOException {
      out.cancel();
    }
  }

  Write newWrite(BlobWriteKey key, ListenableFuture<Long> future) {
    Write write =
        new Write() {
          CancellableOutputStream out = null;
          boolean isReset = false;
          SettableFuture<Void> closedFuture = null;
          long fileCommittedSize = -1;

          @Override
          public synchronized void reset() {
            try {
              if (out != null) {
                out.cancel();
              }
            } catch (IOException e) {
              logger.log(
                  Level.SEVERE,
                  "could not reset write "
                      + DigestUtil.toString(key.getDigest())
                      + ":"
                      + key.getIdentifier(),
                  e);
            } finally {
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
              return key.getDigest().getSizeBytes();
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
                    && containsLocal(key.getDigest(), /* result=*/ null, (key) -> {}));
          }

          @Override
          public synchronized ListenableFuture<FeedbackOutputStream> getOutputFuture(
              long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) {
            if (closedFuture == null || closedFuture.isDone()) {
              try {
                // this isn't great, and will block when there are multiple requesters
                return immediateFuture(
                    getOutput(deadlineAfter, deadlineAfterUnits, onReadyHandler));
              } catch (IOException e) {
                return immediateFailedFuture(e);
              }
            }
            return transformAsync(
                closedFuture,
                result -> getOutputFuture(deadlineAfter, deadlineAfterUnits, onReadyHandler),
                directExecutor());
          }

          @Override
          public synchronized FeedbackOutputStream getOutput(
              long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler)
              throws IOException {
            // caller will be the exclusive owner of this write stream. all other requests
            // will block until it is returned via a close.
            if (closedFuture != null) {
              try {
                closedFuture.get();
              } catch (ExecutionException e) {
                throw new IOException(e.getCause());
              } catch (InterruptedException e) {
                throw new IOException(e);
              }
            }
            SettableFuture<Void> outClosedFuture = SettableFuture.create();
            UniqueWriteOutputStream uniqueOut =
                createUniqueWriteOutput(
                    out,
                    key.getDigest(),
                    UUID.fromString(key.getIdentifier()),
                    () -> outClosedFuture.set(null),
                    this::isComplete,
                    isReset);
            commitOpenState(uniqueOut.delegate(), outClosedFuture);
            return uniqueOut;
          }

          private void commitOpenState(
              CancellableOutputStream out, SettableFuture<Void> closedFuture) {
            // transition the Write to an open state, and modify all internal state required
            // atomically
            // this function must. not. throw.

            this.out = out;
            this.closedFuture = closedFuture;
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
    write.getFuture().addListener(write::reset, directExecutor());
    return write;
  }

  UniqueWriteOutputStream createUniqueWriteOutput(
      CancellableOutputStream out,
      Digest digest,
      UUID uuid,
      Runnable onClosed,
      BooleanSupplier isComplete,
      boolean isReset)
      throws IOException {
    if (out == null) {
      out = newOutput(digest, uuid, onClosed, isComplete, isReset);
    }
    if (out == null) {
      // duplicate output stream
      out =
          new CancellableOutputStream(nullOutputStream()) {
            @Override
            public long getWritten() {
              return digest.getSizeBytes();
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
    return new UniqueWriteOutputStream(out, onClosed, digest.getSizeBytes());
  }

  CancellableOutputStream newOutput(
      Digest digest, UUID uuid, Runnable onClosed, BooleanSupplier isComplete, boolean isReset)
      throws IOException {
    String key = getKey(digest, false);
    final CancellableOutputStream cancellableOut;
    try {
      logger.log(Level.FINE, format("getWrite: %s", key));
      cancellableOut =
          putImpl(
              key,
              uuid,
              () -> completeWrite(digest),
              digest.getSizeBytes(),
              /* isExecutable=*/ false,
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
              decrementReference(key);
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
  public void put(Blob blob) throws InterruptedException {
    put(blob, /* onExpiration=*/ null);
  }

  @Override
  public long maxEntrySize() {
    return maxEntrySizeInBytes;
  }

  private static final class SharedLock implements Lock {
    private final AtomicBoolean locked = new AtomicBoolean(false);

    @Override
    public void lock() {
      for (; ; ) {
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

    @SuppressWarnings("NullableProblems")
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

    @SuppressWarnings("NullableProblems")
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
    private final String key;
    private final long size;
    private final boolean isExecutable;
    private final Digest digest;
    private final boolean legacy; // file is in old format name, should be renamed

    FileEntryKey(String key, long size, boolean isExecutable, Digest digest, boolean legacy) {
      this.key = key;
      this.size = size;
      this.isExecutable = isExecutable;
      this.digest = digest;
      this.legacy = legacy;
    }

    String getKey() {
      return key;
    }

    long getSize() {
      return size;
    }

    boolean getIsExecutable() {
      return isExecutable;
    }

    Digest getDigest() {
      return digest;
    }

    boolean isLegacy() {
      return legacy;
    }
  }

  public void initializeRootDirectory() throws IOException {
    for (Path dir : entryPathStrategy) {
      Files.createDirectories(dir);
    }
    fileStore = Files.getFileStore(root);
  }

  public StartupCacheResults start(boolean skipLoad) throws IOException, InterruptedException {
    return start(newDirectExecutorService(), skipLoad);
  }

  public StartupCacheResults start(ExecutorService removeDirectoryService, boolean skipLoad)
      throws IOException, InterruptedException {
    return start(onPut, removeDirectoryService, skipLoad);
  }

  /**
   * initialize the cache for persistent storage and inject any consistent entries which already
   * exist under the root into the storage map. This call will create the root if it does not exist,
   * and will scale in cost with the number of files already present.
   */
  public StartupCacheResults start(
      Consumer<Digest> onStartPut, ExecutorService removeDirectoryService, boolean skipLoad)
      throws IOException, InterruptedException {
    CasFallbackDelegate.start(delegate, onStartPut, removeDirectoryService, skipLoad);

    logger.log(Level.INFO, "Initializing cache at: " + root);
    Instant startTime = Instant.now();

    CacheLoadResults loadResults = new CacheLoadResults();
    loadResults.loadSkipped = skipLoad;

    // Load the cache
    if (!skipLoad) {
      initializeRootDirectory();
      loadResults = loadCache(onStartPut, removeDirectoryService);
    } else {
      // Skip loading the cache and ensure it is empty
      Directories.remove(root, removeDirectoryService);
      initializeRootDirectory();
    }

    logger.log(Level.INFO, "Creating Index");
    directoriesIndex.start();
    logger.log(Level.INFO, "Index Created");

    // Calculate Startup time
    Instant endTime = Instant.now();
    Duration startupTime = Duration.between(startTime, endTime);
    logger.log(Level.INFO, "Startup Time: " + startupTime.getSeconds() + "s");

    // Start metrics collection thread
    prometheusMetricsThread =
        new Thread(
            () -> {
              while (!Thread.currentThread().isInterrupted()) {
                try {
                  casSizeMetric.set(size());
                  casEntryCountMetric.set(entryCount());
                  TimeUnit.MINUTES.sleep(5);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  break;
                } catch (Exception e) {
                  logger.log(Level.SEVERE, "Could not update CasFileCache metrics", e);
                }
              }
            },
            "Prometheus CAS Metrics Collector");
    prometheusMetricsThread.start();

    // return information about the cache startup.
    StartupCacheResults startupResults = new StartupCacheResults();
    startupResults.cacheDirectory = root;
    startupResults.load = loadResults;
    startupResults.startupTime = startupTime;
    return startupResults;
  }

  private CacheLoadResults loadCache(
      Consumer<Digest> onStartPut, ExecutorService removeDirectoryService)
      throws IOException, InterruptedException {
    CacheLoadResults results = new CacheLoadResults();

    // Phase 1: Scan
    // build scan cache results by analyzing each file on the root.
    results.scan = scanRoot(onStartPut);
    logCacheScanResults(results.scan);
    deleteInvalidFileContent(results.scan.deleteFiles, removeDirectoryService);

    // Phase 2: Compute
    // recursively construct all directory structures.
    results.invalidDirectories = computeDirectories(results.scan);
    logComputeDirectoriesResults(results.invalidDirectories);
    deleteInvalidFileContent(results.invalidDirectories, removeDirectoryService);

    return results;
  }

  private void deleteInvalidFileContent(List<Path> files, ExecutorService removeDirectoryService) {
    try {
      for (Path path : files) {
        if (Files.isDirectory(path)) {
          Directories.remove(path, removeDirectoryService);
        } else {
          Files.delete(path);
        }
      }
    } catch (Exception e) {
      logger.log(Level.SEVERE, "failure to delete CAS content: ", e);
    }
  }

  @SuppressWarnings("unchecked")
  private void logCacheScanResults(CacheScanResults cacheScanResults) {
    JSONObject obj = new JSONObject();
    obj.put("dirs", cacheScanResults.computeDirs.size());
    obj.put("keys", cacheScanResults.fileKeys.size());
    obj.put("delete", cacheScanResults.deleteFiles.size());
    logger.log(Level.INFO, obj.toString());
  }

  @SuppressWarnings("unchecked")
  private void logComputeDirectoriesResults(List<Path> invalidDirectories) {
    JSONObject obj = new JSONObject();
    obj.put("invalid dirs", invalidDirectories.size());
    logger.log(Level.INFO, obj.toString());
  }

  private CacheScanResults scanRoot(Consumer<Digest> onStartPut)
      throws IOException, InterruptedException {
    // create thread pool
    int nThreads = Runtime.getRuntime().availableProcessors();
    String threadNameFormat = "scan-cache-pool-%d";
    ExecutorService pool =
        Executors.newFixedThreadPool(
            nThreads, new ThreadFactoryBuilder().setNameFormat(threadNameFormat).build());

    // collect keys from cache root.
    ImmutableList.Builder<Path> computeDirsBuilder = new ImmutableList.Builder<>();
    ImmutableList.Builder<Path> deleteFilesBuilder = new ImmutableList.Builder<>();
    ImmutableMap.Builder<Object, Entry> fileKeysBuilder = new ImmutableMap.Builder<>();

    // TODO invalidate mismatched hash prefix
    Iterable<Path> files = ImmutableList.of();
    for (Path path : entryPathStrategy) {
      files = Iterables.concat(files, listDir(path));
    }

    for (Path branchDir : entryPathStrategy.branchDirectories()) {
      boolean isRoot = branchDir == root;
      for (Path file : listDir(branchDir)) {
        // allow migration for digest-y names
        String name = file.getFileName().toString();
        if (!(isRoot && name.equals(directoriesIndexDbName)) && !name.matches("[0-9a-f]{2}")) {
          deleteFilesBuilder.add(file);
        }
      }
    }

    for (Path file : files) {
      pool.execute(
          () -> {
            try {
              processRootFile(
                  onStartPut, file, computeDirsBuilder, deleteFilesBuilder, fileKeysBuilder);
            } catch (Exception e) {
              logger.log(Level.SEVERE, "error reading file " + file.toString(), e);
            }
          });
    }

    joinThreads(pool, "Scanning Cache Root...");

    // log information from scanning cache root.
    CacheScanResults cacheScanResults = new CacheScanResults();
    cacheScanResults.computeDirs = computeDirsBuilder.build();
    cacheScanResults.deleteFiles = deleteFilesBuilder.build();
    cacheScanResults.fileKeys = fileKeysBuilder.build();

    return cacheScanResults;
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  private void processRootFile(
      Consumer<Digest> onStartPut,
      Path file,
      ImmutableList.Builder<Path> computeDirs,
      ImmutableList.Builder<Path> deleteFiles,
      ImmutableMap.Builder<Object, Entry> fileKeys)
      throws IOException {
    String basename = file.getFileName().toString();

    FileStatus stat = stat(file, false, fileStore);

    // mark directory for later key compute
    if (file.toString().endsWith("_dir")) {
      if (stat.isDirectory()) {
        synchronized (computeDirs) {
          computeDirs.add(file);
        }
      } else {
        synchronized (deleteFiles) {
          deleteFiles.add(file);
        }
      }
    } else if (stat.isDirectory()) {
      synchronized (deleteFiles) {
        deleteFiles.add(file);
      }
    } else {
      // if cas is full or entry is oversized or empty, mark file for later deletion.
      long size = stat.getSize();
      if (sizeInBytes + size > maxSizeInBytes || size > maxEntrySizeInBytes || size == 0) {
        synchronized (deleteFiles) {
          deleteFiles.add(file);
        }
      } else {
        // get the key entry from the file name.
        FileEntryKey fileEntryKey = parseFileEntryKey(basename, stat.getSize());

        // if key entry file name cannot be parsed, mark file for later deletion.
        if (fileEntryKey == null || stat.isReadOnlyExecutable() != fileEntryKey.getIsExecutable()) {
          synchronized (deleteFiles) {
            deleteFiles.add(file);
          }
        } else {
          String key = fileEntryKey.getKey();
          Path keyPath = getPath(key);
          // remove/refactor when #677 is closed
          if (fileEntryKey.isLegacy()) {
            Files.move(file, keyPath);
          }
          // populate key it is not currently stored.
          Entry e = new Entry(key, size, Deadline.after(10, SECONDS));
          Object fileKey = getFileKey(keyPath, stat);
          synchronized (fileKeys) {
            fileKeys.put(fileKey, e);
          }
          storage.put(e.key, e);
          onStartPut.accept(fileEntryKey.getDigest());
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
  private List<Path> computeDirectories(CacheScanResults cacheScanResults)
      throws InterruptedException {
    // create thread pool
    int nThreads = Runtime.getRuntime().availableProcessors();
    String threadNameFormat = "compute-cache-pool-%d";
    ExecutorService pool =
        Executors.newFixedThreadPool(
            nThreads, new ThreadFactoryBuilder().setNameFormat(threadNameFormat).build());

    ImmutableList.Builder<Path> invalidDirectories = new ImmutableList.Builder<>();

    for (Path path : cacheScanResults.computeDirs) {
      pool.execute(
          () -> {
            try {
              ImmutableList.Builder<String> inputsBuilder = ImmutableList.builder();

              List<NamedFileKey> sortedDirent = listDirentSorted(path, fileStore);

              Directory directory =
                  computeDirectory(path, sortedDirent, cacheScanResults.fileKeys, inputsBuilder);

              Digest digest = directory == null ? null : digestUtil.compute(directory);

              // apply legacy rename if possible
              // Remove on major release or when #677 is closed
              Path dirPath = path;
              String basename = path.getFileName().toString();
              if (basename.equals(digest.getHash() + "_" + digest.getSizeBytes() + "_dir")) {
                dirPath = getDirectoryPath(digest);
                if (Files.exists(dirPath)) {
                  // destroy this directory if the destination already exists
                  digest = null;
                } else {
                  Files.move(path, dirPath);
                }
              }
              // end legacy support, drop modified dirPath

              if (digest != null && getDirectoryPath(digest).equals(dirPath)) {
                DirectoryEntry e = new DirectoryEntry(directory, Deadline.after(10, SECONDS));
                directoriesIndex.put(digest, inputsBuilder.build());
                directoryStorage.put(digest, e);
              } else {
                synchronized (invalidDirectories) {
                  invalidDirectories.add(dirPath);
                }
              }
            } catch (Exception e) {
              logger.log(Level.SEVERE, "error processing directory " + path.toString(), e);
            }
          });
    }

    joinThreads(pool, "Populating Directories...");

    return invalidDirectories.build();
  }

  private Directory computeDirectory(
      Path path,
      List<NamedFileKey> sortedDirent,
      Map<Object, Entry> fileKeys,
      ImmutableList.Builder<String> inputsBuilder)
      throws IOException {
    Directory.Builder b = Directory.newBuilder();

    for (NamedFileKey dirent : sortedDirent) {
      String name = dirent.getName();
      Path entryPath = path.resolve(name);
      if (dirent.getFileStatus().isSymbolicLink()) {
        b.addSymlinksBuilder()
            .setName(name)
            .setTarget(Files.readSymbolicLink(entryPath).toString());
        // TODO symlink properties
      } else {
        Entry e = fileKeys.get(dirent.getFileKey());

        // decide if file is a directory or empty/non-empty file
        boolean isDirectory = dirent.getFileStatus().isDirectory();
        boolean isEmptyFile = false;
        if (e == null && !isDirectory) {
          if (dirent.getFileStatus().getSize() == 0) {
            isEmptyFile = true;
          } else {
            // no entry, not a directory, will NPE
            b.addFilesBuilder().setName(name + "-MISSING");
            // continue here to hopefully result in invalid directory
            break;
          }
        }

        // directory
        if (isDirectory) {
          List<NamedFileKey> childDirent = listDirentSorted(entryPath, fileStore);
          Directory dir = computeDirectory(entryPath, childDirent, fileKeys, inputsBuilder);
          b.addDirectoriesBuilder().setName(name).setDigest(digestUtil.compute(dir));
        } else if (isEmptyFile) {
          // empty file
          boolean isExecutable = isReadOnlyExecutable(entryPath, fileStore);
          b.addFilesBuilder()
              .setName(name)
              .setDigest(digestUtil.empty())
              .setIsExecutable(isExecutable);
        } else {
          // non-empty file
          inputsBuilder.add(e.key);
          Digest digest = CASFileCache.keyToDigest(e.key, e.size, digestUtil);
          boolean isExecutable = e.key.endsWith("_exec");
          b.addFilesBuilder().setName(name).setDigest(digest).setIsExecutable(isExecutable);
        }
      }
    }

    return b.build();
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  private void joinThreads(ExecutorService pool, String message) throws InterruptedException {
    pool.shutdown();
    while (!pool.isTerminated()) {
      logger.log(Level.INFO, message);
      pool.awaitTermination(1, TimeUnit.MINUTES);
    }
  }

  private static String digestFilename(Digest digest) {
    return digest.getHash();
  }

  public static String getFileName(Digest digest, boolean isExecutable) {
    return digestFilename(digest) + (isExecutable ? "_exec" : "");
  }

  public String getKey(Digest digest, boolean isExecutable) {
    return getFileName(digest, isExecutable);
  }

  private synchronized void decrementReference(String inputFile) throws IOException {
    decrementReferencesSynchronized(ImmutableList.of(inputFile), ImmutableList.of());
  }

  public synchronized void decrementReferences(
      Iterable<String> inputFiles, Iterable<Digest> inputDirectories)
      throws IOException, InterruptedException {
    try {
      decrementReferencesSynchronized(inputFiles, inputDirectories);
    } catch (ClosedByInterruptException e) {
      InterruptedException intEx = new InterruptedException();
      intEx.addSuppressed(e);
      throw intEx;
    }
  }

  @SuppressWarnings("NonAtomicOperationOnVolatileField")
  private int decrementInputReferences(Iterable<String> inputFiles) {
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

  @GuardedBy("this")
  private void decrementReferencesSynchronized(
      Iterable<String> inputFiles, Iterable<Digest> inputDirectories) throws IOException {
    // decrement references and notify if any dropped to 0
    // insert after the last 0-reference count entry in list
    int entriesDereferenced = decrementInputReferences(inputFiles);
    for (Digest inputDirectory : inputDirectories) {
      DirectoryEntry dirEntry = directoryStorage.get(inputDirectory);
      if (dirEntry == null) {
        throw new IllegalStateException(
            "inputDirectory "
                + DigestUtil.toString(inputDirectory)
                + " is not in directoryStorage");
      }
      entriesDereferenced +=
          decrementInputReferences(directoriesIndex.directoryEntries(inputDirectory));
    }
    if (entriesDereferenced > 0) {
      notify();
    }
  }

  public Path getRoot() {
    return root;
  }

  public Path getPath(String filename) {
    return entryPathStrategy.getPath(filename);
  }

  private synchronized void dischargeAndNotify(long size) {
    discharge(size);
    notify();
  }

  @GuardedBy("this")
  private void discharge(long size) {
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

  @VisibleForTesting
  public Path getDirectoryPath(Digest digest) {
    return getPath(digestFilename(digest) + "_dir");
  }

  @GuardedBy("this")
  private Entry waitForLastUnreferencedEntry(long blobSizeInBytes) throws InterruptedException {
    while (header.after == header) {
      int references = 0;
      int keys = 0;
      int min = -1;
      int max = 0;
      String minkey = null;
      String maxkey = null;
      logger.log(
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
          logger.log(
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
      logger.log(
          Level.INFO,
          format(
              "CASFileCache::expireEntry(%d) unreferenced list is empty, %d bytes, %d keys with %d references, min(%d, %s), max(%d, %s)",
              blobSizeInBytes, sizeInBytes, keys, references, min, minkey, max, maxkey));
      wait();
      if (sizeInBytes <= maxSizeInBytes) {
        return null;
      }
    }
    return header.after;
  }

  @SuppressWarnings("NonAtomicOperationOnVolatileField")
  @GuardedBy("this")
  List<ListenableFuture<Void>> unlinkAndExpireDirectories(Entry entry, ExecutorService service) {
    ImmutableList.Builder<ListenableFuture<Void>> builder = ImmutableList.builder();
    Iterable<Digest> containingDirectories;
    try {
      containingDirectories = directoriesIndex.removeEntry(entry.key);
    } catch (Exception e) {
      logger.log(
          Level.SEVERE, format("error removing entry %s from directoriesIndex", entry.key), e);
      containingDirectories = ImmutableList.of();
    }
    for (Digest containingDirectory : containingDirectories) {
      builder.add(expireDirectory(containingDirectory, service));
    }
    entry.unlink();
    unreferencedEntryCount--;
    if (entry.referenceCount != 0) {
      logger.log(Level.SEVERE, "removed referenced entry " + entry.key);
    }
    return builder.build();
  }

  @GuardedBy("this")
  private ListenableFuture<Entry> dischargeEntryFuture(Entry entry, ExecutorService service) {
    List<ListenableFuture<Void>> directoryExpirationFutures =
        unlinkAndExpireDirectories(entry, service);
    discharge(entry.size);
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
                    logger.log(
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
            logger.log(
                Level.SEVERE, "undeferrable exception during discharge of " + entry.key, cause);
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

  @SuppressWarnings("NonAtomicOperationOnVolatileField")
  @GuardedBy("this")
  private ListenableFuture<Entry> expireEntry(long blobSizeInBytes, ExecutorService service)
      throws IOException, InterruptedException {
    for (Entry e = waitForLastUnreferencedEntry(blobSizeInBytes);
        e != null;
        e = waitForLastUnreferencedEntry(blobSizeInBytes)) {
      if (e.referenceCount != 0) {
        throw new IllegalStateException(
            "ERROR: Reference counts lru ordering has not been maintained correctly, attempting to expire referenced (or negatively counted) content "
                + e.key
                + " with "
                + e.referenceCount
                + " references");
      }
      expireEntryFallback(e);
      Entry removedEntry = storage.remove(e.key);
      // reference compare on purpose
      if (removedEntry == e) {
        return dischargeEntryFuture(e, service);
      }
      if (removedEntry == null) {
        logger.log(Level.SEVERE, format("entry %s was already removed during expiration", e.key));
        if (e.isLinked()) {
          logger.log(Level.SEVERE, format("removing spuriously non-existent entry %s", e.key));
          e.unlink();
          unreferencedEntryCount--;
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
        storage.put(e.key, removedEntry);
      }
    }
    return null;
  }

  @GuardedBy("this")
  private ListenableFuture<Void> expireDirectory(Digest digest, ExecutorService service) {
    DirectoryEntry e = directoryStorage.remove(digest);
    if (e == null) {
      logger.log(
          Level.SEVERE,
          format("CASFileCache::expireDirectory(%s) does not exist", DigestUtil.toString(digest)));
      return immediateFuture(null);
    }

    return Directories.remove(getDirectoryPath(digest), service);
  }

  // FIXME look into whether this is needed at all
  public Iterable<ListenableFuture<Path>> putFiles(
      Iterable<FileNode> files,
      Iterable<SymlinkNode> symlinks,
      Path path,
      ImmutableList.Builder<String> inputsBuilder,
      ExecutorService service)
      throws IOException, InterruptedException {
    ImmutableList.Builder<ListenableFuture<Path>> putFutures = ImmutableList.builder();
    putDirectoryFiles(files, symlinks, path, inputsBuilder, putFutures, service);
    return putFutures.build();
  }

  @SuppressWarnings("ConstantConditions")
  private void putDirectoryFiles(
      Iterable<FileNode> files,
      Iterable<SymlinkNode> symlinks,
      Path path,
      ImmutableList.Builder<String> inputsBuilder,
      ImmutableList.Builder<ListenableFuture<Path>> putFutures,
      ExecutorService service) {
    for (FileNode fileNode : files) {
      Path filePath = path.resolve(fileNode.getName());
      final ListenableFuture<Path> putFuture;
      if (fileNode.getDigest().getSizeBytes() != 0) {
        String key = getKey(fileNode.getDigest(), fileNode.getIsExecutable());
        putFuture =
            transformAsync(
                put(fileNode.getDigest(), fileNode.getIsExecutable(), service),
                (cacheFilePath) -> {
                  // FIXME this can die with 'too many links'... needs some cascading fallout
                  Files.createLink(filePath, cacheFilePath);
                  // we saw null entries in the built immutable list without synchronization
                  synchronized (inputsBuilder) {
                    inputsBuilder.add(key);
                  }
                  return immediateFuture(cacheFilePath);
                },
                service);
      } else {
        putFuture =
            listeningDecorator(service)
                .submit(
                    () -> {
                      Files.createFile(filePath);
                      setReadOnlyPerms(filePath, fileNode.getIsExecutable(), fileStore);
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

  private void fetchDirectory(
      Path path,
      Digest digest,
      Map<Digest, Directory> directoriesIndex,
      ImmutableList.Builder<String> inputsBuilder,
      ImmutableList.Builder<ListenableFuture<Path>> putFutures,
      ExecutorService service)
      throws IOException, InterruptedException {
    if (Files.exists(path)) {
      if (Files.isDirectory(path)) {
        logger.log(Level.FINE, "removing existing directory " + path + " for fetch");
        Directories.remove(path);
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
          format("directory not found for %s(%s)", path, DigestUtil.toString(digest)));
    }
    Files.createDirectory(path);
    putDirectoryFiles(
        directory.getFilesList(),
        directory.getSymlinksList(),
        path,
        inputsBuilder,
        putFutures,
        service);
    for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
      fetchDirectory(
          path.resolve(directoryNode.getName()),
          directoryNode.getDigest(),
          directoriesIndex,
          inputsBuilder,
          putFutures,
          service);
    }
  }

  public ListenableFuture<Path> putDirectory(
      Digest digest, Map<Digest, Directory> directoriesIndex, ExecutorService service) {
    Path path = getDirectoryPath(digest);
    Lock l = locks.acquire(path);
    logger.log(Level.FINE, format("locking directory %s", path.getFileName()));
    try {
      l.lockInterruptibly();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return immediateFailedFuture(e);
    }
    logger.log(Level.FINE, format("locked directory %s", path.getFileName()));
    ListenableFuture<Path> putFuture;
    try {
      putFuture = putDirectorySynchronized(path, digest, directoriesIndex, service);
    } catch (IOException e) {
      putFuture = immediateFailedFuture(e);
    }
    putFuture.addListener(
        () -> {
          l.unlock();
          logger.log(Level.FINE, format("directory %s has been unlocked", path.getFileName()));
        },
        service);
    return putFuture;
  }

  private boolean directoryExists(
      Path path, Directory directory, Map<Digest, Directory> directoriesIndex) {
    if (!Files.exists(path)) {
      logger.log(Level.SEVERE, format("directory path %s does not exist", path));
      return false;
    }
    for (FileNode fileNode : directory.getFilesList()) {
      Path filePath = path.resolve(fileNode.getName());
      if (!Files.exists(filePath)) {
        logger.log(Level.SEVERE, format("directory file entry %s does not exist", filePath));
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

  private boolean directoryEntryExists(
      Path path, DirectoryEntry dirEntry, Map<Digest, Directory> directoriesIndex) {
    if (!dirEntry.existsDeadline.isExpired()) {
      return true;
    }

    if (directoryExists(path, dirEntry.directory, directoriesIndex)) {
      dirEntry.existsDeadline = Deadline.after(10, SECONDS);
      return true;
    }
    return false;
  }

  static class PutDirectoryException extends IOException {
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

  @SuppressWarnings("ConstantConditions")
  private ListenableFuture<Path> putDirectorySynchronized(
      Path path, Digest digest, Map<Digest, Directory> directoriesByDigest, ExecutorService service)
      throws IOException {
    logger.log(Level.FINE, format("directory %s has been locked", path.getFileName()));
    ListenableFuture<Void> expireFuture;
    synchronized (this) {
      DirectoryEntry e = directoryStorage.get(digest);
      if (e == null) {
        expireFuture = immediateFuture(null);
      } else {
        ImmutableList.Builder<String> inputsBuilder = ImmutableList.builder();
        for (String input : directoriesIndex.directoryEntries(digest)) {
          Entry fileEntry = storage.get(input);
          if (fileEntry == null) {
            logger.log(
                Level.SEVERE,
                format(
                    "CASFileCache::putDirectory(%s) exists, but input %s does not, purging it with fire and resorting to fetch",
                    DigestUtil.toString(digest), input));
            e = null;
            break;
          }
          if (fileEntry.incrementReference()) {
            unreferencedEntryCount--;
          }
          checkNotNull(input);
          inputsBuilder.add(input);
        }

        if (e != null) {
          logger.log(Level.FINE, format("found existing entry for %s", path.getFileName()));
          if (directoryEntryExists(path, e, directoriesByDigest)) {
            return immediateFuture(path);
          }
          logger.log(
              Level.SEVERE,
              format(
                  "directory %s does not exist in cache, purging it with fire and resorting to fetch",
                  path.getFileName()));
        }

        decrementReferencesSynchronized(inputsBuilder.build(), ImmutableList.of());
        expireFuture = expireDirectory(digest, service);
        logger.log(Level.FINE, format("expiring existing entry for %s", path.getFileName()));
      }
    }

    ListenableFuture<Void> deindexFuture =
        transformAsync(
            expireFuture,
            result -> {
              try {
                directoriesIndex.remove(digest);
              } catch (IOException e) {
                return immediateFailedFuture(e);
              }
              return immediateFuture(null);
            },
            service);

    ImmutableList.Builder<String> inputsBuilder = ImmutableList.builder();
    ListenableFuture<Void> fetchFuture =
        transformAsync(
            deindexFuture,
            result -> {
              logger.log(Level.FINE, format("expiry complete, fetching %s", path.getFileName()));
              ImmutableList.Builder<ListenableFuture<Path>> putFuturesBuilder =
                  ImmutableList.builder();
              fetchDirectory(
                  path, digest, directoriesByDigest, inputsBuilder, putFuturesBuilder, service);
              ImmutableList<ListenableFuture<Path>> putFutures = putFuturesBuilder.build();

              // is this better suited for whenAllComplete?

              return transformAsync(
                  successfulAsList(putFutures),
                  paths -> {
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
                      return immediateFailedFuture(
                          new PutDirectoryException(path, digest, failures.build()));
                    }
                    return immediateFuture(null);
                  },
                  service);
            },
            service);

    ListenableFuture<Void> chmodAndIndexFuture =
        transformAsync(
            fetchFuture,
            (result) -> {
              try {
                disableAllWriteAccess(path);
              } catch (IOException e) {
                logger.log(Level.SEVERE, "error while disabling write permissions on " + path, e);
                return immediateFailedFuture(e);
              }
              try {
                directoriesIndex.put(digest, inputsBuilder.build());
              } catch (IOException e) {
                logger.log(Level.SEVERE, "error while indexing " + path, e);
                return immediateFailedFuture(e);
              }
              return immediateFuture(null);
            },
            service);

    ListenableFuture<Void> rollbackFuture =
        catchingAsync(
            chmodAndIndexFuture,
            Throwable.class,
            e -> {
              ImmutableList<String> inputs = inputsBuilder.build();
              directoriesIndex.remove(digest);
              synchronized (this) {
                try {
                  decrementReferencesSynchronized(inputs, ImmutableList.of());
                } catch (IOException ioEx) {
                  e.addSuppressed(ioEx);
                }
              }
              try {
                logger.log(Level.FINE, "removing directory to roll back " + path);
                Directories.remove(path);
              } catch (IOException removeException) {
                logger.log(
                    Level.SEVERE,
                    "error during directory removal after fetch failure of " + path,
                    removeException);
                e.addSuppressed(removeException);
              }
              return immediateFailedFuture(e);
            },
            service);

    return transform(
        rollbackFuture,
        (results) -> {
          logger.log(
              Level.FINE, format("directory fetch complete, inserting %s", path.getFileName()));
          DirectoryEntry e =
              new DirectoryEntry(
                  // might want to have this treatment ahead of this
                  digest.getSizeBytes() == 0
                      ? Directory.getDefaultInstance()
                      : directoriesByDigest.get(digest),
                  Deadline.after(10, SECONDS));
          directoryStorage.put(digest, e);
          return path;
        },
        service);
  }

  @VisibleForTesting
  public Path put(Digest digest, boolean isExecutable) throws IOException, InterruptedException {
    checkState(digest.getSizeBytes() > 0, "file entries may not be empty");

    return putAndCopy(digest, isExecutable);
  }

  // This can result in deadlock if called with a direct executor. I'm unsure how to guard
  // against it, until we can get to using a current-download future
  public ListenableFuture<Path> put(Digest digest, boolean isExecutable, Executor executor) {
    checkState(digest.getSizeBytes() > 0, "file entries may not be empty");

    return transformAsync(
        immediateFuture(null),
        (result) -> immediateFuture(putAndCopy(digest, isExecutable)),
        executor);
  }

  @SuppressWarnings("ThrowFromFinallyBlock")
  Path putAndCopy(Digest digest, boolean isExecutable) throws IOException, InterruptedException {
    String key = getKey(digest, isExecutable);
    CancellableOutputStream out =
        putImpl(
            key,
            UUID.randomUUID(),
            () -> completeWrite(digest),
            digest.getSizeBytes(),
            isExecutable,
            () -> invalidateWrite(digest),
            /* isReset=*/ true);
    if (out != null) {
      boolean complete = false;
      try {
        copyExternalInput(digest, out);
        complete = true;
      } finally {
        try {
          logger.log(
              Level.FINE, format("closing output stream for %s", DigestUtil.toString(digest)));
          if (complete) {
            out.close();
          } else {
            out.cancel();
          }
          logger.log(
              Level.FINE, format("output stream closed for %s", DigestUtil.toString(digest)));
        } catch (IOException e) {
          if (Thread.interrupted()) {
            logger.log(
                Level.SEVERE,
                format("could not close stream for %s", DigestUtil.toString(digest)),
                e);
            //noinspection deprecation
            Throwables.propagateIfInstanceOf(e.getCause(), InterruptedException.class);
            throw new InterruptedException();
          } else {
            logger.log(
                Level.FINE,
                format("failed output stream close for %s", DigestUtil.toString(digest)),
                e);
          }
          throw e;
        }
      }
    }
    return getPath(key);
  }

  private void copyExternalInput(Digest digest, CancellableOutputStream out)
      throws IOException, InterruptedException {
    logger.log(Level.FINE, format("downloading %s", DigestUtil.toString(digest)));
    try (InputStream in = newExternalInput(digest)) {
      ByteStreams.copy(in, out);
    } catch (IOException e) {
      out.cancel();
      logger.log(
          Level.WARNING,
          format("error downloading %s", DigestUtil.toString(digest)),
          e); // prevent burial by early end of stream during close
      throw e;
    }
    logger.log(Level.FINE, format("download of %s complete", DigestUtil.toString(digest)));
  }

  @FunctionalInterface
  private interface IORunnable {
    void run() throws IOException;
  }

  private abstract static class CancellableOutputStream extends WriteOutputStream {
    CancellableOutputStream(OutputStream out) {
      super(out);
    }

    CancellableOutputStream(WriteOutputStream out) {
      super(out);
    }

    abstract void cancel() throws IOException;
  }

  private static final CancellableOutputStream DUPLICATE_OUTPUT_STREAM =
      new CancellableOutputStream(nullOutputStream()) {
        @Override
        void cancel() {}
      };

  private CancellableOutputStream putImpl(
      String key,
      UUID writeId,
      Supplier<Boolean> writeWinner,
      long blobSizeInBytes,
      boolean isExecutable,
      Runnable onInsert,
      boolean isReset)
      throws IOException, InterruptedException {
    CancellableOutputStream out =
        putOrReference(key, writeId, writeWinner, blobSizeInBytes, isExecutable, onInsert, isReset);
    if (out == DUPLICATE_OUTPUT_STREAM) {
      return null;
    }
    logger.log(Level.FINE, format("entry %s is missing, downloading and populating", key));
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

  private synchronized boolean referenceIfExists(String key) throws IOException {
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
        dischargeAndNotify(blobSizeInBytes);
      }
    }
  }

  @SuppressWarnings({"ConstantConditions", "ResultOfMethodCallIgnored"})
  private boolean charge(String key, long blobSizeInBytes, AtomicBoolean requiresDischarge)
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
                        Files.delete(getPath(expiredKey));
                      } catch (NoSuchFileException eNoEnt) {
                        logger.log(
                            Level.SEVERE,
                            format(
                                "CASFileCache::putImpl: expired key %s did not exist to delete",
                                expiredKey));
                      }
                      FileEntryKey fileEntryKey = parseFileEntryKey(expiredKey, expiredEntry.size);
                      if (fileEntryKey == null) {
                        logger.log(
                            Level.SEVERE, format("error parsing expired key %s", expiredKey));
                      } else if (storage.containsKey(
                          getKey(fileEntryKey.getDigest(), !fileEntryKey.getIsExecutable()))) {
                        return immediateFuture(null);
                      }
                      expiredKeyCounter.inc();
                      logger.log(Level.INFO, format("expired key %s", expiredKey));
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
    return true;
  }

  private CancellableOutputStream putOrReferenceGuarded(
      String key,
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

    String writeKey = key + "." + writeId;
    Path writePath = getPath(key).resolveSibling(writeKey);
    final long committedSize;
    final HashingOutputStream hashOut;
    if (!isReset && Files.exists(writePath)) {
      committedSize = Files.size(writePath);
      try (InputStream in = Files.newInputStream(writePath)) {
        SkipOutputStream skipStream =
            new SkipOutputStream(Files.newOutputStream(writePath, APPEND), committedSize);
        hashOut = digestUtil.newHashingOutputStream(skipStream);
        ByteStreams.copy(in, hashOut);
        checkState(skipStream.isSkipped());
      }
    } else {
      committedSize = 0;
      hashOut = digestUtil.newHashingOutputStream(Files.newOutputStream(writePath, CREATE));
    }
    return new CancellableOutputStream(hashOut) {
      long written = committedSize;
      final Digest expectedDigest = keyToDigest(key, blobSizeInBytes, digestUtil);

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
        if (written >= blobSizeInBytes) {
          throw new IOException(
              format("attempted overwrite at %d by 1 byte for %s", written, writeKey));
        }
        hashOut.write(b);
        written++;
      }

      @Override
      public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        if (written + len > blobSizeInBytes) {
          throw new IOException(
              format("attempted overwrite at %d by %d bytes for %s", written, len, writeKey));
        }
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
          Digest actual = Digest.newBuilder().setHash(hash).setSizeBytes(size).build();
          throw new DigestMismatchException(actual, expectedDigest);
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
          Digest actual = Digest.newBuilder().setHash(hash).setSizeBytes(getWritten()).build();
          throw new DigestMismatchException(actual, expectedDigest);
        }
        try {
          setReadOnlyPerms(writePath, isExecutable, fileStore);
        } catch (IOException e) {
          dischargeAndNotify(blobSizeInBytes);
          throw e;
        }

        Entry entry = new Entry(key, blobSizeInBytes, Deadline.after(10, SECONDS));

        Entry existingEntry = null;
        boolean inserted = false;
        try {
          Files.createLink(CASFileCache.this.getPath(key), writePath);
          existingEntry = storage.putIfAbsent(key, entry);
          inserted = existingEntry == null;
        } catch (FileAlreadyExistsException e) {
          logger.log(
              Level.FINE, "file already exists for " + key + ", nonexistent entry will fail");
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
          logger.log(Level.FINE, "lost the race to insert " + key);
          if (!referenceIfExists(key)) {
            // we would lose our accountability and have a presumed reference if we returned
            throw new IllegalStateException("storage conflict with existing key for " + key);
          }
        } else if (writeWinner.get()) {
          logger.log(Level.FINE, "won the race to insert " + key);
          try {
            onInsert.run();
          } catch (RuntimeException e) {
            throw new IOException(e);
          }
        } else {
          logger.log(Level.FINE, "did not win the race to insert " + key);
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
      logger.log(
          Level.FINER,
          "incrementing references to "
              + key
              + " from "
              + referenceCount
              + " to "
              + (referenceCount + 1));
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
      logger.log(
          Level.FINER,
          "decrementing references to "
              + key
              + " from "
              + referenceCount
              + " to "
              + (referenceCount - 1));
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

  protected static class DirectoryEntry {
    public final Directory directory;
    Deadline existsDeadline;

    public DirectoryEntry(Directory directory, Deadline existsDeadline) {
      this.directory = directory;
      this.existsDeadline = existsDeadline;
    }
  }

  protected abstract InputStream newExternalInput(Digest digest)
      throws IOException, InterruptedException;

  // CAS fallback methods

  private InputStream newInputFallback(Digest digest, long offset) throws IOException {
    checkNotNull(delegate);

    if (digest.getSizeBytes() > maxEntrySizeInBytes) {
      return delegate.newInput(digest, offset);
    }
    Write write = getWrite(digest, UUID.randomUUID(), RequestMetadata.getDefaultInstance());
    return newReadThroughInput(digest, offset, write);
  }

  ReadThroughInputStream newReadThroughInput(Digest digest, long offset, Write write)
      throws IOException {
    return new ReadThroughInputStream(
        delegate.newInput(digest, 0),
        localOffset -> newTransparentInput(digest, localOffset),
        digest.getSizeBytes(),
        offset,
        write);
  }

  private void expireEntryFallback(Entry e) throws IOException, InterruptedException {
    if (delegate != null) {
      FileEntryKey fileEntryKey = parseFileEntryKey(e.key, e.size);
      if (fileEntryKey == null) {
        logger.log(Level.SEVERE, format("error parsing expired key %s", e.key));
      } else {
        Write write =
            delegate.getWrite(
                fileEntryKey.getDigest(), UUID.randomUUID(), RequestMetadata.getDefaultInstance());
        performCopy(write, e);
      }
    }
  }

  private void performCopy(Write write, Entry e) throws IOException, InterruptedException {
    try (OutputStream out = write.getOutput(1, MINUTES, () -> {});
        InputStream in = Files.newInputStream(getPath(e.key))) {
      ByteStreams.copy(in, out);
    } catch (IOException ioEx) {
      write.reset();
      logger.log(Level.SEVERE, format("error delegating expired entry %s", e.key), ioEx);
      throw new InterruptedException();
    }
  }
}
