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

import static build.buildfarm.common.io.Directories.disableAllWriteAccess;
import static build.buildfarm.common.io.EvenMoreFiles.isReadOnlyExecutable;
import static build.buildfarm.common.io.Utils.listDirentSorted;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.catchingAsync;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.successfulAsList;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.HOURS;

import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.common.BuildfarmExecutors;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.common.ZstdDecompressingOutputStream.FixedBufferPool;
import build.buildfarm.common.io.Directories;
import build.buildfarm.common.io.NamedFileKey;
import build.buildfarm.v1test.Digest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Deadline;
import io.prometheus.client.Counter;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.logging.Level;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import lombok.extern.java.Log;

@Log
public class LegacyDirectoryCFC extends CASFileCache {
  // Prometheus metrics
  private static final Counter casCopyFallbackMetric =
      Counter.build()
          .name("cas_copy_fallback")
          .help("Number of times the CAS performed a file copy because hardlinking failed")
          .register();

  public static final String DEFAULT_DIRECTORIES_INDEX_NAME = "directories.sqlite";
  protected static final String DIRECTORIES_INDEX_NAME_MEMORY = ":memory:";

  private final boolean execRootFallback;
  private final Map<Digest, DirectoryEntry> directoryStorage = Maps.newConcurrentMap();
  private final DirectoriesIndex directoriesIndex;
  private final String directoriesIndexDbName;
  private final LockMap locks = new LockMap();

  @Override
  public long directoryStorageCount() {
    return directoryStorage.size();
  }

  public LegacyDirectoryCFC(
      Path root,
      long maxSizeInBytes,
      long maxEntrySizeInBytes,
      int hexBucketLevels,
      boolean storeFileDirsIndexInMemory,
      boolean execRootFallback,
      ExecutorService expireService,
      Executor accessRecorder,
      ConcurrentMap<String, Entry> storage,
      String directoriesIndexDbName,
      FixedBufferPool zstdBufferPool,
      Consumer<Digest> onPut,
      Consumer<Iterable<Digest>> onExpire,
      @Nullable ContentAddressableStorage delegate,
      boolean delegateSkipLoad,
      InputStreamFactory externalInputStreamFactory) {
    super(
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
        externalInputStreamFactory);
    this.execRootFallback = execRootFallback;
    this.directoriesIndexDbName = directoriesIndexDbName;

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
        while (!locked.compareAndSet(/* expected= */ false, /* update= */ true)) {
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
      if (!locked.compareAndSet(/* expected= */ true, /* update= */ false)) {
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
  }

  @Override
  protected CacheLoadResults loadCache(
      Consumer<Digest> onStartPut, ExecutorService removeDirectoryService)
      throws IOException, InterruptedException {
    CacheLoadResults loadResults = super.loadCache(onStartPut, removeDirectoryService);

    log.log(Level.INFO, "Creating Index");
    directoriesIndex.start();
    log.log(Level.INFO, "Index Created");

    return loadResults;
  }

  @Override
  protected boolean shouldDeleteBranchFile(Path branchDir, String name) {
    boolean isRoot = branchDir.equals(getRoot());
    return !(isRoot && name.equals(directoriesIndexDbName))
        && super.shouldDeleteBranchFile(branchDir, name);
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  protected List<Path> computeDirectories(CacheScanResults cacheScanResults)
      throws InterruptedException {
    // create thread pool
    ExecutorService pool = BuildfarmExecutors.getComputeCachePool();

    ImmutableList.Builder<Path> invalidDirectories = new ImmutableList.Builder<>();

    for (Path path : cacheScanResults.computeDirs()) {
      DigestUtil digestUtil = parseDirectoryDigestUtil(path.getFileName().toString());
      pool.execute(
          () -> {
            try {
              ImmutableList.Builder<String> inputsBuilder = ImmutableList.builder();

              List<NamedFileKey> sortedDirent = listDirentSorted(path, fileStore);

              Directory directory =
                  computeDirectory(
                      digestUtil, path, sortedDirent, cacheScanResults.fileKeys(), inputsBuilder);

              Digest digest = directory == null ? null : digestUtil.compute(directory);

              Path dirPath = path;
              if (digest != null && getDirectoryPath(digest).equals(dirPath)) {
                DirectoryEntry e = new DirectoryEntry(directory, Deadline.after(10, HOURS));
                directoriesIndex.put(digest, inputsBuilder.build());
                directoryStorage.put(digest, e);
              } else {
                synchronized (invalidDirectories) {
                  invalidDirectories.add(dirPath);
                }
              }
            } catch (Exception e) {
              log.log(Level.SEVERE, "error processing directory " + path.toString(), e);
            }
          });
    }

    joinThreads(pool, "Populating Directories...");

    return invalidDirectories.build();
  }

  private Directory computeDirectory(
      DigestUtil digestUtil,
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
          Directory dir =
              computeDirectory(digestUtil, entryPath, childDirent, fileKeys, inputsBuilder);
          if (dir == null) {
            return null;
          }
          b.addDirectoriesBuilder()
              .setName(name)
              .setDigest(DigestUtil.toDigest(digestUtil.compute(dir)));
        } else if (isEmptyFile) {
          // empty file
          boolean isExecutable = isReadOnlyExecutable(entryPath, fileStore);
          b.addFilesBuilder()
              .setName(name)
              .setDigest(DigestUtil.toDigest(digestUtil.empty()))
              .setIsExecutable(isExecutable);
        } else {
          // non-empty file
          inputsBuilder.add(e.key);
          Digest digest;
          try {
            digest = CASFileCache.keyToDigest(e.key, e.size, digestUtil);
          } catch (NumberFormatException mismatchEx) {
            // inspire directory deletion for mismatched hash
            return null;
          }
          boolean isExecutable = e.key.endsWith("_exec");
          b.addFilesBuilder()
              .setName(name)
              .setDigest(DigestUtil.toDigest(digest))
              .setIsExecutable(isExecutable);
        }
      }
    }

    return b.build();
  }

  public synchronized void decrementReferences(
      Iterable<String> inputFiles,
      Iterable<build.bazel.remote.execution.v2.Digest> inputDirectories,
      DigestFunction.Value digestFunction)
      throws IOException, InterruptedException {
    try {
      decrementReferencesSynchronized(inputFiles, inputDirectories, digestFunction);
    } catch (ClosedByInterruptException e) {
      InterruptedException intEx = new InterruptedException();
      intEx.addSuppressed(e);
      throw intEx;
    }
  }

  @GuardedBy("this")
  private void decrementReferencesSynchronized(
      Iterable<String> inputFiles,
      Iterable<build.bazel.remote.execution.v2.Digest> inputDirectories,
      DigestFunction.Value digestFunction)
      throws IOException {
    // decrement references and notify if any dropped to 0
    // insert after the last 0-reference count entry in list
    int entriesDereferenced = decrementInputReferences(inputFiles);
    for (build.bazel.remote.execution.v2.Digest inputDirectory : inputDirectories) {
      DirectoryEntry dirEntry =
          directoryStorage.get(DigestUtil.fromDigest(inputDirectory, digestFunction));
      if (dirEntry == null) {
        throw new IllegalStateException(
            "inputDirectory "
                + DigestUtil.toString(DigestUtil.fromDigest(inputDirectory, digestFunction))
                + " is not in directoryStorage");
      }
      entriesDereferenced +=
          decrementInputReferences(
              directoriesIndex.directoryEntries(
                  DigestUtil.fromDigest(inputDirectory, digestFunction)));
    }
    if (entriesDereferenced > 0) {
      notify();
    }
  }

  @SuppressWarnings("NonAtomicOperationOnVolatileField")
  @Override
  protected synchronized List<ListenableFuture<Void>> unlinkAndExpireDirectories(
      Entry entry, ExecutorService service) {
    ImmutableList.Builder<ListenableFuture<Void>> builder = ImmutableList.builder();
    Iterable<Digest> containingDirectories;
    try {
      containingDirectories = directoriesIndex.removeEntry(entry.key);
    } catch (Exception e) {
      log.log(Level.SEVERE, "error removing entry " + entry.key + " from directoriesIndex", e);
      containingDirectories = ImmutableList.of();
    }
    for (Digest containingDirectory : containingDirectories) {
      builder.add(expireDirectory(containingDirectory, service));
    }

    // candidate for parent-only method

    entry.unlink();
    unreferencedEntryCount--;
    if (entry.referenceCount != 0) {
      log.severe("removed referenced entry " + entry.key);
    }
    return builder.build();
  }

  @GuardedBy("this")
  private ListenableFuture<Void> expireDirectory(Digest digest, ExecutorService service) {
    DirectoryEntry e = directoryStorage.remove(digest);
    if (e == null) {
      log.severe(
          format("CASFileCache::expireDirectory(%s) does not exist", DigestUtil.toString(digest)));
      return immediateFuture(null);
    }

    return Directories.remove(getDirectoryPath(digest), fileStore, service);
  }

  private void linkCachedFile(Path filePath, Path cacheFilePath) throws IOException {
    // = Hardlink Limitations =
    // Creating hardlinks is fast and saves space within the CAS.
    // However, some filesystems such as ext4 have a total hardlink limit of 65k for individual
    // files. Hitting this limit is easier than you think because the hardlinking occurs across
    // actions.  A recommended filesystem to back the CAS is XFS, due to its high link counts limits
    // per inode. If you are using a filesystem with low hardlink limits, this call will likely fail
    // with 'Too many links...`.

    try {
      Files.createLink(filePath, cacheFilePath);
    } catch (IOException e) {
      // propagate the exception if we do not want to perform the fallback strategy.
      // The client should expect a failed action with an explanation of 'Too many links...`.
      if (!execRootFallback) {
        throw e;
      }

      // = Fallback Strategy =
      // Buildfarm provides a configuration fallback that copies files in the event
      // that hardlinking fails.  If you are copying files more often than hardlinking,
      // you're performance may degrade significantly.  Therefore we provide a metric
      // signal to allow detection of this fallback.
      Files.copy(cacheFilePath, filePath, StandardCopyOption.REPLACE_EXISTING);
      casCopyFallbackMetric.inc();

      // TODO: A more optimal strategy would be to provide additional inodes
      // (i.e. one backing file for a 65k or smaller link count) as a strategy,
      // with pools of the same hash getting replicated.
    }
  }

  @Override
  public ListenableFuture<PathResult> putDirectory(
      Digest digest,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      ExecutorService service) {
    // Claim lock.
    // Claim the directory path so no other threads try to create/delete it.
    Path path = getDirectoryPath(digest);
    Lock l = locks.acquire(path);
    log.finer(format("locking directory %s", path.getFileName()));
    try {
      l.lockInterruptibly();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return immediateFailedFuture(e);
    }
    log.finer(format("locked directory %s", path.getFileName()));

    // Now that a lock has been claimed, we can proceed to create the directory.
    ListenableFuture<PathResult> putFuture;
    try {
      putFuture = putDirectorySynchronized(path, digest, directoriesIndex, service);
    } catch (IOException e) {
      putFuture = immediateFailedFuture(e);
    }

    // Release lock.
    putFuture.addListener(
        () -> {
          l.unlock();
          log.finer(format("directory %s has been unlocked", path.getFileName()));
        },
        service);
    return putFuture;
  }

  private boolean directoryExists(
      DigestFunction.Value digestFunction,
      Path path,
      Directory directory,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex) {
    if (!Files.exists(path)) {
      log.severe(format("directory path %s does not exist", path));
      return false;
    }
    for (FileNode fileNode : directory.getFilesList()) {
      Path filePath = path.resolve(fileNode.getName());
      if (!Files.exists(filePath)) {
        log.severe(format("directory file entry %s does not exist", filePath));
        return false;
      }
      // additional stat check to ensure that the cache entry exists for hard link inode match?
    }
    for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
      if (!directoryExists(
          digestFunction,
          path.resolve(directoryNode.getName()),
          directoriesIndex.get(directoryNode.getDigest()),
          directoriesIndex)) {
        return false;
      }
    }
    return true;
  }

  private boolean directoryEntryExists(
      DigestFunction.Value digestFunction,
      Path path,
      DirectoryEntry dirEntry,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex) {
    if (!dirEntry.existsDeadline.isExpired()) {
      return true;
    }

    if (directoryExists(digestFunction, path, dirEntry.directory, directoriesIndex)) {
      dirEntry.existsDeadline = Deadline.after(10, HOURS);
      return true;
    }
    return false;
  }

  @SuppressWarnings("ConstantConditions")
  private ListenableFuture<PathResult> putDirectorySynchronized(
      Path path,
      Digest digest,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesByDigest,
      ExecutorService service)
      throws IOException {
    log.finer(format("directory %s has been locked", path.getFileName()));
    ListenableFuture<Void> expireFuture;
    synchronized (this) {
      DirectoryEntry e = directoryStorage.get(digest);
      if (e == null) {
        expireFuture = immediateFuture(null);
      } else {
        ImmutableList.Builder<String> inputsBuilder = ImmutableList.builder();
        // this seems to be a point that would be great to not be synchronized on...
        for (String input : directoriesIndex.directoryEntries(digest)) {
          Entry fileEntry = storage.get(input);
          if (fileEntry == null) {
            log.severe(
                format(
                    "CASFileCache::putDirectory(%s) exists, but input %s does not, purging it with"
                        + " fire and resorting to fetch",
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
          log.finer(format("found existing entry for %s", path.getFileName()));
          if (directoryEntryExists(digest.getDigestFunction(), path, e, directoriesByDigest)) {
            return immediateFuture(new PathResult(path, /* missed= */ false));
          }
          log.severe(
              format(
                  "directory %s does not exist in cache, purging it with fire and resorting to"
                      + " fetch",
                  path.getFileName()));
        }

        decrementReferencesSynchronized(
            inputsBuilder.build(), ImmutableList.of(), digest.getDigestFunction());
        expireFuture = expireDirectory(digest, service);
        log.finer(format("expiring existing entry for %s", path.getFileName()));
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
              log.finer(format("expiry complete, fetching %s", path.getFileName()));
              ImmutableList.Builder<ListenableFuture<Path>> putFuturesBuilder =
                  ImmutableList.builder();
              fetchDirectory(
                  path,
                  digest,
                  directoriesByDigest,
                  (dst, src, size, isExecutable) -> {
                    linkCachedFile(dst, src);
                    // we saw null entries in the built immutable list without synchronization
                    synchronized (inputsBuilder) {
                      inputsBuilder.add(src.getFileName().toString());
                    }
                  },
                  putFuturesBuilder,
                  service);
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
                        } catch (ExecutionException e) {
                          failures.add(e.getCause());
                        } catch (Throwable t) {
                          // cancelled or interrupted during get
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
                disableAllWriteAccess(path, fileStore);
              } catch (IOException e) {
                log.log(Level.SEVERE, "error while disabling write permissions on " + path, e);
                return immediateFailedFuture(e);
              }
              try {
                directoriesIndex.put(digest, inputsBuilder.build());
              } catch (IOException e) {
                log.log(Level.SEVERE, "error while indexing " + path, e);
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
                  decrementReferencesSynchronized(
                      inputs, ImmutableList.of(), digest.getDigestFunction());
                } catch (IOException ioEx) {
                  e.addSuppressed(ioEx);
                }
              }
              try {
                log.log(Level.FINER, "removing directory to roll back " + path);
                Directories.remove(path, fileStore);
              } catch (IOException removeException) {
                log.log(
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
          log.finer(format("directory fetch complete, inserting %s", path.getFileName()));
          DirectoryEntry e =
              new DirectoryEntry(
                  // might want to have this treatment ahead of this
                  digest.getSize() == 0
                      ? Directory.getDefaultInstance()
                      : directoriesByDigest.get(DigestUtil.toDigest(digest)),
                  Deadline.after(10, HOURS));
          directoryStorage.put(digest, e);
          return new PathResult(path, /* missed= */ true);
        },
        service);
  }

  protected static class DirectoryEntry {
    public final Directory directory;
    Deadline existsDeadline;

    public DirectoryEntry(Directory directory, Deadline existsDeadline) {
      this.directory = directory;
      this.existsDeadline = existsDeadline;
    }
  }
}
