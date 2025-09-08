// Copyright 2024 The Buildfarm Authors. All rights reserved.
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
import static build.buildfarm.common.io.EvenMoreFiles.setReadOnlyPerms;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.catchingAsync;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.successfulAsList;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.concurrent.TimeUnit.HOURS;

import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.common.BuildfarmExecutors;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.common.ZstdDecompressingOutputStream.FixedBufferPool;
import build.buildfarm.common.io.Directories;
import build.buildfarm.v1test.Digest;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.grpc.Deadline;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.logging.Level;
import javax.annotation.Nullable;
import lombok.extern.java.Log;

@Log
public class DirectoryEntryCFC extends CASFileCache {
  private final Cache<Digest, ListenableFuture<Void>> fetchers = CacheBuilder.newBuilder().build();

  public DirectoryEntryCFC(
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
  }

  private void computeDirectory(Path path, ImmutableList.Builder<Path> invalidDirectories) {
    String key = path.getFileName().toString();
    try {
      AtomicLong blobSizeInBytes = new AtomicLong();
      Files.walkFileTree(
          path,
          new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
              if (attrs.isRegularFile()) {
                blobSizeInBytes.addAndGet(attrs.size());
              }
              return FileVisitResult.CONTINUE;
            }
          });
      Entry e = new Entry(key, blobSizeInBytes.get(), Deadline.after(10, HOURS));
      // this is now a little gross
      if (sizeInBytes + e.size > maxSizeInBytes || e.size > maxEntrySizeInBytes || e.size == 0) {
        synchronized (invalidDirectories) {
          invalidDirectories.add(path);
        }
      } else {
        storage.put(key, e);
        synchronized (this) {
          if (e.decrementReference(header)) {
            unreferencedEntryCount++;
          }
        }
        sizeInBytes += e.size;
      }
    } catch (Exception e) {
      log.log(Level.SEVERE, "error processing directory " + path.toString(), e);
      synchronized (invalidDirectories) {
        invalidDirectories.add(path);
      }
    }
  }

  @Override
  protected List<Path> computeDirectories(CacheScanResults cacheScanResults)
      throws InterruptedException {
    // create thread pool
    ExecutorService pool = BuildfarmExecutors.getComputeCachePool();

    ImmutableList.Builder<Path> invalidDirectories = new ImmutableList.Builder<>();

    for (Path path : cacheScanResults.computeDirs()) {
      pool.execute(() -> computeDirectory(path, invalidDirectories));
    }

    joinThreads(pool, "Populating Directories...");

    return invalidDirectories.build();
  }

  @Override
  protected void deleteExpiredKey(String expiredKey) throws IOException {
    if (expiredKey.endsWith("_dir")) {
      Path path = getRemovingPath(expiredKey);
      Directories.remove(path, fileStore);
      // accounting for expiration metric?
    } else {
      super.deleteExpiredKey(expiredKey);
    }
  }

  private void copyLocalFileAndDereference(Path dst, Path src, boolean isExecutable)
      throws IOException {
    // consider charging here, maybe we deserve a digest size...
    try (InputStream in = Files.newInputStream(src)) {
      try (OutputStream out = Files.newOutputStream(dst)) {
        ByteStreams.copy(in, out);
      }
      setReadOnlyPerms(dst, isExecutable, fileStore);
    } finally {
      decrementReference(src.getFileName().toString());
    }
  }

  private <T> T getCompleted(ListenableFuture<? extends T> future) {
    checkState(future.isDone());
    try {
      return future.get();
    } catch (ExecutionException e) {
      // must not happen
      throw new UncheckedExecutionException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private ListenableFuture<Void> add(
      Path path,
      Digest digest,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      ExecutorService service) {
    ListenableFuture<Long> fetched = fetch(path, digest, directoriesIndex, service);
    ListenableFuture<Void> limited =
        transformAsync(
            fetched,
            weight -> {
              try {
                disableAllWriteAccess(path, fileStore);
              } catch (IOException e) {
                return immediateFailedFuture(e);
              }
              return immediateFuture(null);
            },
            service);
    ListenableFuture<Void> rolled =
        catchingAsync(
            limited,
            Throwable.class,
            e -> {
              try {
                Directories.remove(path, fileStore);
              } catch (IOException removeException) {
                e.addSuppressed(removeException);
              }
              return immediateFailedFuture(e);
            },
            service);
    return transformAsync(
        rolled,
        result -> {
          String key = path.getFileName().toString();
          long blobSizeInBytes = getCompleted(fetched);

          // might be able to clean this call up, need the expiration, but not the boolean
          // consider the file size being too large for the cas
          // consider just calling a safe 'charge' during the enumeration of the size
          // ... since we're consuming the size anyway, but then we have to worry about rolling the
          // partial charge back
          // ... or we just compute early and charge then, though we run the risk of evicting useful
          // blobs for this fetch
          try {
            checkState(charge(key, blobSizeInBytes, new AtomicBoolean()), true);
          } catch (IOException e) {
            return immediateFailedFuture(e);
          }
          Entry e = new Entry(key, blobSizeInBytes, Deadline.after(10, HOURS));
          safeStorageInsertion(key, e);
          fetchers.invalidate(digest);
          return immediateFuture(result);
        },
        service);
  }

  private ListenableFuture<Long> fetch(
      Path path,
      Digest digest,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      ExecutorService service) {
    ImmutableList.Builder<ListenableFuture<Path>> putFuturesBuilder = ImmutableList.builder();
    AtomicLong weight = new AtomicLong();
    try {
      fetchDirectory(
          path,
          digest,
          directoriesIndex,
          (dst, src, size, isExecutable) -> {
            copyLocalFileAndDereference(dst, src, isExecutable);
            weight.addAndGet(size);
          },
          putFuturesBuilder,
          service);
    } catch (Exception e) {
      return immediateFailedFuture(e);
    }
    ImmutableList<ListenableFuture<Path>> putFutures = putFuturesBuilder.build();

    // is there a better wrapper for this?
    // should probably destroy the directory on failure
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
            return immediateFailedFuture(new PutDirectoryException(path, digest, failures.build()));
          }
          return immediateFuture(weight.get());
        },
        service);
  }

  public ListenableFuture<PathResult> putDirectory(
      Digest digest,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      ExecutorService service) {
    Path path = getDirectoryPath(digest);
    String key = path.getFileName().toString();
    try {
      if (referenceIfExists(key)) {
        return immediateFuture(new PathResult(path, false));
      }
    } catch (IOException e) {
      return immediateFailedFuture(e);
    }
    // we must acquire a reference to this entry before delivering it to the client
    // unfortunately that means that it will be placed onto the LRU queue briefly
    // we could probably retry the whole procedure in that case, but maybe there is
    // simply too much contention at that point
    //
    // to correct this, we could find a way to identify the fetching thread and perform
    // the dereference only after all of the callbacks have been satisfied. Very funky
    // future stuff would need to be in place.
    //
    // we can play games with the immediacy of the callback though
    ListenableFuture<Void> fetch;
    // true if we were the creator of the fetch
    AtomicBoolean owner = new AtomicBoolean();
    try {
      fetch =
          fetchers.get(
              digest,
              () -> {
                owner.set(true);
                return add(path, digest, directoriesIndex, service);
              });
    } catch (ExecutionException e) {
      return immediateFailedFuture(e);
    }
    // we wil need to increment the reference count, as we did not call the future creator
    if (!owner.get()) {
      fetch =
          transformAsync(
              fetch,
              result -> {
                // completely unacceptable if it does not exist or is not available for reference
                // here
                try {
                  checkState(referenceIfExists(key));
                } catch (IOException e) {
                  return immediateFailedFuture(e);
                }
                return immediateFuture(result);
              },
              directExecutor());
    }
    return transform(fetch, result -> new PathResult(path, true), directExecutor());
  }

  @Override
  public synchronized void decrementReferences(
      Iterable<String> inputFiles,
      Iterable<build.bazel.remote.execution.v2.Digest> inputDirectories,
      DigestFunction.Value digestFunction)
      throws IOException {
    Iterable<String> directoryDigests =
        Iterables.transform(
            inputDirectories,
            digest -> getDirectoryKey(DigestUtil.fromDigest(digest, digestFunction)));
    // decrement references and notify if any dropped to 0
    // insert after the last 0-reference count entry in list
    inputFiles = Iterables.concat(inputFiles, directoryDigests);
    if (decrementInputReferences(inputFiles) > 0) {
      notify();
    }
    // this is very funky as is, it smells like something we should elevate to parent
    // even funkier with the synchronized requirement
  }

  @Override
  protected synchronized List<ListenableFuture<Void>> unlinkAndExpireDirectories(
      Entry entry, ExecutorService service) {
    entry.unlink();
    unreferencedEntryCount--;
    if (entry.referenceCount != 0) {
      log.log(Level.SEVERE, "removed referenced entry " + entry.key);
    }
    // we have no expiration that can be triggered by an entry
    return ImmutableList.of(immediateFuture(null));
  }
}
