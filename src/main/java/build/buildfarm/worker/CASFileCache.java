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

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;

import build.buildfarm.common.ContentAddressableStorage;
import build.buildfarm.common.DigestUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.DirectoryNode;
import com.google.devtools.remoteexecution.v1test.FileNode;
import com.google.protobuf.ByteString;
import io.grpc.StatusRuntimeException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CASFileCache implements ContentAddressableStorage, InputStreamFactory, OutputStreamFactory {
  private final InputStreamFactory inputStreamFactory;
  private final Path root;
  private final long maxSizeInBytes;
  private final DigestUtil digestUtil;
  private final Map<Path, Entry> storage = new ConcurrentHashMap<>();
  private final Map<Digest, DirectoryEntry> directoryStorage = new HashMap<>();
  private final LockMap locks = new LockMap();
  private final Consumer<Digest> onPut;
  private final Consumer<Iterable<Digest>> onExpire;
  private final ListeningExecutorService removeDirectoryPool =
      listeningDecorator(
          Executors.newFixedThreadPool(
            /* nThreads=*/ 32,
            new ThreadFactoryBuilder().setNameFormat("remove-directory-pool-%d").build()));

  private transient long sizeInBytes = 0;
  private transient Entry header = new SentinelEntry();

  public CASFileCache(
      InputStreamFactory inputStreamFactory,
      Path root,
      long maxSizeInBytes,
      DigestUtil digestUtil) {
    this(
        inputStreamFactory,
        root, maxSizeInBytes,
        digestUtil,
        /* onPut=*/ (digest) -> {},
        /* onExpire=*/ (digests) -> {});
  }

  public CASFileCache(
      InputStreamFactory inputStreamFactory,
      Path root,
      long maxSizeInBytes,
      DigestUtil digestUtil,
      Consumer<Digest> onPut,
      Consumer<Iterable<Digest>> onExpire) {
    this.inputStreamFactory = inputStreamFactory;
    this.root = root;
    this.maxSizeInBytes = maxSizeInBytes;
    this.digestUtil = digestUtil;
    this.onPut = onPut;
    this.onExpire = onExpire;

    header.before = header.after = header;
  }

  public static <T> T getOrIOException(ListenableFuture<T> future) throws IOException {
    try {
      return future.get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new UncheckedExecutionException(e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
  }

  private static Digest keyToDigest(Path key, DigestUtil digestUtil) throws NumberFormatException {
    String fileName = key.getFileName().toString();
    String[] components = fileName.split("_");

    String hashComponent = components[0];
    String sizeComponent = components[1];
    int parsedSizeComponent = Integer.parseInt(sizeComponent);

    return digestUtil.build(hashComponent, parsedSizeComponent);
  }

  private static <T> T parseFileEntryKey(String fileName, long size, DigestUtil digestUtil, BiFunction<Digest, Boolean, T> onKey) {
    String[] components = fileName.split("_");
    if (components.length < 2 || components.length > 3) {
      return null;
    }

    boolean isExecutable = false;
    Digest digest;
    try {
      String sizeComponent = components[1];
      int parsedSizeComponent = Integer.parseInt(sizeComponent);

      if (parsedSizeComponent != size) {
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

  private boolean contains(Digest digest, boolean isExecutable) throws IOException {
    Path key = getKey(digest, isExecutable);
    Lock l = locks.acquire(key);
    if (l.tryLock()) {
      try {
        Entry e = storage.get(key);
        if (e == null) {
          return false;
        }
        synchronized (this) {
          if (!entryExists(e)) {
            Entry removedEntry = storage.remove(key);
            if (removedEntry != null) {
              unlinkEntry(removedEntry);
            }
            return false;
          }
          e.recordAccess(header);
          return true;
        }
      } finally {
        locks.release(key);
        l.unlock();
      }
    } else {
      // can't preserve the integrity, but if another of the contains has the lock, it is doing so
      return storage.get(key) != null;
    }
  }

  private boolean entryExists(Entry e) {
    long now = System.nanoTime();
    if (now < e.ttl) {
      return true;
    }

    if (Files.exists(e.key)) {
      // exists check is good for 10s
      e.ttl = now + 10000000000l;
      return true;
    }
    return false;
  }

  @Override
  public boolean contains(Digest digest) {
    try {
      /* maybe swap the order here if we're higher in ratio on one side */
      return contains(digest, false) || contains(digest, true);
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }
  }

  private InputStream newInput(Digest digest, long offset, boolean isExecutable) throws IOException, InterruptedException {
    try {
      Path blobPath = put(digest, isExecutable, /* containingDirectory=*/ null);
      try {
        InputStream input = Files.newInputStream(blobPath);
        input.skip(offset);
        return input;
      } finally {
        decrementReferences(ImmutableList.<Path>of(blobPath), ImmutableList.<Digest>of());
      }
    } catch (NoSuchFileException e) {
      remove(digest, isExecutable);
      throw e;
    } catch (IOException e) {
      if (!e.getMessage().equals("file not found")) {
        e.printStackTrace();
      }
      remove(digest, isExecutable);
      throw e;
    }
  }

  @Override
  public InputStream newInput(Digest digest, long offset) throws IOException, InterruptedException {
    boolean isExecutable = false;
    do {
      Path key = getKey(digest, isExecutable);
      Entry e = storage.get(key);
      if (e != null) {
        InputStream input = newInput(digest, offset, isExecutable);
        if (input != null) {
          synchronized (this) {
            e.recordAccess(header);
          }
        }
        return input;
      }
      isExecutable = !isExecutable;
    } while (isExecutable != false);
    throw new NoSuchFileException(DigestUtil.toString(digest));
  }

  @Override
  public Blob get(Digest digest) {
    int retries = 5;
    while (retries > 0) {
      try {
        try (InputStream input = newInput(digest, 0)) {
          ByteString content = ByteString.readFrom(input);
          return new Blob(content, digestUtil);
        }
      } catch (IOException e) {
        e.printStackTrace();
        retries--;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return null;
      }
    }
    System.err.println(String.format("CASFileCache::get(%s): exceeded IOException retry", DigestUtil.toString(digest)));
    return null;
  }

  @Override
  public void put(Blob blob) {
    Path blobPath = getKey(blob.getDigest(), false);
    try {
      OutputStream out =
          putImpl(
              blobPath,
              blob.getDigest().getSizeBytes(),
              /* isExecutable=*/ false,
              null,
              () -> onPut.accept(blob.getDigest()));
      try {
        if (out != null) {
          blob.getData().writeTo(out);
        }
      } finally {
        if (out != null) {
          out.close();
        }
      }

      decrementReferences(ImmutableList.<Path>of(blobPath), ImmutableList.<Digest>of());
    } catch (IOException e) {
      /* unlikely, our stream comes from the blob */
      e.printStackTrace();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public OutputStream newOutput(Digest digest) throws IOException {
    Path blobPath = getKey(digest, false);
    final OutputStream out;
    try {
      out = putImpl(
          blobPath,
          digest.getSizeBytes(),
          /* isExecutable=*/ false,
          null,
          () -> onPut.accept(digest));
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    if (out == null) {
      decrementReferences(ImmutableList.<Path>of(blobPath), ImmutableList.<Digest>of());
      return null;
    }
    return new OutputStream() {
      @Override
      public void close() throws IOException {
        out.close();

        decrementReferences(ImmutableList.<Path>of(blobPath), ImmutableList.<Digest>of());
      }

      @Override
      public void flush() throws IOException {
        out.flush();
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
      public void write(int b) throws IOException {
        out.write(b);
      }
    };
  }

  @Override
  public void put(Blob blob, Runnable onExpiration) {
    throw new UnsupportedOperationException();
  }

  private static final class LockMap {
    private final Map<Path, Lock> mutexes = new HashMap<>();

    private synchronized Lock acquire(Path key) {
      Lock mutex = mutexes.get(key);
      if (mutex == null) {
        mutex = new ReentrantLock();
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
    private final Digest digest;

    FileEntryKey(Path key, Digest digest) {
      this.key = key;
      this.digest = digest;
    }

    Path getKey() {
      return key;
    }

    Digest getDigest() {
      return digest;
    }
  }

  public void start() throws IOException, InterruptedException {
    start(onPut);
  }

  public void stop() {
    removeDirectoryPool.shutdown();
    while (!removeDirectoryPool.isTerminated()) {
      try {
        if (!removeDirectoryPool.awaitTermination(60, TimeUnit.SECONDS)) {
          removeDirectoryPool.shutdownNow();
        }
      } catch (InterruptedException e) {
        removeDirectoryPool.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }

  private Directory computeDirectory(
      Path path,
      Map<Object, Entry> fileKeys,
      ImmutableList.Builder<Path> inputsBuilder) {
    Directory.Builder b = Directory.newBuilder();

    List<Dirent> sortedDirent;
    try {
      sortedDirent = UploadManifest.readdir(path, /* followSymlinks= */ false);
    } catch (IOException e) {
      e.printStackTrace();
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
        inputsBuilder.add(e.key);
        Digest digest = CASFileCache.keyToDigest(e.key, digestUtil);
        b.addFilesBuilder().setName(name).setDigest(digest).setIsExecutable(Files.isExecutable(child));
      } else {
        return null;
      }
    }

    return b.build();
  }

  /**
   * initialize the cache for persistent storage and inject any
   * consistent entries which already exist under the root into
   * the storage map. This call will create the root if it does
   * not exist, and will scale in cost with the number of files
   * already present.
   */
  public void start(Consumer<Digest> onPut) throws IOException, InterruptedException {
    Files.createDirectories(root);

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
        if (sizeInBytes + size > maxSizeInBytes) {
          Files.delete(file);
        } else {
          FileEntryKey fileEntryKey = null;
          if (file.getParent().equals(root)) {
            fileEntryKey = parseFileEntryKey(
                file.getFileName().toString(),
                size,
                digestUtil,
                (digest, isExecutable) -> new FileEntryKey(getKey(digest, isExecutable), digest));
          }
          if (fileEntryKey != null) {
            Path key = fileEntryKey.getKey();
            Lock l = locks.acquire(key);
            l.lock();
            try {
              if (storage.get(key) == null) {
                long now = System.nanoTime();
                Entry e = new Entry(key, size, null, now + 10000000000l);
                fileKeysBuilder.put(attrs.fileKey(), e);
                storage.put(e.key, e);
                onPut.accept(fileEntryKey.getDigest());
                synchronized (this) {
                  e.decrementReference(header);
                }
                sizeInBytes += size;
              }
            } finally {
              locks.release(key);
              l.unlock();
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
          List<Path> inputs = inputsBuilder.build();
          DirectoryEntry e = new DirectoryEntry(directory, inputs);
          synchronized (this) {
            directoryStorage.put(digest, e);
            for (Path input : inputs) {
              Entry entry = storage.get(input);
              entry.containingDirectories.add(digest);
            }
          }
        } else {
          invalidDirectories.add(path);
        }
      });
    }
    pool.shutdown();
    while (!pool.isTerminated()) {
      System.out.println("Waiting for directory population to complete");
      pool.awaitTermination(60, TimeUnit.SECONDS);
    }

    for (Path path : invalidDirectories.build()) {
      removeDirectoryAsync(path);
    }
  }

  public ListenableFuture<Void> removeDirectoryAsync(Path path) throws IOException {
    String suffix = UUID.randomUUID().toString();
    Path filename = path.getFileName();
    String tmpFilename = filename + ".tmp." + suffix;
    Path tmpPath = path.resolveSibling(tmpFilename);
    Files.move(path, tmpPath);
    return removeDirectoryPool.submit(() ->  {
      try {
        removeDirectory(tmpPath);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }, null);
  }

  private static String digestFilename(Digest digest) {
    return String.format("%s_%d", digest.getHash(), digest.getSizeBytes());
  }

  public static String getFileName(Digest digest, boolean isExecutable) {
    return String.format(
        "%s%s",
        digestFilename(digest),
        (isExecutable ? "_exec" : ""));
  }

  public Path getKey(Digest digest, boolean isExecutable) {
    return getPath(getFileName(digest, isExecutable));
  }

  private void decrementReferencesSynchronized(Iterable<Path> inputFiles, Iterable<Digest> inputDirectories) {
    // decrement references and notify if any dropped to 0
    // insert after the last 0-reference count entry in list
    boolean entriesMadeAvailable = false;

    for (Digest inputDirectory : inputDirectories) {
      DirectoryEntry dirEntry = directoryStorage.get(inputDirectory);
      if (dirEntry == null) {
        throw new IllegalStateException("inputDirectory " + DigestUtil.toString(inputDirectory) + " is not in directoryStorage");
      }
      inputFiles = Iterables.concat(inputFiles, dirEntry.inputs);
    }

    for (Path input : inputFiles) {
      Entry e = storage.get(input);
      if (e == null) {
        throw new IllegalStateException(input + " has been removed with references");
      }
      if (!e.key.equals(input)) {
        throw new RuntimeException("ERROR: entry retrieved: " + e.key + " != " + input);
      }
      e.decrementReference(header);
      if (e.referenceCount == 0) {
        entriesMadeAvailable = true;
      }
    }
    if (entriesMadeAvailable) {
      notify();
    }
  }

  public synchronized void decrementReferences(Iterable<Path> inputFiles, Iterable<Digest> inputDirectories) {
    decrementReferencesSynchronized(inputFiles, inputDirectories);
  }

  public Path getRoot() {
    return root;
  }

  public Path getPath(String filename) {
    return root.resolve(filename);
  }

  private Path getDirectoryPath(Digest digest) {
    return root.resolve(digestFilename(digest) + "_dir");
  }

  private void remove(Digest digest, boolean isExecutable) throws IOException {
    Path key = getKey(digest, isExecutable);
    Lock l = locks.acquire(key);
    l.lock();
    try {
      Entry e = storage.remove(key);
      if (e != null) {
        synchronized (this) {
          unlinkEntry(e);
        }
      }
    } finally {
      locks.release(key);
      l.unlock();
    }
  }

  /** must be called in synchronized context */
  private void unlinkEntry(Entry e) throws IOException {
    if (e.referenceCount == 0) {
      e.unlink();
    } else {
      System.out.println(String.format("CASFileCache::unlinkEntry(%s): Removed referenced entry!", e.key));
    }
    for (Digest containingDirectory : e.containingDirectories) {
      getOrIOException(expireDirectory(containingDirectory));
    }
    // still debating this one being in this method
    sizeInBytes -= e.size;
    // technically we should attempt to remove the file here,
    // but we're only called in contexts where it doesn't exist...
  }

  /** must be called in synchronized context */
  private ListenableFuture<Path> expireEntry(long blobSizeInBytes) throws IOException, InterruptedException {
    while (header.after == header) {
      int references = 0;
      int keys = 0;
      int min = -1, max = 0;
      Path minkey = null, maxkey = null;
      System.out.println(String.format("CASFileCache::expireEntry(%d) header(%s): { after: %s, before: %s }", blobSizeInBytes, header.hashCode(), header.after.hashCode(), header.before.hashCode()));
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
          System.out.println(String.format("CASFileCache::expireEntry(%d) unreferenced entry(%s): { after: %s, before: %s }",
                blobSizeInBytes, e.hashCode(), e.after == null ? null : e.after.hashCode(), e.before == null ? null : e.before.hashCode()));
        }
        references += e.referenceCount;
        keys++;
      }
      if (keys == 0) {
        throw new IllegalStateException("CASFileCache::expireEntry(" + blobSizeInBytes + ") there are no keys to wait for expiration on");
      }
      System.out.println(String.format(
          "CASFileCache::expireEntry(%d) unreferenced list is empty, %d bytes, %d keys with %d references, min(%d, %s), max(%d, %s)",
          blobSizeInBytes, sizeInBytes, keys, references, min, minkey, max, maxkey));
      wait();
    }
    Entry e = header.after;
    if (e.referenceCount != 0) {
      throw new RuntimeException("ERROR: Reference counts lru ordering has not been maintained correctly, attempting to expire referenced (or negatively counted) content");
    }
    storage.remove(e.key);
    ImmutableList.Builder<ListenableFuture<Void>> directoryExpirationFutures = new ImmutableList.Builder<>();
    for (Digest containingDirectory : e.containingDirectories) {
      directoryExpirationFutures.add(expireDirectory(containingDirectory));
    }
    e.unlink();
    sizeInBytes -= e.size;
    return transform(allAsList(directoryExpirationFutures.build()), (result) -> e.key);
  }

  @VisibleForTesting
  public static void removeDirectory(Path directory) throws IOException {
    Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
        if (e == null) {
          Files.delete(dir);
          return FileVisitResult.CONTINUE;
        }
        // directory iteration failed
        throw e;
      }
    });
  }

  /** must be called in synchronized context */
  private void purgeDirectoryFromInputs(Digest digest, Iterable<Path> inputs) {
    for (Path input : inputs) {
      Entry fileEntry = storage.get(input);

      if (fileEntry != null) {
        fileEntry.containingDirectories.remove(digest);
      }
    }
  }

  /** must be called in synchronized context */
  private ListenableFuture<Void> expireDirectory(Digest digest) throws IOException {
    DirectoryEntry e = directoryStorage.remove(digest);
    if (e == null) {
      System.err.println("CASFileCache::expireDirectory(" + DigestUtil.toString(digest) + ") does not exist");
      return immediateFuture(null);
    }

    purgeDirectoryFromInputs(digest, e.inputs);
    return removeDirectoryAsync(getDirectoryPath(digest));
  }

  /** must be called in synchronized context */
  private void fetchDirectory(
      Digest containingDirectory,
      Path path,
      Digest digest,
      Map<Digest, Directory> directoriesIndex,
      ImmutableList.Builder<Path> inputsBuilder) throws IOException, InterruptedException {
    if (Files.exists(path)) {
      if (Files.isDirectory(path)) {
        getOrIOException(removeDirectoryAsync(path));
      } else {
        Files.delete(path);
      }
    }
    Files.createDirectory(path);
    Directory directory = directoriesIndex.get(digest);
    if (directory == null) {
      throw new IOException("directory not found");
    }
    for (FileNode fileNode : directory.getFilesList()) {
      if (fileNode.getDigest().getSizeBytes() == 0) {
        Files.createFile(path.resolve(fileNode.getName()));
      } else {
        Path fileCacheKey = put(fileNode.getDigest(), fileNode.getIsExecutable(), containingDirectory);
        // FIXME this can die with 'too many links'... needs some cascading fallout
        Files.createLink(path.resolve(fileNode.getName()), fileCacheKey);
        inputsBuilder.add(fileCacheKey);
      }
    }
    for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
      fetchDirectory(
          containingDirectory,
          path.resolve(directoryNode.getName()),
          directoryNode.getDigest(),
          directoriesIndex,
          inputsBuilder);
    }
  }

  public Path putDirectory(
      Digest digest,
      Map<Digest, Directory> directoriesIndex) throws IOException, InterruptedException {
    Path path = getDirectoryPath(digest);

    synchronized (this) {
      DirectoryEntry e = directoryStorage.get(digest);
      if (e != null) {
        ImmutableList.Builder<Path> inputsBuilder = new ImmutableList.Builder<>();
        for (Path input : e.inputs) {
          Entry fileEntry = storage.get(input);
          if (fileEntry == null) {
            System.err.println("CASFileCache::putDirectory(" + DigestUtil.toString(digest) + ") exists, but input " + input + " does not, purging it with fire and resorting to fetch");
            e = null;
            break;
          }
          fileEntry.incrementReference();
          inputsBuilder.add(input);
        }

        if (e != null) {
          return path;
        }

        decrementReferencesSynchronized(inputsBuilder.build(), ImmutableList.<Digest>of());
        getOrIOException(expireDirectory(digest));
      }
    }

    ImmutableList.Builder<Path> inputsBuilder = new ImmutableList.Builder<>();
    boolean fetched = false;
    try {
      fetchDirectory(digest, path, digest, directoriesIndex, inputsBuilder);
      fetched = true;
    } finally {
      if (!fetched) {
        ImmutableList<Path> inputs = inputsBuilder.build();
        synchronized (this) {
          purgeDirectoryFromInputs(digest, inputs);
          decrementReferencesSynchronized(inputs, ImmutableList.<Digest>of());
        }
        removeDirectoryAsync(path);
      }
    }

    DirectoryEntry e = new DirectoryEntry(directoriesIndex.get(digest), inputsBuilder.build());

    synchronized (this) {
      directoryStorage.put(digest, e);
    }

    return path;
  }

  public Path put(Digest digest, boolean isExecutable, Digest containingDirectory) throws IOException, InterruptedException {
    Path key = getKey(digest, isExecutable);
    OutputStream out = putImpl(
        key,
        digest.getSizeBytes(),
        isExecutable,
        containingDirectory,
        () -> onPut.accept(digest));
    // already has it
    if (out != null) {
      try (InputStream in = inputStreamFactory.newInput(digest, 0)) {
        ByteStreams.copy(in, out);
      } catch (IOException e) {
        e.printStackTrace(); // prevent burial by early end of stream during close
        throw e;
      } finally {
        try {
          out.close();
        } catch (IOException e) {
          if (Thread.interrupted()) {
            if (e.getCause() instanceof InterruptedException) {
              throw (InterruptedException) e.getCause();
            }
            throw new InterruptedException();
          }
          throw e;
        }
      }
    }
    return key;
  }

  @FunctionalInterface
  interface InputStreamSupplier {
    InputStream newInput() throws IOException, InterruptedException;
  }

  private static final OutputStream DUPLICATE_OUTPUT_STREAM = new OutputStream() {
    @Override
    public void write(int b) {
    }
  };

  private OutputStream putImpl(
      Path key,
      long blobSizeInBytes,
      boolean isExecutable,
      Digest containingDirectory,
      Runnable onInsert)
      throws IOException, InterruptedException {
    // uhhh, should we just serialize all of these per key with a single executor?
    ThreadFactory factory = new ThreadFactoryBuilder()
        .setNameFormat(String.format("blob-%s-lock-manager", key.getFileName()))
        .build();
    ListeningExecutorService service = listeningDecorator(Executors.newSingleThreadExecutor(factory));
    Lock l = locks.acquire(key);
    ListenableFuture<OutputStream> future = service.submit(() -> {
      final OutputStream out;
      boolean outIsSet = false;
      l.lock();
      try {
        out = putImplSynchronized(
            key,
            blobSizeInBytes,
            isExecutable,
            containingDirectory,
            onInsert);
        outIsSet = true;
      } finally {
        if (!outIsSet) {
          locks.release(key);
          l.unlock();
        }
      }
      return out;
    });
    final OutputStream out;
    try {
      out = future.get();
    } catch (ExecutionException e) {
      service.shutdownNow();
      service.awaitTermination(10, TimeUnit.MINUTES);

      Throwable cause = e.getCause();
      while (cause != null) {
        if (cause instanceof IOException) {
          throw (IOException) cause;
        }
        if (cause instanceof InterruptedException) {
          throw (InterruptedException) cause;
        }
        cause = cause.getCause();
      }
      throw new UncheckedExecutionException(e);
    } catch (InterruptedException e) {
      service.shutdownNow();
      service.awaitTermination(10, TimeUnit.MINUTES);
      throw e;
    }
    if (out == DUPLICATE_OUTPUT_STREAM) {
      service.execute(() -> {
        locks.release(key);
        l.unlock();
      });
      service.shutdown();
      try {
        service.awaitTermination(10, TimeUnit.MINUTES);
      } finally {
        if (!service.isTerminated()) {
          service.shutdownNow();
        }
      }
      return null;
    }
    return new OutputStream() {
      @Override
      public void close() throws IOException {
        try {
          out.close();
        } finally {
          service.execute(() -> {
            locks.release(key);
            l.unlock();
          });
          service.shutdown();
          try {
            service.awaitTermination(10, TimeUnit.MINUTES);
          } catch (InterruptedException e) {
            service.shutdownNow();

            Thread.currentThread().interrupt();
            throw new IOException(e);
          }
        }
      }

      @Override
      public void flush() throws IOException {
        out.flush();
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
      public void write(int b) throws IOException {
        out.write(b);
      }
    };
  }

  // must have key locked
  private OutputStream putImplSynchronized(
      Path key,
      long blobSizeInBytes,
      boolean isExecutable,
      Digest containingDirectory,
      Runnable onInsert)
      throws IOException, InterruptedException {
    ImmutableList.Builder<ListenableFuture<Path>> expiredKeysFutures = null;

    synchronized (this) {
      Entry e = storage.get(key);
      if (e != null && !entryExists(e)) {
        Entry removedEntry = storage.remove(key);
        if (removedEntry != null) {
          unlinkEntry(removedEntry);
        }
        e = null;
      }

      if (e != null) {
        if (containingDirectory != null) {
          e.containingDirectories.add(containingDirectory);
        }
        e.incrementReference();
        return DUPLICATE_OUTPUT_STREAM;
      }

      sizeInBytes += blobSizeInBytes;

      while (sizeInBytes > maxSizeInBytes) {
        if (expiredKeysFutures == null) {
          expiredKeysFutures = new ImmutableList.Builder<ListenableFuture<Path>>();
        }
        expiredKeysFutures.add(expireEntry(blobSizeInBytes));
      }
    }

    if (expiredKeysFutures != null) {
      ListenableFuture<List<Path>> expiredKeys = allAsList(expiredKeysFutures.build());
      ImmutableList.Builder<Digest> expiredDigests = new ImmutableList.Builder<>();
      for (Path expiredKey : getOrIOException(expiredKeys)) {
        try {
          Files.delete(expiredKey);
        } catch (NoSuchFileException e) {
          System.err.println("CASFileCache::putImpl: expired key " + expiredKey.toString() + " did not exist to delete");
        }

        try {
          Digest digest = CASFileCache.keyToDigest(expiredKey, digestUtil);
          if (!contains(digest)) {
            expiredDigests.add(digest);
          }
        } catch (NumberFormatException e) {
          e.printStackTrace();
        }
      }
      onExpire.accept(expiredDigests.build());
    }

    Path tmpPath = key.resolveSibling(key.getFileName() + ".tmp");
    return new OutputStream() {
      OutputStream tmpFile = Files.newOutputStream(tmpPath, CREATE, TRUNCATE_EXISTING);
      long size = 0;

      @Override
      public void flush() throws IOException {
        tmpFile.flush();
      }

      @Override
      public void write(byte[] b) throws IOException {
        tmpFile.write(b);
        size += b.length;
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        tmpFile.write(b, off, len);
        size += len;
      }

      @Override
      public void write(int b) throws IOException {
        tmpFile.write(b);
        size++;
      }

      @Override
      public void close() throws IOException {
        tmpFile.close();

        if (size != blobSizeInBytes) {
          Files.delete(tmpPath);
          throw new IOException("blob digest size mismatch, expected " + blobSizeInBytes + ", was " + size);
        }

        setPermissions(tmpPath, isExecutable);
        Files.move(tmpPath, key, REPLACE_EXISTING);

        Entry e = new Entry(key, blobSizeInBytes, containingDirectory, System.nanoTime() + 10000000000l);

        if (storage.put(key, e) != null) {
          throw new IllegalStateException("storage conflict with existing key for " + key);
        }
        onInsert.run();
      }
    };
  }

  private static void setPermissions(Path path, boolean isExecutable) throws IOException {
    new File(path.toString()).setExecutable(isExecutable, true);
  }

  private static class Entry {
    Entry before, after;
    final Path key;
    final long size;
    final Set<Digest> containingDirectories;
    int referenceCount;
    long ttl;

    private Entry() {
      key = null;
      size = -1;
      containingDirectories = null;
      referenceCount = -1;
      ttl = -1;
    }

    public Entry(Path key, long size, Digest containingDirectory, long ttl) {
      this.key = key;
      this.size = size;
      referenceCount = 1;
      containingDirectories = new HashSet<>();
      if (containingDirectory != null) {
        containingDirectories.add(containingDirectory);
      }
      this.ttl = ttl;
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
    // FIXME we need to do a periodic sweep here to see that the filesystem has not degraded for each directory...

    public DirectoryEntry(Directory directory, Iterable<Path> inputs) {
      this.directory = directory;
      this.inputs = inputs;
    }
  }
}
