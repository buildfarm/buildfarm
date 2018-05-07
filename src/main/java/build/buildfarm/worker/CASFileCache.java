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

import build.buildfarm.common.ContentAddressableStorage;
import build.buildfarm.common.DigestUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.DirectoryNode;
import com.google.devtools.remoteexecution.v1test.FileNode;
import com.google.protobuf.ByteString;
import io.grpc.StatusRuntimeException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.concurrent.ConcurrentHashMap;

public class CASFileCache implements ContentAddressableStorage {
  private final InputStreamFactory inputStreamFactory;
  private final Path root;
  private final long maxSizeInBytes;
  private final DigestUtil digestUtil;
  private final Map<Path, Entry> storage = new ConcurrentHashMap<>();
  private final Map<Digest, DirectoryEntry> directoryStorage = new HashMap<>();
  private final Map<Digest, Object> mutexes = new HashMap<>();

  private transient long sizeInBytes = 0;
  private transient Entry header = new SentinelEntry();

  public synchronized boolean contains(Digest digest, boolean isExecutable) {
    Path key = getKey(digest, isExecutable);
    Entry e = storage.get(key);
    if (e != null) {
      e.recordAccess(header);
      return true;
    }
    return false;
  }

  @Override
  public boolean contains(Digest digest) {
    /* maybe swap the order here if we're higher in ratio on one side */
    return contains(digest, false) || contains(digest, true);
  }

  private Blob get(Digest digest, boolean isExecutable) {
    try {
      Path blobPath = put(digest, isExecutable, null);
      try (InputStream in = Files.newInputStream(blobPath)) {
        return new Blob(ByteString.readFrom(in), digestUtil);
      } finally {
        decrementReferences(ImmutableList.<Path>of(blobPath), ImmutableList.<Digest>of());
      }
    } catch (java.nio.file.NoSuchFileException e) {
    } catch (IOException e) {
      if (!e.getMessage().equals("file not found")) {
        e.printStackTrace();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return null;
  }

  @Override
  public Blob get(Digest digest) {
    if (contains(digest, false)) {
      return get(digest, false);
    }
    return get(digest, true);
  }


  @Override
  public void put(Blob blob) {
    Path blobPath = getKey(blob.getDigest(), false);
    try {
      putImpl(blobPath, blob.getDigest().getSizeBytes(), false, () -> blob.getData().newInput(), null);
      decrementReferences(ImmutableList.<Path>of(blobPath), ImmutableList.<Digest>of());
    } catch (IOException e) {
      /* unlikely, our stream comes from the blob */
      e.printStackTrace();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void put(Blob blob, Runnable onExpiration) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized Object acquire(Digest digest) {
    Object mutex = mutexes.get(digest);
    if (mutex == null) {
      mutex = new Object();
      mutexes.put(digest, mutex);
    }
    return mutex;
  }

  @Override
  public synchronized void release(Digest digest) {
    // prevents this lock from being exclusive to other accesses, since it
    // must now be present
    mutexes.remove(digest);
  }

  public CASFileCache(InputStreamFactory inputStreamFactory, Path root, long maxSizeInBytes, DigestUtil digestUtil) {
    this.inputStreamFactory = inputStreamFactory;
    this.root = root;
    this.maxSizeInBytes = maxSizeInBytes;
    this.digestUtil = digestUtil;

    header.before = header.after = header;
  }

  /**
   * initialize the cache for persistent storage and inject any
   * consistent entries which already exist under the root into
   * the storage map. This call will create the root if it does
   * not exist, and will scale in cost with the number of files
   * already present.
   */
  public void start() throws IOException {
    Files.createDirectories(root);

    Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
      private Path parseFileEntryKey(String fileName, long size) {
        String[] components = fileName.toString().split("_");
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
        } catch (NumberFormatException ex) {
          return null;
        }

        return getKey(digest, isExecutable);
      }

      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        long size = attrs.size();
        if (sizeInBytes + size > maxSizeInBytes) {
          Files.delete(file);
        } else {
          Path key = null;
          if (file.getParent().equals(root)) {
            key = parseFileEntryKey(file.getFileName().toString(), size);
          }
          if (key != null) {
            Entry e = new Entry(key, size, null);
            storage.put(e.key, e);
            e.decrementReference(header);
            sizeInBytes += size;
          } else {
            Files.delete(file);
          }
        }
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        if (!dir.equals(root)) {
          CASFileCache.removeDirectory(dir);
        }
        return FileVisitResult.CONTINUE;
      }
    });
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

  public synchronized void decrementReferences(Iterable<Path> inputFiles, Iterable<Digest> inputDirectories) {
    // decrement references and notify if any dropped to 0
    // insert after the last 0-reference count entry in list
    boolean entriesMadeAvailable = false;

    for (Digest inputDirectory : inputDirectories) {
      inputFiles = Iterables.concat(inputFiles, directoryStorage.get(inputDirectory).inputs);
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

  public Path getPath(String filename) {
    return root.resolve(filename);
  }

  private Path getDirectoryPath(Digest digest) {
    return root.resolve(digestFilename(digest));
  }

  /** must be called in synchronized context */
  private Path expireEntry() throws IOException, InterruptedException {
    while (header.after == header) {
      wait();
    }
    Entry e = header.after;
    if (e.referenceCount != 0) {
      throw new RuntimeException("ERROR: Reference counts lru ordering has not been maintained correctly, attempting to expire referenced (or negatively counted) content");
    }
    storage.remove(e.key);
    for (Digest containingDirectory : e.containingDirectories) {
      expireDirectory(containingDirectory);
    }
    e.remove();
    sizeInBytes -= e.size;
    return e.key;
  }

  public static void removeDirectory(Path directory) throws IOException {
    Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  /** must be called in synchronized context */
  private void expireDirectory(Digest digest) throws IOException {
    DirectoryEntry e = directoryStorage.remove(digest);
    Path path = getDirectoryPath(digest);

    for (Path input : e.inputs) {
      Entry fileEntry = storage.get(input);

      if (fileEntry != null) {
        fileEntry.containingDirectories.remove(digest);
      }
    }
    CASFileCache.removeDirectory(path);
  }

  /** must be called in synchronized context */
  private void incrementReferences(Iterable<Path> inputs) {
    for (Path input : inputs) {
      storage.get(input).incrementReference();
    }
  }

  /** must be called in synchronized context */
  private void fetchDirectory(
      Digest containingDirectory,
      Path path,
      Digest digest,
      Map<Digest, Directory> directoriesIndex,
      ImmutableList.Builder<Path> inputsBuilder) throws IOException, InterruptedException {
    Directory directory = directoriesIndex.get(digest);
    if (Files.isDirectory(path)) {
      CASFileCache.removeDirectory(path);
    }
    Files.createDirectory(path);
    for (FileNode fileNode : directory.getFilesList()) {
      if (fileNode.getDigest().getSizeBytes() != 0) {
        Path fileCacheKey = put(fileNode.getDigest(), fileNode.getIsExecutable(), containingDirectory);
        // FIXME this can die with 'too many links'... needs some cascading fallout
        Files.createLink(path.resolve(fileNode.getName()), fileCacheKey);
        inputsBuilder.add(fileCacheKey);
      } else {
        Files.createFile(path.resolve(fileNode.getName()));
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

  public synchronized Path putDirectory(
      Digest digest,
      Map<Digest, Directory> directoriesIndex) throws IOException, InterruptedException {
    Path path = getDirectoryPath(digest);

    DirectoryEntry e = directoryStorage.get(digest);
    if (e != null) {
      incrementReferences(e.inputs);
      return path;
    }

    ImmutableList.Builder<Path> inputsBuilder = new ImmutableList.Builder<>();
    fetchDirectory(digest, path, digest, directoriesIndex, inputsBuilder);

    e = new DirectoryEntry(directoriesIndex.get(digest), inputsBuilder.build());

    directoryStorage.put(digest, e);

    return path;
  }

  public Path put(Digest digest, boolean isExecutable, Digest containingDirectory) throws IOException, InterruptedException {
    Path key = getKey(digest, isExecutable);
    putImpl(key, digest.getSizeBytes(), isExecutable, () -> inputStreamFactory.newInput(digest), containingDirectory);
    return key;
  }

  @FunctionalInterface
  interface InputStreamSupplier {
    InputStream newInput() throws IOException;
  }

  private void putImpl(
      Path key,
      long blobSizeInBytes,
      boolean isExecutable,
      InputStreamSupplier inSupplier,
      Digest containingDirectory)
      throws IOException, InterruptedException {
    ImmutableList.Builder<Path> expiredKeys = null;

    synchronized(this) {
      Entry e = storage.get(key);
      if (e != null) {
        if (containingDirectory != null) {
          e.containingDirectories.add(containingDirectory);
        }
        e.incrementReference();
        return;
      }

      sizeInBytes += blobSizeInBytes;

      while (sizeInBytes > maxSizeInBytes) {
        if (expiredKeys == null) {
          expiredKeys = new ImmutableList.Builder<Path>();
        }
        expiredKeys.add(expireEntry());
      }
    }

    if (expiredKeys != null) {
      for (Path expiredKey : expiredKeys.build()) {
        Files.delete(expiredKey);
      }
    }

    Path tmpPath;
    long copySize;
    try (InputStream in = inSupplier.newInput()) {
      // FIXME make a validating file copy object and verify digest
      tmpPath = key.resolveSibling(key.getFileName() + ".tmp");
      copySize = Files.copy(in, tmpPath);
    }

    if (copySize != blobSizeInBytes) {
      Files.delete(tmpPath);
      throw new IOException("blob digest size mismatch, expected " + blobSizeInBytes + ", was " + copySize);
    }
    setPermissions(tmpPath, isExecutable);
    Files.move(tmpPath, key, REPLACE_EXISTING);

    Entry e = new Entry(key, blobSizeInBytes, containingDirectory);

    storage.put(key, e);
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

    private Entry() {
      key = null;
      size = -1;
      referenceCount = -1;
      containingDirectories = null;
    }

    public Entry(Path key, long size, Digest containingDirectory) {
      this.key = key;
      this.size = size;
      referenceCount = 1;
      containingDirectories = new HashSet<>();
      if (containingDirectory != null) {
        containingDirectories.add(containingDirectory);
      }
    }

    public void remove() {
      before.after = after;
      after.before = before;
    }

    public void addBefore(Entry existingEntry) {
      after = existingEntry;
      before = existingEntry.before;
      before.after = this;
      after.before = this;
    }

    public void incrementReference() {
      if (referenceCount == 0) {
        remove();
      }
      referenceCount++;
    }

    public void decrementReference(Entry header) {
      referenceCount--;
      if (referenceCount == 0) {
        addBefore(header);
      }
    }

    public void recordAccess(Entry header) {
      if (referenceCount == 0) {
        remove();
        addBefore(header);
      }
    }
  }

  private static class SentinelEntry extends Entry {
    @Override
    public void remove() {
      throw new UnsupportedOperationException("sentinal cannot be removed");
    }

    @Override
    public void addBefore(Entry existingEntry) {
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
    Directory directory;
    Iterable<Path> inputs;

    public DirectoryEntry(Directory directory, Iterable<Path> inputs) {
      this.directory = directory;
      this.inputs = inputs;
    }
  }
}
