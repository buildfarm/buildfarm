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

import build.buildfarm.common.DigestUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.DirectoryNode;
import com.google.devtools.remoteexecution.v1test.FileNode;
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
import java.util.concurrent.ConcurrentHashMap;

public class CASFileCache {
  private final InputStreamFactory inputStreamFactory;
  private final Path root;
  private final long maxSizeInBytes;
  private final DigestUtil digestUtil;
  private final Map<Path, Entry> storage;
  private final Map<Digest, DirectoryEntry> directoryStorage;

  private transient long sizeInBytes;
  private transient Entry header;

  public CASFileCache(InputStreamFactory inputStreamFactory, Path root, long maxSizeInBytes, DigestUtil digestUtil) {
    this.inputStreamFactory = inputStreamFactory;
    this.root = root;
    this.maxSizeInBytes = maxSizeInBytes;
    this.digestUtil = digestUtil;

    sizeInBytes = 0;
    header = new SentinelEntry();
    header.before = header.after = header;
    storage = new ConcurrentHashMap<>();
    directoryStorage = new HashMap<>();
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
          Files.delete(dir);
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
    removeDirectory(path);
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
    ImmutableList.Builder<Path> expiredKeys = null;

    synchronized(this) {
      Entry e = storage.get(key);
      if (e != null) {
        if (containingDirectory != null) {
          e.containingDirectories.add(containingDirectory);
        }
        e.incrementReference();
        return key;
      }

      sizeInBytes += digest.getSizeBytes();

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

    if (digest.getSizeBytes() > 0) {
      try (InputStream in = inputStreamFactory.apply(digest)) {
        // FIXME make a validating file copy object and verify digest
        tmpPath = key.resolveSibling(key.getFileName() + ".tmp");
        copySize = Files.copy(in, tmpPath);
      }
    } else {
      tmpPath = key.resolveSibling(key.getFileName() + ".tmp");
      Files.createFile(tmpPath);
      copySize = 0;
    }

    if (copySize != digest.getSizeBytes()) {
      Files.delete(tmpPath);
      return null;
    }
    setPermissions(tmpPath, isExecutable);
    Files.move(tmpPath, key, REPLACE_EXISTING);

    Entry e = new Entry(key, digest.getSizeBytes(), containingDirectory);

    storage.put(key, e);

    return key;
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
