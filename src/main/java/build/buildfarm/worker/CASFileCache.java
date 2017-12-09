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

import build.buildfarm.common.Digests;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.devtools.remoteexecution.v1test.Digest;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class CASFileCache {
  private final InputStreamFactory inputStreamFactory;
  private final Path root;
  private final long maxSizeInBytes;
  private final Map<String, Entry> storage;

  private transient long sizeInBytes;
  private transient Entry header;

  CASFileCache(InputStreamFactory inputStreamFactory, Path root, long maxSizeInBytes) {
    this.inputStreamFactory = inputStreamFactory;
    this.root = root;
    this.maxSizeInBytes = maxSizeInBytes;

    sizeInBytes = 0;
    header = new SentinelEntry();
    header.before = header.after = header;
    storage = new ConcurrentHashMap<>();
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
      private String parseFileEntryKey(String fileName, long size) {
        String[] components = fileName.toString().split("_");
        Digest digest;
        if (components.length < 2 || components.length > 3) {
          return null;
        }

        String hashComponent = components[0];
        String sizeComponent = components[1];
        // FIXME hash size check, and it should probably match hash func...
        int parsedSizeComponent;
        try {
          parsedSizeComponent = Integer.parseInt(sizeComponent);
        } catch (NumberFormatException ex) {
          return null;
        }

        if (parsedSizeComponent != size) {
          return null;
        }

        digest = Digests.buildDigest(hashComponent, parsedSizeComponent);
        boolean isExecutable = false;
        if (components.length == 3) {
          if (components[2].equals("exec")) {
            isExecutable = true;
          } else {
            return null;
          }
        }

        return toFileEntryKey(
            Digests.buildDigest(hashComponent, parsedSizeComponent),
            isExecutable);
      }

      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        long size = attrs.size();
        if (sizeInBytes + size > maxSizeInBytes) {
          Files.delete(file);
        } else {
          String key = null;
          if (file.getParent().equals(root)) {
            key = parseFileEntryKey(file.getFileName().toString(), size);
          }
          if (key != null) {
            Entry e = new Entry(key, size);
            storage.put(e.key, e);
            e.decrementReference(header);
            sizeInBytes += size;
          } else {
            Files.delete(file);
          }
        }
        return FileVisitResult.CONTINUE;
      }
    });
  }

  private static String digestFilename(Digest digest) {
    return String.format("%s_%d", digest.getHash(), digest.getSizeBytes());
  }

  public static String toFileEntryKey(Digest digest, boolean isExecutable) {
    return String.format(
        "%s%s",
        digestFilename(digest),
        (isExecutable ? "_exec" : ""));
  }

  public synchronized void update(Iterable<String> inputs) {
    // decrement references and notify if any dropped to 0
    boolean entriesMadeAvailable = false;

    for (String input : inputs) {
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

  public Path getPath(String key) {
    return root.resolve(key);
  }

  /** must be called in synchronized context */
  private String expireEntry() throws IOException, InterruptedException {
    while (header.before == header.after) {
      wait();
    }
    Entry e = header.after;
    if (e.referenceCount != 0) {
      throw new RuntimeException("ERROR: Reference counts lru ordering has not been maintained correctly, attempting to expire referenced (or negatively counted) content");
    }
    storage.remove(e.key);
    e.remove();
    sizeInBytes -= e.size;
    return e.key;
  }

  public String put(Digest digest, boolean isExecutable) throws InterruptedException {
    String key = toFileEntryKey(digest, isExecutable);
    ImmutableList.Builder<String> expiredKeys = null;

    synchronized(this) {
      Entry e = storage.get(key);
      if (e != null) {
        e.incrementReference(header);
        return key;
      }

      sizeInBytes += digest.getSizeBytes();

      while (sizeInBytes > maxSizeInBytes) {
        if (expiredKeys == null) {
          expiredKeys = new ImmutableList.Builder<String>();
        }
        try {
          expiredKeys.add(expireEntry());
        } catch (IOException ex) {
        }
      }
    }

    if (expiredKeys != null) {
      for (String expiredKey : expiredKeys.build()) {
        try {
          Files.delete(getPath(expiredKey));
        } catch (IOException ex) {
        }
      }
    }

    try (InputStream in = inputStreamFactory.apply(digest)) {
      Path path = getPath(key);
      // FIXME make a validating file copy object and verify digest
      Path tmpPath = path.resolveSibling(path.getFileName() + ".tmp");
      long copySize = Files.copy(in, tmpPath);
      in.close();
      if (copySize != digest.getSizeBytes()) {
        Files.delete(tmpPath);
        return null;
      }
      setPermissions(tmpPath, isExecutable);
      Files.move(tmpPath, path, REPLACE_EXISTING);
    } catch (IOException ex) {
      ex.printStackTrace();
      return null;
    } catch (StatusRuntimeException ex) {
      ex.printStackTrace();
      return null;
    }

    Entry e = new Entry(key, digest.getSizeBytes());

    storage.put(key, e);

    return key;
  }

  private static void setPermissions(Path path, boolean isExecutable) throws IOException {
    ImmutableSet.Builder<PosixFilePermission> perms = new ImmutableSet.Builder<PosixFilePermission>()
      .add(PosixFilePermission.OWNER_READ);
    if (isExecutable) {
      perms.add(PosixFilePermission.OWNER_EXECUTE);
    }
    Files.setPosixFilePermissions(path, perms.build());
  }

  private static class Entry {
    Entry before, after;
    final String key;
    final long size;
    int referenceCount;

    private Entry() {
      key = null;
      size = -1;
      referenceCount = -1;
    }

    public Entry(String key, long size) {
      this.key = key;
      this.size = size;
      referenceCount = 1;
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

    public void addAfter(Entry existingEntry) {
      before = existingEntry;
      after = existingEntry.after;
      after.before = this;
      before.after = this;
    }

    public void incrementReference(Entry header) {
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
    public void addAfter(Entry existingEntry) {
      throw new UnsupportedOperationException("sentinal cannot be added");
    }

    @Override
    public void incrementReference(Entry header) {
      throw new UnsupportedOperationException("sentinal cannot be referenced");
    }

    @Override
    public void decrementReference(Entry header) {
      throw new UnsupportedOperationException("sentinal cannot be referenced");
    }
  }
}
