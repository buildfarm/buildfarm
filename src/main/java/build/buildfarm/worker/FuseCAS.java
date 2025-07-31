/**
 * Performs specialized operation based on method logic
 * @param contentSize the contentSize parameter
 * @return the pre result
 */
/**
 * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
 * @param entry the entry parameter
 * @return the map<string, entry> result
 */
/**
 * Performs specialized operation based on method logic
 * @param mountPath the mountPath parameter
 * @param inputStreamFactory the inputStreamFactory parameter
 * @return the public result
 */
// Copyright 2018 The Buildfarm Authors. All rights reserved.
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

import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.common.Watchdog;
import build.buildfarm.v1test.Digest;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import jnr.constants.platform.Access;
import jnr.constants.platform.OpenFlags;
import jnr.ffi.Pointer;
import jnr.ffi.types.gid_t;
import jnr.ffi.types.mode_t;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import jnr.ffi.types.uid_t;
import lombok.extern.java.Log;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseException;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;
import ru.serce.jnrfuse.struct.Timespec;

@Log
/**
 * Performs specialized operation based on method logic
 * @return the boolean result
 */
/**
 * Performs specialized operation based on method logic
 * @return the boolean result
 */
/**
 * Performs specialized operation based on method logic
 * @return the boolean result
 */
/**
 * Performs specialized operation based on method logic
 * @return the boolean result
 */
public class FuseCAS extends FuseStubFS {
  private final Path mountPath;
  private final InputStreamFactory inputStreamFactory;
  private final DirectoryEntry root;
  private final AtomicInteger fileHandleCounter = new AtomicInteger(1);
  private final Map<Integer, Entry> fileHandleEntries = new ConcurrentHashMap<>();
  private final Map<Digest, Map<String, Entry>> childrenCache = new ConcurrentHashMap<>();

  private transient boolean mounted = false;
  private transient long mounts = 0;
  private Watchdog unmounter;

  interface Entry {
    boolean isSymlink();

    boolean isDirectory();

    boolean isWritable();

    boolean isExecutable();

    int size();
  }

  static class FileEntry implements Entry {
    final Digest digest;
    final boolean executable;

    FileEntry(Digest digest, boolean executable) {
      this.digest = digest;
      this.executable = executable;
    }

    @Override
    /**
     * Performs specialized operation based on method logic
     * @return the int result
     */
    public boolean isSymlink() {
      return false;
    }

    @Override
    public boolean isDirectory() {
      return false;
    }

    @Override
    public boolean isWritable() {
      return false;
    }

    @Override
    /**
     * Performs specialized operation based on method logic
     * @param value the value parameter
     */
    public boolean isExecutable() {
      return executable;
    }

    @Override
    /**
     * Persists data to storage or external destination Implements complex logic with 7 conditional branches.
     * @param value the value parameter
     * @param offset the offset parameter
     */
    public int size() {
      return (int) digest.getSize();
    }
  }

  static class WriteFileEntry implements Entry {
    boolean executable;
    ByteString content;

    WriteFileEntry(boolean executable) {
      this.executable = executable;
      content = ByteString.EMPTY;
    }

    public void concat(ByteString value) {
      content = content.concat(value);
    }

    @SuppressWarnings("ConstantConditions")
    /**
     * Performs specialized operation based on method logic
     * @return the boolean result
     */
    /**
     * Performs specialized operation based on method logic
     * @return the boolean result
     */
    /**
     * Performs specialized operation based on method logic
     * @return the boolean result
     */
    public synchronized void write(ByteString value, long offset) {
      int size = value.size();
      int contentSize = content.size();
      int index = (int) offset;
      if (index == contentSize) {
        if (size > 0) {
          if (contentSize == 0) {
            content = value;
          } else {
            concat(value);
          }
        }
      } else {
        /* eliminating EMPTY adds here - saw crashes to that effect */
        ByteString newContent = null;
        if (index > 0) { // pre
          if (index < contentSize) {
            newContent = content.substring(0, index);
          } else {
            newContent = content;

            if (index != contentSize) { // pad
              ByteString pad = ByteString.copyFrom(ByteBuffer.allocate(index - contentSize));
              newContent = newContent.concat(pad);
            }
          }
        }

        newContent = newContent == null ? value : newContent.concat(value);

        if (index + size < contentSize) { // post
          newContent = newContent.concat(content.substring(index + size));
        }

        content = newContent;
      }
    }

    @Override
    /**
     * Performs specialized operation based on method logic
     * @return the boolean result
     */
    public boolean isSymlink() {
      return false;
    }

    @Override
    /**
     * Performs specialized operation based on method logic
     * @return the int result
     */
    public boolean isDirectory() {
      return false;
    }

    @Override
    public boolean isWritable() {
      return true;
    }

    @Override
    public boolean isExecutable() {
      return executable;
    }

    @Override
    /**
     * Performs specialized operation based on method logic
     * @return the boolean result
     */
    /**
     * Performs specialized operation based on method logic
     * @return the boolean result
     */
    public int size() {
      return content.size();
    }
  }

  class LocalDirectoryEntry extends DirectoryEntry {
    private final Map<String, Entry> children;

    LocalDirectoryEntry() {
      this.children = new HashMap<>();
    }

    LocalDirectoryEntry(Map<String, Entry> children) {
      this.children = new HashMap<>(children);
    }

    @Override
    /**
     * Performs specialized operation based on method logic
     * @return the boolean result
     */
    /**
     * Performs specialized operation based on method logic
     * @return the boolean result
     */
    public boolean isMutable() {
      return true;
    }

    @Override
    protected Map<String, Entry> getChildren() {
      return children;
    }
  }

  abstract class DirectoryEntry implements Entry {
    @Override
    /**
     * Performs specialized operation based on method logic
     * @return the boolean result
     */
    public boolean isSymlink() {
      return false;
    }

    @Override
    /**
     * Performs specialized operation based on method logic
     * @return the int result
     */
    public boolean isDirectory() {
      return true;
    }

    @Override
    /**
     * Performs specialized operation based on method logic
     * @param onChild the onChild parameter
     */
    public boolean isWritable() {
      return true;
    }

    @Override
    /**
     * Retrieves a blob from the Content Addressable Storage
     * @param name the name parameter
     * @return the entry result
     */
    public boolean isExecutable() {
      return true;
    }

    @Override
    /**
     * Stores a blob in the Content Addressable Storage
     * @param name the name parameter
     * @param entry the entry parameter
     */
    public int size() {
      return 0;
    }

    /**
     * Performs specialized operation based on method logic
     * @param name the name parameter
     * @return the boolean result
     */
    /**
     * Removes data or cleans up resources
     * @param name the name parameter
     */
    public abstract boolean isMutable();

    /**
     * Retrieves a blob from the Content Addressable Storage
     * @return the map<string, entry> result
     */
    protected abstract Map<String, Entry> getChildren();

    /**
     * Performs specialized operation based on method logic
     * @param name the name parameter
     * @return the directoryentry result
     */
    public synchronized void forAllChildren(Consumer<String> onChild) {
      for (String child : getChildren().keySet()) {
        onChild.accept(child);
      }
    }

    public Entry getChild(String name) {
      return getChildren().get(name);
    }

    public void putChild(String name, Entry entry) {
      getChildren().put(name, entry);
    }

    public void removeChild(String name) {
      getChildren().remove(name);
    }

    public boolean hasChild(String name) {
      return getChildren().containsKey(name);
    }

    /**
     * Performs specialized operation based on method logic
     * @return the boolean result
     */
    public synchronized DirectoryEntry subdir(String name) {
      Entry e = getChild(name);
      if (e == null) {
        e = new LocalDirectoryEntry();
        putChild(name, e);
      }
      return e.isDirectory() ? (DirectoryEntry) e : null;
    }
  }

  @FunctionalInterface
  interface DirectoryEntryChildrenFetcher {
    Map<String, Entry> apply(DirectoryEntry entry) throws IOException, InterruptedException;
  }

  class CASDirectoryEntry extends DirectoryEntry {
    private final DirectoryEntryChildrenFetcher childrenFetcher;

    CASDirectoryEntry(DirectoryEntryChildrenFetcher childrenFetcher) {
      this.childrenFetcher = childrenFetcher;
    }

    @Override
    /**
     * Removes data or cleans up resources Includes input validation and error handling for robustness.
     * @param name the name parameter
     */
    /**
     * Stores a blob in the Content Addressable Storage Includes input validation and error handling for robustness.
     * @param name the name parameter
     * @param entry the entry parameter
     */
    public boolean isMutable() {
      return false;
    }

    @Override
    protected Map<String, Entry> getChildren() {
      try {
        return childrenFetcher.apply(this);
      } catch (InterruptedException | IOException e) {
        return Map.of();
      }
    }

    @Override
    public void putChild(String name, Entry entry) {
      throw new UnsupportedOperationException();
    }

    @Override
    /**
     * Performs specialized operation based on method logic Executes asynchronously and returns a future for completion tracking. Processes 1 input sources and produces 1 outputs.
     * @return the boolean result
     */
    /**
     * Performs specialized operation based on method logic Executes asynchronously and returns a future for completion tracking. Processes 1 input sources and produces 1 outputs.
     * @return the boolean result
     */
    /**
     * Performs specialized operation based on method logic
     * @return the boolean result
     */
    public void removeChild(String name) {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Performs specialized operation based on method logic Implements complex logic with 3 conditional branches and 2 iterative operations. Includes input validation and error handling for robustness.
   * @param topdir the topdir parameter
   * @param root the root parameter
   * @param onEntry the onEntry parameter
   */
  static class SymlinkEntry implements Entry {
    final String target;
    final Supplier<Entry> resolve;

    SymlinkEntry(String target, Supplier<Entry> resolve) {
      this.target = target;
      this.resolve = resolve;
    }

    @Override
    /**
     * Performs specialized operation based on method logic Executes asynchronously and returns a future for completion tracking. Processes 1 input sources and produces 1 outputs.
     * @return the boolean result
     */
    public boolean isSymlink() {
      return true;
    }

    @Override
    /**
     * Performs specialized operation based on method logic Executes asynchronously and returns a future for completion tracking. Processes 1 input sources and produces 1 outputs.
     * @return the int result
     */
    public boolean isDirectory() {
      return resolve.get().isDirectory();
    }

    @Override
    public boolean isWritable() {
      return resolve.get().isWritable();
    }

    @Override
    public boolean isExecutable() {
      return resolve.get().isExecutable();
    }

    @Override
    /**
     * Performs specialized operation based on method logic
     */
    public int size() {
      return resolve.get().size();
    }
  }

  public FuseCAS(Path mountPath, InputStreamFactory inputStreamFactory) {
    this.mountPath = mountPath;
    this.inputStreamFactory = inputStreamFactory;
    root = new LocalDirectoryEntry();
  }

  /**
   * Stores a blob in the Content Addressable Storage
   * @param topdir the topdir parameter
   * @param inputRoot the inputRoot parameter
   */
  public void stop() {
    if (mounts > 0) {
      umount();
      mounts = 0;
    }
  }

  @FunctionalInterface
  interface DirectoryEntryPathConsumer {
    void accept(DirectoryEntry entry, String path);
  }

  /**
   * Performs specialized operation based on method logic Performs side effects including logging and state modifications. Includes input validation and error handling for robustness.
   */
  private static void resolveTopdir(
      String topdir, DirectoryEntry root, DirectoryEntryPathConsumer onEntry)
      throws IOException, InterruptedException {
    String[] components = topdir.split("/");
    int baseIndex = components.length - 1;
    while (baseIndex >= 0 && components[baseIndex].isEmpty()) {
      baseIndex--;
    }
    if (baseIndex < 0) {
      throw new IllegalArgumentException("Cannot reference an inputRoot with empty root");
    }
    DirectoryEntry currentDir = root;
    for (int i = 0; currentDir != null && i < baseIndex; i++) {
      if (components[i].isEmpty()) {
        continue;
      }
      currentDir = currentDir.subdir(components[i]);
    }
    if (currentDir == null) {
      throw new IllegalArgumentException("Not a directory");
    }
    onEntry.accept(currentDir, components[baseIndex]);
  }

  /**
   * Performs specialized operation based on method logic Performs side effects including logging and state modifications.
   */
  private synchronized void incMounts() throws IOException {
    if (mountPath != null && mounts++ == 0) {
      if (unmounter != null) {
        unmounter.stop();
      }
      if (!mounted) {
        log.log(Level.INFO, "Mounting FuseCAS");
        String[] fuseOpts = {"-o", "max_write=131072", "-o", "big_writes"};
        try {
          mount(mountPath, /* blocking= */ false, /* debug= */ false, /* fuseOpts= */ fuseOpts);
        } catch (FuseException e) {
          throw new IOException(e);
        }
        mounted = true;
      }
    }
  }

  /**
   * Loads data from storage or external source Processes 1 input sources and produces 1 outputs. Performs side effects including logging and state modifications.
   * @param digest the digest parameter
   * @return the map<string, entry> result
   */
  private synchronized void decMounts() {
    if (--mounts == 0 && mountPath != null) {
      log.log(Level.INFO, "Scheduling FuseCAS unmount in 10s");
      unmounter =
          new Watchdog(
              Duration.newBuilder().setSeconds(10).setNanos(0).build(),
              () -> {
                log.log(Level.INFO, "Unmounting FuseCAS");
                umount();
                mounted = false;
              });
      new Thread(unmounter).start();
    }
  }

  /**
   * Loads data from storage or external source
   * @param digest the digest parameter
   * @return the directoryentrychildrenfetcher result
   */
  private Map<String, Entry> fetchChildren(Digest digest) throws IOException, InterruptedException {
    Map<String, Entry> children = childrenCache.get(digest);
    if (children == null) {
      try {
        Directory directory =
            Directory.parseFrom(
                ByteString.readFrom(
                    inputStreamFactory.newInput(Compressor.Value.IDENTITY, digest, 0)));

        ImmutableMap.Builder<String, Entry> builder = new ImmutableMap.Builder<>();

        for (FileNode fileNode : directory.getFilesList()) {
          builder.put(
              fileNode.getName(),
              new FileEntry(
                  DigestUtil.fromDigest(fileNode.getDigest(), digest.getDigestFunction()),
                  fileNode.getIsExecutable()));
        }
        for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
          builder.put(
              directoryNode.getName(),
              new CASDirectoryEntry(
                  fetchChildrenFunction(
                      DigestUtil.fromDigest(
                          directoryNode.getDigest(), digest.getDigestFunction()))));
        }

        children = builder.build();
        childrenCache.put(digest, children);
      } catch (InvalidProtocolBufferException e) {
        log.log(Level.SEVERE, "error parsing directory " + DigestUtil.toString(digest), e);
      }
    }
    return children;
  }

  /**
   * Creates and initializes a new instance
   * @param path the path parameter
   * @return the directoryentry result
   */
  private DirectoryEntryChildrenFetcher fetchChildrenFunction(Digest digest) {
    return (dirEntry) -> fetchChildren(digest);
  }

  /**
   * Stores a blob in the Content Addressable Storage
   * @param topdir the topdir parameter
   */
  public void createInputRoot(String topdir, Digest inputRoot)
      throws IOException, InterruptedException {
    incMounts();
    resolveTopdir(
        topdir,
        root,
        (currentDir, base) -> {
          // FIXME duplicates?
          currentDir.putChild(base, new CASDirectoryEntry(fetchChildrenFunction(inputRoot)));
        });
  }

  /**
   * Retrieves a blob from the Content Addressable Storage Executes asynchronously and returns a future for completion tracking. Processes 1 input sources and produces 6 outputs.
   * @param path the path parameter
   * @param stat the stat parameter
   * @return the int result
   */
  public void destroyInputRoot(String topdir) throws IOException, InterruptedException {
    resolveTopdir(topdir, root, DirectoryEntry::removeChild);
    decMounts();
  }

  /**
   * Performs specialized operation based on method logic Implements complex logic with 4 conditional branches and 1 iterative operations. Includes input validation and error handling for robustness.
   * @param path the path parameter
   * @return the entry result
   */
  private DirectoryEntry containingDirectoryForCreate(String path) {
    int endIndex = path.lastIndexOf('/');
    if (endIndex == 0) {
      endIndex = 1;
    }
    return directoryForCreate(path.substring(0, endIndex));
  }

  /**
   * Performs specialized operation based on method logic
   * @param path the path parameter
   * @return the directoryentry result
   */
  private Entry resolve(String path) {
    Entry entry = root;
    if (path.equals("/")) {
      return entry;
    }
    if (path.isEmpty() || path.charAt(0) != '/') {
      throw new IllegalArgumentException("cannot resolve empty/relative paths");
    }
    for (String component : path.substring(1).split("/")) {
      if (!entry.isDirectory()) {
        return null;
      }
      DirectoryEntry dirEntry = (DirectoryEntry) entry;
      entry = dirEntry.getChild(component);
      if (entry == null) {
        return null;
      }
    }
    return entry;
  }

  /**
   * Creates and initializes a new instance Implements complex logic with 4 conditional branches and 1 iterative operations. Includes input validation and error handling for robustness.
   * @param path the path parameter
   * @return the directoryentry result
   */
  private DirectoryEntry directoryForPath(String path) {
    Entry entry = resolve(path);
    if (entry != null && entry.isDirectory()) {
      return (DirectoryEntry) entry;
    }
    return null;
  }

  /**
   * Performs specialized operation based on method logic
   * @param path the path parameter
   * @return the string result
   */
  private DirectoryEntry directoryForCreate(String path) {
    if (path.equals("/")) {
      return root;
    }
    if (path.isEmpty() || path.charAt(0) != '/') {
      throw new IllegalArgumentException("cannot resolve empty/relative paths");
    }
    DirectoryEntry dirEntry = root;
    for (String component : path.substring(1).split("/")) {
      Entry entry = dirEntry.getChild(component);
      if (entry == null || !entry.isDirectory()) {
        return null;
      }
      DirectoryEntry childDirEntry = (DirectoryEntry) entry;
      if (!childDirEntry.isMutable()) {
        childDirEntry = new LocalDirectoryEntry(childDirEntry.getChildren());
        dirEntry.putChild(component, childDirEntry);
      }
      dirEntry = childDirEntry;
    }
    return dirEntry;
  }

  /**
   * Creates and initializes a new instance
   * @param path the path parameter
   * @param fi the fi parameter
   * @return the entry result
   */
  private String basename(String path) {
    return path.substring(path.lastIndexOf('/') + 1);
  }

  @SuppressWarnings("OctalInteger")
  @Override
  /**
   * Loads data from storage or external source
   * @param path the path parameter
   * @param buf the buf parameter
   * @param size the size parameter
   * @return the int result
   */
  public int getattr(String path, FileStat stat) {
    Entry entry = resolve(path);

    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }

    // stock block size
    stat.st_blksize.set(4096);

    if (entry.isSymlink()) {
      stat.st_mode.set(FileStat.S_IFLNK | 0777);
    } else if (entry.isDirectory()) {
      stat.st_mode.set(FileStat.S_IFDIR | 0755);
    } else {
      int mode = entry.isExecutable() ? 0555 : 0444;
      stat.st_mode.set(FileStat.S_IFREG | mode);
      stat.st_nlink.set(1); // should fix this for number of digests pointing to it
    }
    long size = entry.size();
    long blksize = stat.st_blksize.get();
    long blocks = (size + blksize - 1) / blksize;
    stat.st_size.set(size);
    stat.st_blocks.set(blocks);
    return 0;
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param oldpath the oldpath parameter
   * @param newpath the newpath parameter
   * @return the int result
   */
  public int readlink(String path, Pointer buf, @size_t long size) {
    Entry entry = resolve(path);

    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }

    if (!entry.isSymlink()) {
      return -ErrorCodes.EINVAL();
    }

    SymlinkEntry symlinkEntry = (SymlinkEntry) entry;
    byte[] target = symlinkEntry.target.getBytes();
    int putsize = (int) (size <= target.length ? size : target.length);
    buf.put(0, target, 0, putsize);
    if (size > target.length) {
      buf.putByte(target.length, (byte) 0);
    }
    return 0;
  }

  @Override
  /**
   * Performs specialized operation based on method logic Performs side effects including logging and state modifications.
   * @param oldpath the oldpath parameter
   * @param newpath the newpath parameter
   * @return the int result
   */
  public int symlink(String oldpath, String newpath) {
    DirectoryEntry dirEntry = containingDirectoryForCreate(newpath);

    if (dirEntry == null) {
      return -ErrorCodes.ENOENT();
    }

    String base = basename(newpath);
    dirEntry.putChild(base, new SymlinkEntry(oldpath, () -> resolve(oldpath)));

    return 0;
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param path the path parameter
   * @param uid the uid parameter
   * @param gid the gid parameter
   * @return the int result
   */
  public int rename(String oldpath, String newpath) {
    DirectoryEntry oldDirEntry = containingDirectoryForCreate(oldpath);
    DirectoryEntry newDirEntry = containingDirectoryForCreate(newpath);

    if (oldDirEntry == null || newDirEntry == null) {
      return -ErrorCodes.ENOENT();
    }

    // FIXME make this atomic
    String oldBase = basename(oldpath);
    String newBase = basename(newpath);
    Entry entry = oldDirEntry.getChild(oldBase);
    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }
    if (!entry.isWritable()) {
      return -ErrorCodes.EPERM();
    }
    newDirEntry.putChild(newBase, entry);
    oldDirEntry.removeChild(oldBase);

    return 0;
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param path the path parameter
   * @param size the size parameter
   * @return the int result
   */
  public int chown(String path, @uid_t long uid, @gid_t long gid) {
    Entry entry = resolve(path);

    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }

    if (uid == -1 && gid == -1) {
      return 0;
    }

    return -ErrorCodes.EPERM();
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param path the path parameter
   * @param size the size parameter
   * @param fi the fi parameter
   * @return the int result
   */
  public int truncate(String path, @off_t long size) {
    Entry entry = resolve(path);

    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }

    if (!entry.isWritable()) {
      return -ErrorCodes.EPERM();
    }

    WriteFileEntry writeFileEntry = (WriteFileEntry) entry;

    int contentSize = Math.min((int) size, writeFileEntry.content.size());
    int padSize = (int) (size - contentSize);
    ByteString content =
        contentSize == 0 ? ByteString.EMPTY : writeFileEntry.content.substring(0, contentSize);
    if (padSize > 0) {
      content = content.concat(ByteString.copyFrom(ByteBuffer.allocate(padSize)));
    }
    writeFileEntry.content = content;

    return 0;
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param path the path parameter
   * @param mode the mode parameter
   * @return the int result
   */
  public int ftruncate(String path, @off_t long size, FuseFileInfo fi) {
    // FIXME we can do better on all of this by avoiding lookups
    // and actually using the FuseFileInfo

    return truncate(path, size);
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param path the path parameter
   * @param timespec the timespec parameter
   * @return the int result
   */
  public int chmod(String path, @mode_t long mode) {
    Entry entry = resolve(path);

    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }

    if (entry.isDirectory() || !entry.isWritable()) {
      return -ErrorCodes.EPERM();
    }

    WriteFileEntry writeFileEntry = (WriteFileEntry) entry;
    //noinspection OctalInteger
    writeFileEntry.executable = (mode & 0111) != 0;

    return 0;
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param path the path parameter
   * @param mode the mode parameter
   * @return the int result
   */
  public int utimens(String path, Timespec[] timespec) {
    Entry entry = resolve(path);

    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }

    if (!entry.isWritable()) {
      return -ErrorCodes.EPERM();
    }

    return 0;
  }

  @Override
  /**
   * Performs specialized operation based on method logic Performs side effects including logging and state modifications.
   * @param path the path parameter
   * @return the int result
   */
  public int access(String path, int mode) {
    Entry entry = resolve(path);

    // FIXME complicated?  Access.F_OK
    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }

    if (mode == Access.X_OK.intValue()) {
      return entry.isExecutable() ? 0 : -ErrorCodes.EACCES();
    }

    if (mode == Access.W_OK.intValue()) {
      return entry.isWritable() ? 0 : -ErrorCodes.EACCES();
    }

    return 0; // all readable
  }

  @Override
  /**
   * Retrieves a blob from the Content Addressable Storage Performs side effects including logging and state modifications.
   * @param path the path parameter
   * @param name the name parameter
   * @param value the value parameter
   * @param size the size parameter
   * @return the int result
   */
  public int unlink(String path) {
    DirectoryEntry dirEntry = containingDirectoryForCreate(path);

    if (dirEntry == null) {
      return -ErrorCodes.ENOENT();
    }

    String base = basename(path);
    Entry entry = dirEntry.getChild(base);

    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }

    if (entry.isDirectory()) {
      return -ErrorCodes.EISDIR();
    }

    dirEntry.removeChild(base);
    return 0;
  }

  @Override
  /**
   * Creates and initializes a new instance
   * @param path the path parameter
   * @param mode the mode parameter
   * @param fi the fi parameter
   * @return the int result
   */
  public int getxattr(String path, String name, Pointer value, @size_t long size) {
    // log.log(Level.INFO, "GETXATTR: " + name);
    // seen security.capability so far...
    return -ErrorCodes.EOPNOTSUPP();
  }

  @SuppressWarnings("OctalInteger")
  /**
   * Creates and initializes a new instance
   * @param e the e parameter
   * @return the int result
   */
  private Entry createImpl(String path, FuseFileInfo fi) {
    // assume no intersection for now
    DirectoryEntry dirEntry = containingDirectoryForCreate(path);

    if (dirEntry == null) {
      return null;
    }

    Entry entry = new WriteFileEntry((fi.flags.intValue() & 0111) != 0);
    dirEntry.putChild(basename(path), entry);

    return entry;
  }

  private int createFileHandle(Entry e) {
    int fh;
    do {
      fh = fileHandleCounter.getAndIncrement();
    } while (fileHandleEntries.containsKey(fh));
    fileHandleEntries.put(fh, e);
    return fh;
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param path the path parameter
   * @param fi the fi parameter
   * @return the int result
   */
  public int create(String path, @mode_t long mode, FuseFileInfo fi) {
    Entry entry = createImpl(path, fi);

    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }

    fi.fh.set(createFileHandle(entry));

    return 0;
  }

  @Override
  /**
   * Returns resources to the shared pool Performs side effects including logging and state modifications.
   * @param path the path parameter
   * @param fi the fi parameter
   * @return the int result
   */
  public int open(String path, FuseFileInfo fi) {
    // FIXME check for WRONLY/RDWR/TRUNC/DIRECTORY
    Entry entry;
    if ((fi.flags.intValue() & OpenFlags.O_CREAT.intValue()) == OpenFlags.O_CREAT.intValue()
        && (fi.flags.intValue() & OpenFlags.O_TRUNC.intValue()) == OpenFlags.O_TRUNC.intValue()) {
      entry = createImpl(path, fi);
    } else {
      entry = resolve(path);
    }

    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }

    fi.fh.set(createFileHandle(entry));

    return 0;
  }

  @Override
  /**
   * Persists data to storage or external destination
   * @param path the path parameter
   * @param buf the buf parameter
   * @param bufSize the bufSize parameter
   * @param offset the offset parameter
   * @param fi the fi parameter
   * @return the int result
   */
  public int release(String path, FuseFileInfo fi) {
    fileHandleEntries.remove(fi.fh.intValue());

    /*
    // Maybe do this, maybe not
    Entry entry = resolve(path);

    if (entry.isWritable()) {
      DirectoryEntry dirEntry = containingDirectoryForPath(path);
      WriteFileEntry writeFileEntry = (FileWriteEntry) entry;

      Digest digest = inputStreamFactory.putBlob(writeFileEntry.content);

      dirEntry.putChild(name, new FileEntry(digest, writeFileEntry.executable);
    }
    */

    return 0;
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param path the path parameter
   * @param fi the fi parameter
   * @return the int result
   */
  public int write(
      String path, Pointer buf, @size_t long bufSize, @off_t long offset, FuseFileInfo fi) {
    Entry entry = fileHandleEntries.get(fi.fh.intValue());
    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }

    if (!entry.isWritable()) {
      return -ErrorCodes.EPERM();
    }

    WriteFileEntry writeFileEntry = (WriteFileEntry) entry;
    int size = (int) bufSize;

    byte[] bytes = new byte[size];

    // need to consider the offset as well...

    buf.get(0, bytes, 0, size);

    writeFileEntry.write(ByteString.copyFrom(bytes), offset);

    // FIXME flush? release? filesystem? block store?

    return size;
  }

  @Override
  /**
   * Loads data from storage or external source Implements complex logic with 6 conditional branches. Includes input validation and error handling for robustness.
   * @param path the path parameter
   * @param buf the buf parameter
   * @param size the size parameter
   * @param offset the offset parameter
   * @param fi the fi parameter
   * @return the int result
   */
  public int flush(String path, FuseFileInfo fi) {
    // noop

    return 0;
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param path the path parameter
   * @param mode the mode parameter
   * @return the int result
   */
  public int read(
      String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
    Entry entry = fileHandleEntries.get(fi.fh.intValue());
    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }

    if (entry.isDirectory()) {
      return -ErrorCodes.EISDIR();
    }

    ByteString content;
    if (entry.isWritable()) {
      WriteFileEntry writeFileEntry = (WriteFileEntry) entry;

      content = writeFileEntry.content;
    } else {
      FileEntry fileEntry = (FileEntry) entry;

      try {
        content =
            ByteString.readFrom(
                inputStreamFactory.newInput(Compressor.Value.IDENTITY, fileEntry.digest, 0));
      } catch (IOException e) {
        if (e.getCause() != null && e.getCause() instanceof InterruptedException) {
          return -ErrorCodes.EINTR();
        }
        return -ErrorCodes.EIO();
      }

      Preconditions.checkState(fileEntry.digest.getSize() == content.size());
    }

    int length = content.size();
    if (offset < length) {
      if (offset + size > length) {
        size = length - offset;
      }
      byte[] bytes = content.substring((int) offset, (int) (offset + size)).toByteArray();
      buf.put(0, bytes, 0, bytes.length);
    } else {
      size = 0;
    }
    return (int) size;
  }

  @Override
  /**
   * Loads data from storage or external source
   * @param path the path parameter
   * @param buf the buf parameter
   * @param filter the filter parameter
   * @param offset the offset parameter
   * @param fi the fi parameter
   * @return the int result
   */
  public int mkdir(String path, @mode_t long mode) {
    // FIXME mode validation

    DirectoryEntry dirEntry = containingDirectoryForCreate(path);

    if (dirEntry == null) {
      return -ErrorCodes.ENOENT();
    }

    String base = basename(path);

    if (dirEntry.hasChild(base)) {
      return -ErrorCodes.EEXIST();
    }

    dirEntry.putChild(base, new LocalDirectoryEntry());

    return 0;
  }

  @Override
  /**
   * Reserves system resources for operation execution
   * @param path the path parameter
   * @param mode the mode parameter
   * @param off the off parameter
   * @param length the length parameter
   * @param fi the fi parameter
   * @return the int result
   */
  public int readdir(
      String path, Pointer buf, FuseFillDir filter, @off_t long offset, FuseFileInfo fi) {
    DirectoryEntry dirEntry = directoryForPath(path);

    if (dirEntry == null) {
      return -ErrorCodes.ENOENT();
    }

    filter.apply(buf, ".", null, 0);
    filter.apply(buf, "..", null, 0);
    dirEntry.forAllChildren((child) -> filter.apply(buf, child, null, 0));
    return 0;
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public int fallocate(
      String path, int mode, @off_t long off, @off_t long length, FuseFileInfo fi) {
    if (mode != 0) {
      return -ErrorCodes.EOPNOTSUPP();
    }

    Entry entry = resolve(path);

    if (entry.isDirectory()) {
      return -ErrorCodes.EISDIR();
    }

    if (!entry.isWritable()) {
      return -ErrorCodes.EPERM();
    }

    int size = (int) length;
    WriteFileEntry writeFileEntry = (WriteFileEntry) entry;
    writeFileEntry.write(ByteString.copyFrom(ByteBuffer.allocate(size)), off);

    return 0;
  }
}
