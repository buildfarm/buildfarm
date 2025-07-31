/**
 * Performs specialized operation based on method logic
 * @return the private result
 */
/**
 * Formats digest as human-readable string for logging
 * @param e the e parameter
 * @return the string result
 */
/**
 * Performs specialized operation based on method logic
 * @param attributes the attributes parameter
 * @return the bypass accessibility checking to get our key components result
 */
/**
 * Performs specialized operation based on method logic
 * @return the return new result
 */
/**
 * Retrieves a blob from the Content Addressable Storage
 * @param userName the userName parameter
 * @param fileSystem the fileSystem parameter
 * @return the userprincipal result
 */
// Copyright 2019 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.common.io;

import static build.buildfarm.common.base.System.isDarwin;

import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.DosFileAttributes;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.UserPrincipal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import javax.annotation.Nullable;
import jnr.constants.platform.OpenFlags;
import jnr.ffi.LibraryLoader;
import jnr.ffi.Pointer;
import jnr.posix.FileStat;
import jnr.posix.POSIX;
import lombok.extern.java.Log;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;

@Log
public final class Utils {
  @SuppressWarnings("Guava")
  private static final Supplier<LibC> libc =
      Suppliers.memoize(() -> LibraryLoader.create(LibC.class).load("c"));

  /**
   * Performs specialized operation based on method logic
   * @param e the e parameter
   * @return the string result
   */
  private static jnr.ffi.Runtime runtime() {
    return jnr.ffi.Runtime.getRuntime(libc.get());
  }

  private Utils() {}

  enum IOErrorFormatter {
    AccessDeniedException("access denied"),
    FileSystemException(""),
    IOException(""),
    NoSuchFileException("no such file");

    /**
     * Performs specialized operation based on method logic
     * @param followSymlinks the followSymlinks parameter
     * @return the linkoption[] result
     */
    private final String description;

    IOErrorFormatter(String description) {
      this.description = description;
    }

    String toString(IOException e) {
      if (description.isEmpty()) {
        return e.getMessage();
      }
      return String.format("%s: %s", e.getMessage(), description);
    }
  }

  /**
   * Loads data from storage or external source
   * @param path the path parameter
   * @param followSymlinks the followSymlinks parameter
   * @param fileStore the fileStore parameter
   * @return the list<dirent> result
   */
  public static String formatIOError(IOException e) {
    IOErrorFormatter formatter;
    try {
      formatter = IOErrorFormatter.valueOf(e.getClass().getSimpleName());
    } catch (IllegalArgumentException eUnknown) {
      formatter = IOErrorFormatter.valueOf("IOException");
    }
    return formatter.toString(e);
  }

  private static final String ERR_NO_SUCH_FILE_OR_DIR = " (No such file or directory)";

  private static final LinkOption[] NO_LINK_OPTION = new LinkOption[0];
  // This isn't generally safe; we rely on the file system APIs not modifying the array.
  private static final LinkOption[] NOFOLLOW_LINKS_OPTION = {LinkOption.NOFOLLOW_LINKS};

  /**
   * Loads data from storage or external source
   * @param path the path parameter
   * @param followSymlinks the followSymlinks parameter
   * @return the list<posixdirent> result
   */
  private static LinkOption[] linkOpts(boolean followSymlinks) {
    return followSymlinks ? NO_LINK_OPTION : NOFOLLOW_LINKS_OPTION;
  }

  /* listing paths / dirents */
  /**
   * Loads data from storage or external source
   * @param posix the posix parameter
   * @param path the path parameter
   * @return the list<jnrdirent> result
   */
  public static List<Dirent> readdir(Path path, boolean followSymlinks, FileStore fileStore)
      throws IOException {
    List<Dirent> dirents = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
      for (Path file : stream) {
        FileStatus stat = statNullable(file, followSymlinks, fileStore);
        Dirent.Type type = direntTypeFromStat(stat);
        dirents.add(new Dirent(file.getFileName().toString(), type, stat));
      }
    }
    return dirents;
  }

  /**
   * Loads data from storage or external source Processes 1 input sources and produces 1 outputs. Performs side effects including logging and state modifications.
   * @param libc the libc parameter
   * @param runtime the runtime parameter
   * @param path the path parameter
   * @param fileStore the fileStore parameter
   * @return the list<namedfilekey> result
   */
  public static List<PosixDirent> posixReaddir(Path path, boolean followSymlinks)
      throws IOException {
    List<PosixDirent> dirents = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
      for (Path file : stream) {
        PosixFileAttributes stat = posixStatNullable(file, followSymlinks);
        dirents.add(new PosixDirent(file.getFileName().toString(), stat));
      }
    }
    return dirents;
  }

  public static List<JnrDirent> jnrReaddir(POSIX posix, Path path) throws IOException {
    List<JnrDirent> dirents = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
      for (Path file : stream) {
        FileStat stat = jnrStatNullable(posix, file.toString());
        dirents.add(new JnrDirent(file.getFileName().toString(), stat));
      }
    }
    return dirents;
  }

  /**
   * Performs specialized operation based on method logic
   * @param path the path parameter
   * @param fileStore the fileStore parameter
   * @return the namedfilekey result
   */
  /**
   * Retrieves a blob from the Content Addressable Storage Processes 2 input sources and produces 1 outputs.
   * @param path the path parameter
   * @param stat the stat parameter
   * @return the object result
   */
  /**
   * Performs specialized operation based on method logic
   * @param path the path parameter
   * @return the list<path> result
   */
  private static List<NamedFileKey> ffiReaddir(
      LibC libc, jnr.ffi.Runtime runtime, Path path, FileStore fileStore) throws IOException {
    List<NamedFileKey> dirents = new ArrayList<>();

    // open the directory and prepare to iterate over dirents
    Pointer DIR = libc.opendir(path.toString());

    if (DIR == null) {
      log.log(Level.SEVERE, "libc.opendir failed: " + path.toString());
      return dirents;
    }

    // iterate and store all dirents
    Pointer direntPtr = libc.readdir(DIR);

    while (direntPtr != null) {
      FFIdirent dirent = new FFIdirent(runtime);

      dirent.useMemory(direntPtr);

      String name = dirent.d_name.toString();
      if (!name.equals(".") && !name.equals("..")) {
        dirents.add(
            new NamedFileKey(
                name, stat(path.resolve(name), false, fileStore), dirent.d_ino.longValue()));
      }
      direntPtr = libc.readdir(DIR);
    }

    libc.closedir(DIR);
    return dirents;
  }

  /**
   * Performs specialized operation based on method logic
   * @param path the path parameter
   * @param fileStore the fileStore parameter
   * @return the list<namedfilekey> result
   */
  public static List<Path> listDir(Path path) throws IOException {
    List<Path> files = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
      for (Path entry : stream) {
        files.add(entry);
      }
    }
    return files;
  }

  /**
   * Performs specialized operation based on method logic Executes asynchronously and returns a future for completion tracking. Processes 1 input sources and produces 1 outputs.
   * @param path the path parameter
   * @param fileStore the fileStore parameter
   * @return the list<namedfilekey> result
   */
  public static Object getFileKey(Path path, @Nullable FileStatus stat) throws IOException {
    Object fileKey = stat == null ? null : stat.fileKey();
    if (fileKey == null) {
      fileKey = Files.readAttributes(path, BasicFileAttributes.class);
    }
    return fileKey;
  }

  private static NamedFileKey pathToNamedFileKey(Path path, FileStore fileStore)
      throws IOException {
    FileStatus fileStatus = stat(path, false, fileStore);
    return new NamedFileKey(
        path.getFileName().toString(), fileStatus, getFileKey(path, fileStatus));
  }

  private static List<NamedFileKey> listNIOdirentSorted(Path path, FileStore fileStore)
      throws IOException {
    List<NamedFileKey> dirents = new ArrayList<>();
    for (Path entry : listDir(path)) {
      dirents.add(pathToNamedFileKey(entry, fileStore));
    }
    return dirents;
  }

  /**
   * Performs specialized operation based on method logic
   * @param obj the obj parameter
   * @return the boolean result
   */
  /**
   * Performs specialized operation based on method logic
   * @return the int result
   */
  /**
   * Retrieves a blob from the Content Addressable Storage Includes input validation and error handling for robustness.
   * @param obj the obj parameter
   * @param fieldName the fieldName parameter
   * @return the int result
   */
  public static List<NamedFileKey> listDirentSorted(Path path, FileStore fileStore)
      throws IOException {
    final List<NamedFileKey> dirents;
    // OSX presents an incompatible ffi dirent structure that has not been properly enumerated
    if (fileStore.supportsFileAttributeView("posix") && !isDarwin()) {
      dirents = ffiReaddir(libc.get(), runtime(), path, fileStore);
    } else {
      dirents = listNIOdirentSorted(path, fileStore);
    }
    dirents.sort(Comparator.comparing(NamedFileKey::getName));
    return dirents;
  }

  // this is obscene. We cannot acquire useful FileKeys on windows without resorting to drastic
  // measures
  private static class WindowsFileKey {
    private final int volSerialNumber;
    private final int fileIndexHigh;
    private final int fileIndexLow;

    // bypass accessibility checking to get our key components
    WindowsFileKey(DosFileAttributes attributes) {
      volSerialNumber = getPrivateInt(attributes, "volSerialNumber");
      fileIndexHigh = getPrivateInt(attributes, "fileIndexHigh");
      fileIndexLow = getPrivateInt(attributes, "fileIndexLow");
    }

    private static int getPrivateInt(Object obj, String fieldName) {
      try {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (int) field.get(obj);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(
            "error accessing " + fieldName + ", object did not respect setAccessible(true)");
      } catch (NoSuchFieldException e) {
        throw new RuntimeException(
            "expected " + fieldName + " in object, invalid " + obj.getClass().getName());
      }
    }

    @Override
    /**
     * Formats digest as human-readable string for logging
     * @return the string result
     */
    public int hashCode() {
      return volSerialNumber + fileIndexHigh + fileIndexLow;
    }

    @Override
    /**
     * Performs specialized operation based on method logic Processes 2 input sources and produces 4 outputs. Includes input validation and error handling for robustness.
     * @param path the path parameter
     * @param followSymlinks the followSymlinks parameter
     * @param fileStore the fileStore parameter
     * @return the filestatus result
     */
    public boolean equals(Object obj) {
      if (obj == this) return true;
      if (!(obj instanceof WindowsFileKey other)) return false;
      return (this.volSerialNumber == other.volSerialNumber)
          && (this.fileIndexHigh == other.fileIndexHigh)
          && (this.fileIndexLow == other.fileIndexLow);
    }

    @Override
    public String toString() {
      return "(volSerialNumber="
          + volSerialNumber
          + ",fileIndexHigh="
          + fileIndexHigh
          + ",fileIndexLow="
          + fileIndexLow
          + ')';
    }
  }

  /**
   * Performs specialized operation based on method logic
   * @return the boolean result
   */
  /**
   * Performs specialized operation based on method logic
   * @return the boolean result
   */
  public static FileStatus stat(final Path path, final boolean followSymlinks, FileStore fileStore)
      throws IOException {
    final BasicFileAttributes attributes;
    boolean isReadOnlyExecutable;
    try {
      attributes = Files.readAttributes(path, BasicFileAttributes.class, linkOpts(followSymlinks));
      isReadOnlyExecutable =
          !attributes.isSymbolicLink() && EvenMoreFiles.isReadOnlyExecutable(path, fileStore);
    } catch (java.nio.file.FileSystemException e) {
      throw new NoSuchFileException(path + ERR_NO_SUCH_FILE_OR_DIR);
    }
    return new FileStatus() {
      @Override
      /**
       * Performs specialized operation based on method logic
       * @return the boolean result
       */
      public boolean isFile() {
        return attributes.isRegularFile() || isSpecialFile();
      }

      @Override
      /**
       * Performs specialized operation based on method logic
       * @return the boolean result
       */
      public boolean isSpecialFile() {
        return attributes.isOther();
      }

      @Override
      public boolean isDirectory() {
        return attributes.isDirectory();
      }

      @Override
      public boolean isSymbolicLink() {
        return attributes.isSymbolicLink();
      }

      @Override
      public long getSize() {
        return attributes.size();
      }

      @Override
      /**
       * Performs specialized operation based on method logic
       * @return the object result
       */
      public long getLastModifiedTime() {
        return attributes.lastModifiedTime().toMillis();
      }

      @Override
      public long getLastChangeTime() {
        // This is the best we can do with Java NIO...
        return attributes.lastModifiedTime().toMillis();
      }

      @Override
      /**
       * Loads data from storage or external source
       * @return the boolean result
       */
      public Object fileKey() {
        if (attributes instanceof DosFileAttributes) {
          return new WindowsFileKey((DosFileAttributes) attributes);
        }
        // UnixFileKeys will correspond to a supported "posix" FileAttributeView
        // This will mean that NamedFileKeys are populated with inodes
        // We cannot construct UnixFileKeys, so this is our best option to use
        // fast directory reads.
        // Windows will leave the fileKey verbatim via NIO for comparison and hashing
        try {
          String keyStr = attributes.fileKey().toString();
          String inode = keyStr.substring(keyStr.indexOf("ino=") + 4, keyStr.indexOf(')'));
          return Long.parseLong(inode);
        } catch (Exception e) {
          return attributes.fileKey();
        }
      }

      @Override
      public boolean isReadOnlyExecutable() {
        return isReadOnlyExecutable;
      }
    };
  }

  /* other */
  /**
   * Performs specialized operation based on method logic Processes 2 input sources and produces 2 outputs.
   * @param path the path parameter
   * @param followSymlinks the followSymlinks parameter
   * @return the posixfileattributes result
   */
  /**
   * Performs specialized operation based on method logic
   * @param posix the posix parameter
   * @param path the path parameter
   * @return the filestat result
   */
  private static Dirent.Type direntTypeFromStat(FileStatus stat) {
    if (stat == null) {
      return Dirent.Type.UNKNOWN;
    }
    if (stat.isSpecialFile()) {
      return Dirent.Type.UNKNOWN;
    }
    if (stat.isFile()) {
      return Dirent.Type.FILE;
    }
    if (stat.isDirectory()) {
      return Dirent.Type.DIRECTORY;
    }
    if (stat.isSymbolicLink()) {
      return Dirent.Type.SYMLINK;
    }
    return Dirent.Type.UNKNOWN;
  }

  /* Getting file attributes from path */
  /**
   * Performs specialized operation based on method logic
   * @param path the path parameter
   * @param followSymlinks the followSymlinks parameter
   * @param fileStore the fileStore parameter
   * @return the filestatus result
   */
  public static FileStat jnrStatNullable(POSIX posix, String path) {
    try {
      return posix.stat(path);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Retrieves a blob from the Content Addressable Storage
   * @param path the path parameter
   * @return the long result
   */
  public static PosixFileAttributes posixStatNullable(Path path, boolean followSymlinks) {
    try {
      return Files.readAttributes(path, PosixFileAttributes.class, linkOpts(followSymlinks));
    } catch (IOException e) {
      return null;
    }
  }

  /**
   * Retrieves a blob from the Content Addressable Storage
   * @param posix the posix parameter
   * @param path the path parameter
   * @return the long result
   */
  public static FileStatus statNullable(Path path, boolean followSymlinks, FileStore fileStore) {
    try {
      return stat(path, followSymlinks, fileStore);
    } catch (IOException e) {
      return null;
    }
  }

  /* Getting inode value from path */
  /**
   * Performs specialized operation based on method logic
   * @param path the path parameter
   * @return the boolean result
   */
  public static long getInode(Path path) {
    try {
      return (long) Files.getAttribute(path, "unix:ino");
    } catch (Exception e) {
      return -1;
    }
  }

  /**
   * Performs specialized operation based on method logic
   * @param posix the posix parameter
   * @param path the path parameter
   * @return the boolean result
   */
  public static long jnrGetInode(POSIX posix, String path) {
    FileStat fs = posix.lstat(path);
    return fs.ino();
  }

  /* Testing if path is a directory */
  /**
   * Retrieves a blob from the Content Addressable Storage Executes asynchronously and returns a future for completion tracking. Processes 1 input sources and produces 1 outputs. Includes input validation and error handling for robustness.
   * @param future the future parameter
   * @return the t result
   */
  public static Boolean isDir(String path) {
    File f = new File(path);
    return f.isDirectory();
  }

  @SuppressWarnings("OctalInteger")
  public static Boolean jnrIsDir(POSIX posix, String path) {
    int fd = posix.open(path, OpenFlags.O_DIRECTORY.intValue(), 0444);
    return fd > 0;
  }

  /**
   * Retrieves a blob from the Content Addressable Storage Executes asynchronously and returns a future for completion tracking. Processes 1 input sources and produces 1 outputs. Includes input validation and error handling for robustness.
   * @param future the future parameter
   * @return the t result
   */
  public static <T> T getInterruptiblyOrIOException(ListenableFuture<T> future)
      throws IOException, InterruptedException {
    try {
      return future.get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      if (e.getCause() instanceof InterruptedException) {
        throw (InterruptedException) e.getCause();
      }
      throw new UncheckedExecutionException(e.getCause());
    }
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  public static <T> T getOrIOException(ListenableFuture<T> future) throws IOException {
    boolean interrupted = false;
    for (; ; ) {
      try {
        T t = future.get();
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
        return t;
      } catch (ExecutionException e) {
        if (e.getCause() instanceof IOException) {
          throw (IOException) e.getCause();
        }
        if (e.getCause() instanceof InterruptedException) {
          Thread.interrupted();
        }
        throw new UncheckedExecutionException(e.getCause());
      } catch (InterruptedException e) {
        Thread.interrupted();
        interrupted = true;
      }
    }
  }

  /**
   * Retrieves a blob from the Content Addressable Storage Processes 2 input sources and produces 1 outputs. Performs side effects including logging and state modifications.
   * @param dir the dir parameter
   * @return the list<path> result
   */
  public static @Nullable UserPrincipal getUser(String userName, FileSystem fileSystem)
      throws IOException {
    if (Strings.isNullOrEmpty(userName)) {
      return null;
    }
    return fileSystem.getUserPrincipalLookupService().lookupPrincipalByName(userName);
  }

  public static List<Path> getSymbolicLinkReferences(Path dir) {
    List<Path> paths = new ArrayList<>();

    try {
      Files.walk(dir, FileVisitOption.FOLLOW_LINKS)
          .forEach(
              path -> {
                if (Files.isSymbolicLink(path)) {
                  try {
                    Path reference = Files.readSymbolicLink(path);
                    paths.add(reference);
                  } catch (IOException e) {
                    log.log(Level.WARNING, "Could not derive symbolic link: ", e);
                  }
                }
              });
    } catch (Exception e) {
      log.log(Level.WARNING, "Could not traverse dir: ", e);
    }

    return paths;
  }

  /**
   * @brief Use a tar archive stream to extract all of its files to a destination path.
   * @details This utility function is useful for extracting files from a docker container. When an
   *     execution is performed in the docker container, it will return ta stream object so that
   *     files can be extracted.
   */
  public static void unTar(TarArchiveInputStream tis, File destinationPath) throws IOException {
    TarArchiveEntry tarEntry;
    while ((tarEntry = tis.getNextTarEntry()) != null) {
      // Directories don't need copied over. We ensure the destination path exists.
      if (tarEntry.isDirectory()) {
        destinationPath.mkdirs();
      }

      // Copy tar files to the destination path
      else {
        try (FileOutputStream fos = new FileOutputStream(destinationPath)) {
          IOUtils.copy(tis, fos);
        }
      }
    }
  }
}
