// Copyright 2019 The Bazel Authors. All rights reserved.
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
import java.util.logging.Logger;
import javax.annotation.Nullable;
import jnr.constants.platform.OpenFlags;
import jnr.ffi.LibraryLoader;
import jnr.ffi.Pointer;
import jnr.posix.FileStat;
import jnr.posix.POSIX;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;

public class Utils {
  private static final Logger logger = Logger.getLogger(Utils.class.getName());

  @SuppressWarnings("Guava")
  private static final Supplier<LibC> libc =
      Suppliers.memoize(() -> LibraryLoader.create(LibC.class).load("c"));

  // pretty poor check here, but avoiding apache commons for now
  @SuppressWarnings("Guava")
  private static final Supplier<Boolean> isMacOS =
      Suppliers.memoize(
          () -> {
            String osName = System.getProperty("os.name").toLowerCase();
            return osName.startsWith("mac");
          });

  private static jnr.ffi.Runtime runtime() {
    return jnr.ffi.Runtime.getRuntime(libc.get());
  }

  private Utils() {}

  enum IOErrorFormatter {
    AccessDeniedException("access denied"),
    FileSystemException(""),
    IOException(""),
    NoSuchFileException("no such file");

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

  private static LinkOption[] linkOpts(boolean followSymlinks) {
    return followSymlinks ? NO_LINK_OPTION : NOFOLLOW_LINKS_OPTION;
  }

  /* listing paths / dirents */
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

  private static List<NamedFileKey> ffiReaddir(
      LibC libc, jnr.ffi.Runtime runtime, Path path, FileStore fileStore) throws IOException {
    List<NamedFileKey> dirents = new ArrayList<>();

    // open the directory and prepare to iterate over dirents
    Pointer DIR = libc.opendir(path.toString());

    if (DIR == null) {
      logger.log(Level.SEVERE, "libc.opendir failed: " + path.toString());
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

  public static List<Path> listDir(Path path) throws IOException {
    List<Path> files = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
      for (Path entry : stream) {
        files.add(entry);
      }
    }
    return files;
  }

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

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static List<NamedFileKey> listNIOdirentSorted(Path path, FileStore fileStore)
      throws IOException {
    List<NamedFileKey> dirents = new ArrayList();
    for (Path entry : listDir(path)) {
      dirents.add(pathToNamedFileKey(entry, fileStore));
    }
    return dirents;
  }

  public static List<NamedFileKey> listDirentSorted(Path path, FileStore fileStore)
      throws IOException {
    final List<NamedFileKey> dirents;
    // OSX presents an incompatible ffi dirent structure that has not been properly enumerated
    if (fileStore.supportsFileAttributeView("posix") && !isMacOS.get()) {
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
    public int hashCode() {
      return volSerialNumber + fileIndexHigh + fileIndexLow;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) return true;
      if (!(obj instanceof WindowsFileKey)) return false;
      WindowsFileKey other = (WindowsFileKey) obj;
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

  public static FileStatus stat(final Path path, final boolean followSymlinks, FileStore fileStore)
      throws IOException {
    final BasicFileAttributes attributes;
    boolean isReadOnlyExecutable;
    try {
      attributes = Files.readAttributes(path, BasicFileAttributes.class, linkOpts(followSymlinks));
      isReadOnlyExecutable = EvenMoreFiles.isReadOnlyExecutable(path, fileStore);
    } catch (java.nio.file.FileSystemException e) {
      throw new NoSuchFileException(path + ERR_NO_SUCH_FILE_OR_DIR);
    }
    return new FileStatus() {
      @Override
      public boolean isFile() {
        return attributes.isRegularFile() || isSpecialFile();
      }

      @Override
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
      public long getLastModifiedTime() {
        return attributes.lastModifiedTime().toMillis();
      }

      @Override
      public long getLastChangeTime() {
        // This is the best we can do with Java NIO...
        return attributes.lastModifiedTime().toMillis();
      }

      @Override
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
  public static FileStat jnrStatNullable(POSIX posix, String path) {
    try {
      return posix.stat(path);
    } catch (Exception e) {
      return null;
    }
  }

  public static PosixFileAttributes posixStatNullable(Path path, boolean followSymlinks) {
    try {
      return Files.readAttributes(path, PosixFileAttributes.class, linkOpts(followSymlinks));
    } catch (IOException e) {
      return null;
    }
  }

  public static FileStatus statNullable(Path path, boolean followSymlinks, FileStore fileStore) {
    try {
      return stat(path, followSymlinks, fileStore);
    } catch (IOException e) {
      return null;
    }
  }

  /* Getting inode value from path */
  public static long getInode(Path path) {
    try {
      return (long) Files.getAttribute(path, "unix:ino");
    } catch (Exception e) {
      return -1;
    }
  }

  public static long jnrGetInode(POSIX posix, String path) {
    FileStat fs = posix.lstat(path);
    return fs.ino();
  }

  /* Testing if path is a directory */
  public static Boolean isDir(String path) {
    File f = new File(path);
    return f.isDirectory();
  }

  @SuppressWarnings("OctalInteger")
  public static Boolean jnrIsDir(POSIX posix, String path) {
    int fd = posix.open(path, OpenFlags.O_DIRECTORY.intValue(), 0444);
    return fd > 0;
  }

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

  public static @Nullable UserPrincipal getUser(String userName, FileSystem fileSystem)
      throws IOException {
    if (Strings.isNullOrEmpty(userName)) {
      return null;
    }
    return fileSystem.getUserPrincipalLookupService().lookupPrincipalByName(userName);
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
