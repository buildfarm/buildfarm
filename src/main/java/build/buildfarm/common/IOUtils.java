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

package build.buildfarm.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import jnr.constants.platform.OpenFlags;
import jnr.ffi.Pointer;
import jnr.posix.FileStat;
import jnr.posix.POSIX;
import java.util.logging.Level;
import java.util.logging.Logger;

public class IOUtils {
  private static final Logger logger = Logger.getLogger(IOUtils.class.getName());
  private IOUtils() {}

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
  private static final LinkOption[] NOFOLLOW_LINKS_OPTION =
      new LinkOption[] {LinkOption.NOFOLLOW_LINKS};

  private static LinkOption[] linkOpts(boolean followSymlinks) {
    return followSymlinks ? NO_LINK_OPTION : NOFOLLOW_LINKS_OPTION;
  }

  /* listing paths / dirents */
  public static List<Dirent> readdir(Path path, boolean followSymlinks) throws IOException {
    List<Dirent> dirents = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
      for (Path file : stream) {
        FileStatus stat = statNullable(file, followSymlinks);
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

  public static List<Inode> ffiReaddir(LibC libc, jnr.ffi.Runtime runtime, Path path)
      throws IOException {

    List<Inode> dirents = new ArrayList<>();

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

      if (!dirent.d_name.toString().equals(".") && !dirent.d_name.toString().equals("..")) {

        dirents.add(new Inode(dirent.d_name.toString(), dirent.d_ino.longValue()));
      }
      direntPtr = libc.readdir(DIR);
    }

    int return_value = libc.closedir(DIR);
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

  /* getting paths / dirents sorted */
  public static List<Path> listDirSorted(Path path) {
    try {
      List<Path> files = listDir(path);
      files.sort(Comparator.comparing(Path::toString));
      return files;
    } catch (IOException e) {
    }
    return null;
  }

  public static List<PosixDirent> listDirentSorted(Path path) {
    List<PosixDirent> sortedDirent;
    try {
      sortedDirent = posixReaddir(path, /* followSymlinks= */ false);
    } catch (IOException e) {
      return null;
    }
    sortedDirent.sort(Comparator.comparing(PosixDirent::getName));
    return sortedDirent;
  }

  public static List<JnrDirent> listJnrDirentSorted(POSIX posix, Path path) {
    List<JnrDirent> sortedDirent;
    try {
      sortedDirent = jnrReaddir(posix, path);
    } catch (IOException e) {
      return null;
    }
    sortedDirent.sort(Comparator.comparing(JnrDirent::getName));
    return sortedDirent;
  }

  public static List<Inode> listFFIdirentSorted(LibC libc, jnr.ffi.Runtime runtime, Path path) {
    List<Inode> sortedDirent;
    try {
      sortedDirent = ffiReaddir(libc, runtime, path);
    } catch (IOException e) {
      return null;
    }
    sortedDirent.sort(Comparator.comparing(Inode::getName));
    return sortedDirent;
  }

  /*
   * calling java stat
   * Like stat(), but returns null on failures instead of throwing.
   */
  public static FileStatus stat(final Path path, final boolean followSymlinks) throws IOException {
    final BasicFileAttributes attributes;
    try {
      attributes = Files.readAttributes(path, BasicFileAttributes.class, linkOpts(followSymlinks));
    } catch (java.nio.file.FileSystemException e) {
      throw new FileNotFoundException(path + ERR_NO_SUCH_FILE_OR_DIR);
    }
    FileStatus status =
        new FileStatus() {
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
          public long getSize() throws IOException {
            return attributes.size();
          }

          @Override
          public long getLastModifiedTime() throws IOException {
            return attributes.lastModifiedTime().toMillis();
          }

          @Override
          public long getLastChangeTime() {
            // This is the best we can do with Java NIO...
            return attributes.lastModifiedTime().toMillis();
          }

          @Override
          public Object fileKey() {
            return attributes.fileKey();
          }
        };

    return status;
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

  public static FileStatus statNullable(Path path, boolean followSymlinks) {
    try {
      return stat(path, followSymlinks);
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

  public static Boolean jnrIsDir(POSIX posix, String path) {
    int fd = posix.open(path, OpenFlags.O_DIRECTORY.intValue(), 0444);
    return fd > 0;
  }
}
