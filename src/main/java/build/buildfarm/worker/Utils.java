package build.buildfarm.worker;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.logging.Level.SEVERE;

import com.google.common.util.concurrent.ListenableFuture;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.logging.Logger;

public class Utils {
  private static final Logger logger = Logger.getLogger(Utils.class.getName());

  private static final String ERR_NO_SUCH_FILE_OR_DIR = " (No such file or directory)";

  private static final LinkOption[] NO_LINK_OPTION = new LinkOption[0];
  // This isn't generally safe; we rely on the file system APIs not modifying the array.
  private static final LinkOption[] NOFOLLOW_LINKS_OPTION =
      new LinkOption[] { LinkOption.NOFOLLOW_LINKS };

  private Utils() { }

  public static ListenableFuture<Void> removeDirectory(Path path, ExecutorService service) {
    String suffix = UUID.randomUUID().toString();
    Path filename = path.getFileName();
    String tmpFilename = filename + ".tmp." + suffix;
    Path tmpPath = path.resolveSibling(tmpFilename);
    try {
      // rename must be synchronous to call
      Files.move(path, tmpPath);
    } catch (IOException e) {
      return immediateFailedFuture(e);
    }
    return listeningDecorator(service).submit(() -> {
      try {
        removeDirectory(tmpPath);
      } catch (IOException e) {
        logger.log(SEVERE, "error removing directory", e);
      }
    }, null);
  }

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

  private static LinkOption[] linkOpts(boolean followSymlinks) {
    return followSymlinks ? NO_LINK_OPTION : NOFOLLOW_LINKS_OPTION;
  }

  /**
   * Returns the status of a file.
   */
  private static FileStatus stat(final Path path, final boolean followSymlinks) throws IOException {
    final BasicFileAttributes attributes;
    try {
      attributes =
          Files.readAttributes(path, BasicFileAttributes.class, linkOpts(followSymlinks));
    } catch (java.nio.file.FileSystemException e) {
      throw new FileNotFoundException(path + ERR_NO_SUCH_FILE_OR_DIR);
    }
    FileStatus status = new FileStatus() {
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

  public static FileStatus statIfFound(Path path, boolean followSymlinks) {
    try {
      return stat(path, followSymlinks);
    } catch (FileNotFoundException e) {
      return null;
    } catch (IOException e) {
      // If this codepath is ever hit, then this method should be rewritten to properly distinguish
      // between not-found exceptions and others.
      throw new IllegalStateException(e);
    }
  }

  /**
   * Like stat(), but returns null on failures instead of throwing.
   */
  private static FileStatus statNullable(Path path, boolean followSymlinks) {
    try {
      return stat(path, followSymlinks);
    } catch (IOException e) {
      return null;
    }
  }

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

  public static List<Dirent> readdir(Path path, boolean followSymlinks) throws IOException {
    List<Dirent> dirents = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
      for (Path file : stream) {
        FileStatus stat = statNullable(file, followSymlinks);
        Dirent.Type type = direntTypeFromStat(statNullable(file, followSymlinks));
        dirents.add(new Dirent(file.getFileName().toString(), type, stat));
      }
    }
    return dirents;
  }
}
