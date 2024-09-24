package build.buildfarm.cas.cfc;

import static com.google.common.io.MoreFiles.asCharSink;
import static com.google.common.io.MoreFiles.asCharSource;

import build.buildfarm.v1test.Digest;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Set;

/**
 * Abstract class for file directory index.
 *
 * <p>Provides file backed mappings for directories -> entries.
 */
abstract class FileDirectoriesIndex implements DirectoriesIndex {
  private static final Charset UTF_8 = StandardCharsets.UTF_8;

  final EntryPathStrategy entryPathStrategy;

  FileDirectoriesIndex(EntryPathStrategy entryPathStrategy) {
    this.entryPathStrategy = entryPathStrategy;
  }

  Path path(Digest digest) {
    return entryPathStrategy.getPath(digest.getHash() + "_dir_inputs");
  }

  @Override
  public Iterable<String> directoryEntries(Digest directory) throws IOException {
    try {
      return asCharSource(path(directory), UTF_8).readLines();
    } catch (NoSuchFileException e) {
      return ImmutableList.of();
    }
  }

  @Override
  public void put(Digest directory, Iterable<String> entries) throws IOException {
    asCharSink(path(directory), UTF_8).writeLines(entries);
  }

  @Override
  public void remove(Digest directory) throws IOException {
    try {
      Files.delete(path(directory));
    } catch (NoSuchFileException e) {
      // ignore
    }
  }

  protected void removeDirectories(Set<Digest> directories) throws IOException {
    for (Digest directory : directories) {
      try {
        Files.delete(path(directory));
      } catch (NoSuchFileException e) {
        // ignore
      }
    }
  }
}
