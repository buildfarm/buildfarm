package build.buildfarm.cas.cfc;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;

class HexBucketEntryPathStrategy implements EntryPathStrategy {
  private static final int MAX_LEVEL = 4;
  private final Path path;
  private final int levels;

  private static long depthMaxCounter(int depth) {
    return (1L << (depth * 8)) - 1;
  }

  HexBucketEntryPathStrategy(Path path, int levels) {
    checkState(levels <= MAX_LEVEL);
    this.path = path;
    this.levels = levels;
  }

  @Override
  public Path getPath(String key) {
    // Bucket on the hex hash, skipping any `<digestfn>_` prefix. Without
    // this, a BLAKE3 key like `blake3_220fcb...` would shard on `bl/ak/`,
    // which aren't hex bucket directories, and every read/write would land
    // in a non-existent path.
    int hashStart = levels == 0 ? 0 : hexHashStart(key);
    checkState(levels == 0 || hashStart >= 0);
    Path keyPath = path;
    for (int i = 0; i < levels; i++) {
      int from = hashStart + i * 2;
      keyPath = keyPath.resolve(key.substring(from, from + 2));
    }
    return keyPath.resolve(key);
  }

  /**
   * Returns the offset of the hex hash within {@code key}, skipping an optional {@code <digestfn>_}
   * prefix (e.g. {@code blake3_}), or {@code -1} if no hex hash of at least {@code levels * 2}
   * digits sits at a bucketable position.
   *
   * <p>This is the allocation-free, backtracking-free equivalent of matching {@code
   * (?:[a-z0-9]+_)?([0-9a-f]{levels*2}.*)} and reading the start of group 1: an optional prefix is
   * a maximal run of {@code [a-z0-9]} terminated by {@code _}, preferred over no prefix when both
   * leave a hex hash. The hash captures bare keys from the omitted digest functions (SHA*, MD5) as
   * well as prefixed ones (BLAKE3), and a trailing {@code _exec}/{@code _dir} suffix stays part of
   * the leaf filename rather than being mistaken for a prefix.
   */
  private int hexHashStart(String key) {
    int width = levels * 2;
    // Optional `<digestfn>_` prefix: a non-empty run of [a-z0-9] then '_'.
    int prefixEnd = 0;
    while (prefixEnd < key.length() && isLowerAlnum(key.charAt(prefixEnd))) {
      prefixEnd++;
    }
    if (prefixEnd > 0
        && prefixEnd < key.length()
        && key.charAt(prefixEnd) == '_'
        && isHexRun(key, prefixEnd + 1, width)) {
      return prefixEnd + 1;
    }
    // No prefix: a bare hex hash, as written by the omitted digest functions.
    return isHexRun(key, 0, width) ? 0 : -1;
  }

  private static boolean isHexRun(String s, int from, int length) {
    if (from + length > s.length()) {
      return false;
    }
    for (int i = from; i < from + length; i++) {
      char c = s.charAt(i);
      if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
        return false;
      }
    }
    return true;
  }

  private static boolean isLowerAlnum(char c) {
    return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9');
  }

  @Override
  public Iterable<Path> branchDirectories() {
    return () ->
        new Iterator<Path>() {
          int depth = 0;
          int index = 0;

          @Override
          public boolean hasNext() {
            return depth != levels && index < depthMaxCounter(levels);
          }

          @Override
          public Path next() {
            Path nextPath = path;
            long nextIndex = index++;
            for (int i = 0; i < depth; i++) {
              nextPath = nextPath.resolve(format("%02x", nextIndex & 0xff));
              nextIndex >>= 8;
            }
            if (depth == 0 || (depth <= levels && index > depthMaxCounter(depth))) {
              depth++;
              index = 0;
            }
            return nextPath;
          }
        };
  }

  @SuppressWarnings("NullableProblems")
  @Override
  public Iterator<Path> iterator() {
    return new Iterator<>() {
      long counter;

      @Override
      public boolean hasNext() {
        return counter <= depthMaxCounter(levels);
      }

      @Override
      public Path next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        long index = counter++;
        Path nextPath = path;
        for (int i = 0; i < levels; i++) {
          nextPath = nextPath.resolve(format("%02x", index & 0xff));
          index >>= 8;
        }
        return nextPath;
      }
    };
  }
}
