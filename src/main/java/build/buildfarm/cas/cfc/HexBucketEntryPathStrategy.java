package build.buildfarm.cas.cfc;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

import build.buildfarm.v1test.Digest;
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
  public Path getPath(Digest digest, String fileName) {
    // Bucket on the digest's hash, which is always the bare hex the fileName was computed from.
    // Taking it from the digest rather than parsing fileName keeps bucketing oblivious to the
    // `<digestfn>_` prefix (e.g. `blake3_`) and the `_exec`/`_dir` suffixes a fileName may carry.
    Path keyPath = path;
    if (levels > 0) {
      String hash = digest.getHash();
      for (int i = 0; i < levels; i++) {
        keyPath = keyPath.resolve(hash.substring(i * 2, i * 2 + 2));
      }
    }
    return keyPath.resolve(fileName);
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
