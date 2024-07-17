package build.buildfarm.worker;

import static com.google.common.base.Preconditions.checkNotNull;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.buildfarm.worker.ExecTreeWalker.Event;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Path;
import java.util.Map;

class ExecTree {
  private final Map<Digest, Directory> index;

  ExecTree(Map<Digest, Directory> index) {
    this.index = index;
  }

  public Path walk(Path start, Digest digest, FileVisitor<? super Path> visitor)
      throws IOException {
    try (ExecTreeWalker walker = new ExecTreeWalker(index)) {
      Event ev = walker.walk(start, digest);
      do {
        FileVisitResult result =
            switch (ev.getType()) {
              case ENTRY -> {
                IOException ioe = ev.getIoe();
                if (ioe == null) {
                  yield visitor.visitFile(ev.getPath(), checkNotNull(ev.getAttributes()));
                } else {
                  yield visitor.visitFileFailed(ev.getPath(), ioe);
                }
              }
              case START_DIRECTORY -> {
                FileVisitResult res = visitor.preVisitDirectory(ev.getPath(), ev.getAttributes());

                // if SKIP_SIBLINGS and SKIP_SUBTREE is returned then
                // there shouldn't be any more events for the current
                // directory.
                if (res == FileVisitResult.SKIP_SUBTREE || res == FileVisitResult.SKIP_SIBLINGS) {
                  walker.pop();
                }
                yield res;
              }
              case END_DIRECTORY -> {
                FileVisitResult res = visitor.postVisitDirectory(ev.getPath(), ev.getIoe());

                // SKIP_SIBLINGS is a no-op for postVisitDirectory
                if (res == FileVisitResult.SKIP_SIBLINGS) {
                  res = FileVisitResult.CONTINUE;
                }
                yield res;
              }
              default -> throw new AssertionError("Should not get here");
            };

        if (checkNotNull(result) != FileVisitResult.CONTINUE) {
          if (result == FileVisitResult.TERMINATE) {
            break;
          }
          if (result == FileVisitResult.SKIP_SIBLINGS) {
            walker.skipRemainingSiblings();
          }
        }
        ev = walker.next();
      } while (ev != null);
    }
    return start;
  }
}
