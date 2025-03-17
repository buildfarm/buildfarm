package build.buildfarm.worker;

import static com.google.common.base.Preconditions.checkNotNull;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.worker.ExecFileSystem.ExecDirectoryAttributes;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Map;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Setter;

class ExecTreeWalker implements Closeable {
  enum EventType {
    START_DIRECTORY,
    END_DIRECTORY,
    ENTRY,
  }

  @Data
  static final class Event {
    private final EventType type;
    private final Path path;
    private final BasicFileAttributes attributes;
    private final IOException ioe;

    private Event(EventType type, Path path, BasicFileAttributes attributes, IOException ioe) {
      this.type = type;
      this.path = path;
      this.attributes = attributes;
      this.ioe = ioe;
    }

    private Event(EventType type, Path path, BasicFileAttributes attributes) {
      this(type, path, attributes, /* ioe= */ null);
    }
  }

  @Data
  static class Entry {
    private final Path path;
    private final BasicFileAttributes attributes;

    public Entry(Path path, BasicFileAttributes attributes) {
      this.path = checkNotNull(path);
      this.attributes = checkNotNull(attributes);
    }
  }

  @Data
  private static class DirectoryNode {
    private final Entry directory;
    private final DirectoryStream<Entry> stream;
    private final Iterator<Entry> iterator;

    @Setter(AccessLevel.NONE)
    private boolean skipped;

    DirectoryNode(Entry directory, DirectoryStream<Entry> stream) {
      this.directory = directory;
      this.stream = stream;
      this.iterator = stream.iterator();
    }

    void skip() {
      skipped = true;
    }
  }

  private final Map<Digest, Directory> index;
  private final DigestFunction.Value digestFunction;
  private final ArrayDeque<DirectoryNode> stack = new ArrayDeque<>();
  private boolean closed;

  ExecTreeWalker(Map<Digest, Directory> index, DigestFunction.Value digestFunction) {
    this.index = index;
    this.digestFunction = digestFunction;
  }

  Event walk(Path file, Digest digest) {
    if (closed) {
      throw new IllegalStateException("Closed");
    }

    return checkNotNull(visit(new Entry(file, new ExecDirectoryAttributes(digest))));
  }

  private Event visit(Entry entry) {
    if (!entry.getAttributes().isDirectory()) {
      return new Event(EventType.ENTRY, entry.getPath(), entry.getAttributes());
    }

    // file is a directory, stream it
    Digest digest = (Digest) entry.getAttributes().fileKey();
    Directory directory = index.get(digest);
    if (directory == null) {
      return new Event(
          EventType.ENTRY,
          entry.getPath(),
          entry.getAttributes(),
          new NoSuchFileException(
              DigestUtil.toString(DigestUtil.fromDigest(digest, digestFunction))));
    }
    DirectoryStream<Entry> stream = new ExecDirectoryStream(directory, entry.getPath());

    // push a directory node to the stack and return an event
    stack.push(new DirectoryNode(entry, stream));
    return new Event(EventType.START_DIRECTORY, entry.getPath(), entry.getAttributes());
  }

  Event next() {
    DirectoryNode top = stack.peek();
    if (top == null) {
      return null; // stack is empty, we are done
    }

    // continue iteration of the directory at the top of the stack
    Event ev;
    do {
      Entry entry = null;
      IOException ioe = null;

      if (!top.isSkipped()) {
        Iterator<Entry> iterator = top.getIterator();
        try {
          if (iterator.hasNext()) {
            entry = iterator.next();
          }
        } catch (DirectoryIteratorException x) {
          ioe = x.getCause();
        }
      }

      // no next entry so close and pop directory,
      // creating corresponding event
      if (entry == null) {
        try {
          top.getStream().close();
        } catch (IOException e) {
          if (ioe == null) {
            ioe = e;
          } else {
            ioe.addSuppressed(e);
          }
        }
        stack.pop();
        entry = top.getDirectory();
        return new Event(EventType.END_DIRECTORY, entry.getPath(), entry.getAttributes(), ioe);
      }

      // visit the entry
      ev = visit(entry);
    } while (ev == null);

    return ev;
  }

  /**
   * Pops the directory node that is the current top of the stack so that there are no more events
   * for the directory (including no END_DIRECTORY) event. This method is a no-op if the stack is
   * empty or the walker is closed.
   */
  void pop() {
    if (!stack.isEmpty()) {
      DirectoryNode node = stack.pop();
      try {
        node.getStream().close();
      } catch (IOException ignore) {
      }
    }
  }

  /**
   * Skips the remaining entries in the directory at the top of the stack. This method is a no-op if
   * the stack is empty or the walker is closed.
   */
  void skipRemainingSiblings() {
    if (!stack.isEmpty()) {
      stack.peek().skip();
    }
  }

  @Override
  public void close() {
    if (!closed) {
      while (!stack.isEmpty()) {
        pop();
      }
      closed = true;
    }
  }
}
