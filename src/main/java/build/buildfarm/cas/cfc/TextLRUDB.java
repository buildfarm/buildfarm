package build.buildfarm.cas.cfc;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

import build.buildfarm.common.io.AtomicFileWriter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;

class TextLRUDB implements LRUDB {
  private static class EntriesIterator implements Iterator<SizeEntry> {
    private String next = null;
    private final BufferedReader br;

    EntriesIterator(BufferedReader br) {
      this.br = br;
    }

    @Override
    public boolean hasNext() {
      if (next == null) {
        try {
          next = br.readLine();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      return next != null;
    }

    private SizeEntry parse(String line) {
      int sep = line.indexOf(',');
      checkState(sep != -1);
      return new SizeEntry(line.substring(0, sep), Long.parseLong(line.substring(sep + 1)));
    }

    @Override
    public SizeEntry next() {
      if (next == null) {
        throw new NoSuchElementException();
      }
      SizeEntry sizeEntry = parse(next);
      next = null;
      return sizeEntry;
    }
  }

  @Override
  public Iterable<SizeEntry> entries(BufferedReader br) throws IOException {
    return new Iterable<>() {
      @Override
      public Iterator<SizeEntry> iterator() {
        return new EntriesIterator(br);
      }
    };
  }

  @Override
  public void save(Iterator<SizeEntry> entries, Path path) throws IOException {
    try (AtomicFileWriter atomicWriter = new AtomicFileWriter(path)) {
      BufferedWriter writer = atomicWriter.getWriter();
      while (entries.hasNext()) {
        SizeEntry entry = entries.next();
        writer.write(format("%s,%d\n", entry.key(), entry.size()));
      }
    }
  }
}
