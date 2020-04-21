package build.buildfarm.worker.cgroup;

import build.buildfarm.worker.WorkerContext.IOResource;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;

abstract class Controller implements IOResource {
  protected final Group group;

  private boolean opened = false;

  Controller(Group group) {
    this.group = group;
  }

  public abstract String getName();

  protected final Path getPath() {
    return group.getPath(getName());
  }

  protected final void open() throws IOException {
    if (!opened) {
      group.create(getName());
      opened = true;
    }
  }

  @Override
  public void close() throws IOException {
    Path path = getPath();
    boolean exists = true;
    while (exists) {
      try {
        group.waitUntilEmpty(getName());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
      try {
        Files.delete(path);
        exists = false;
      } catch (IOException e) {
        exists = Files.exists(path);
        if (exists && !e.getMessage().endsWith("Device or resource busy")) {
          throw e;
        }
      }
    }
    opened = false;
  }

  protected void writeInt(String propertyName, int value) throws IOException {
    Path path = getPath().resolve(propertyName);
    try (Writer out = new OutputStreamWriter(Files.newOutputStream(path))) {
      out.write(String.format("%d\n", value));
    }
  }

  protected int readInt(String propertyName) throws IOException {
    char[] data = new char[32];
    Path path = getPath().resolve(propertyName);
    int len;
    try (Reader in = new InputStreamReader(Files.newInputStream(path))) {
      len = in.read(data);
    }
    if (len < 0) {
      throw new NumberFormatException("premature end of stream");
    }
    if (len == 0 || data[0] == '\n' || data[len - 1] != '\n') {
      throw new NumberFormatException("invalid integer in '" + propertyName + "'");
    }
    return Integer.parseInt(String.copyValueOf(data, 0, len - 1));
  }
}
