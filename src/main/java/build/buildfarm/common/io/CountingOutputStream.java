package build.buildfarm.common.io;

import java.io.IOException;
import java.io.OutputStream;

public class CountingOutputStream extends OutputStream {
  private long written;
  OutputStream out;
  private boolean inWrite = false;

  public CountingOutputStream(long written, OutputStream out) {
    this.written = written;
    this.out = out;
  }

  public long written() {
    return written;
  }

  @Override
  public void write(int b) throws IOException {
    boolean count = !inWrite;
    inWrite = true;
    out.write(b);
    if (count) {
      written++;
      inWrite = false;
    }
  }

  @Override
  public void write(byte[] b) throws IOException {
    boolean count = !inWrite;
    inWrite = true;
    out.write(b);
    if (count) {
      written += b.length;
      inWrite = false;
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    boolean count = !inWrite;
    inWrite = true;
    out.write(b, off, len);
    if (count) {
      written += len;
      inWrite = false;
    }
  }

  @Override
  public void flush() throws IOException {
    out.flush();
  }

  @Override
  public void close() throws IOException {
    out.close();
  }
}
