package build.buildfarm.proxy.http;

import java.io.IOException;
import java.io.OutputStream;

class SkipLimitOutputStream extends OutputStream {
  private final OutputStream out;
  private long skipBytesRemaining;
  private long writeBytesRemaining;

  SkipLimitOutputStream(OutputStream out, long skipBytes, long writeBytes) {
    this.out = out;
    skipBytesRemaining = skipBytes;
    writeBytesRemaining = writeBytes;
  }

  @Override
  public void close() throws IOException {
    writeBytesRemaining = 0;

    out.close();
  }

  @Override
  public void flush() throws IOException {
    out.flush();
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (skipBytesRemaining >= len) {
      skipBytesRemaining -= len;
    } else if (writeBytesRemaining > 0) {
      off += skipBytesRemaining;
      // technically an error to int-cast
      len = Math.min((int) writeBytesRemaining, (int) (len - skipBytesRemaining));
      out.write(b, off, len);
      skipBytesRemaining = 0;
      writeBytesRemaining -= len;
    }
  }

  @Override
  public void write(int b) throws IOException {
    if (skipBytesRemaining > 0) {
      skipBytesRemaining--;
    } else if (writeBytesRemaining > 0) {
      out.write(b);
      writeBytesRemaining--;
    }
  }
}
