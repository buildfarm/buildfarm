package build.buildfarm.proxy.http;

import java.io.OutputStream;

abstract class ChunkOutputStream extends OutputStream {
  private final byte[] buffer;
  int buflen = 0;

  ChunkOutputStream(int size) {
    buffer = new byte[size];
  }

  abstract void onChunk(byte[] b, int off, int len);

  @Override
  public void close() {
    flush();
  }

  @Override
  public void flush() {
    if (buflen > 0) {
      onChunk(buffer, 0, buflen);
      buflen = 0;
    }
  }

  @Override
  public void write(byte[] b) {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) {
    while (buflen + len >= buffer.length) {
      int copylen = buffer.length - buflen;
      System.arraycopy(b, off, buffer, buflen, copylen);
      buflen = buffer.length;
      flush();
      len -= copylen;
      off += copylen;
      if (len == 0) {
        return;
      }
    }
    System.arraycopy(b, off, buffer, buflen, len);
    buflen += len;
  }

  @Override
  public void write(int b) {
    buffer[buflen++] = (byte) b;
    if (buflen == buffer.length) {
      flush();
    }
  }
};

