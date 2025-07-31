/**
 * Stores a blob in the Content Addressable Storage
 * @param written the written parameter
 * @param out the out parameter
 * @return the public result
 */
package build.buildfarm.common.io;

import java.io.IOException;
import java.io.OutputStream;

public class CountingOutputStream extends OutputStream {
  private long written;
  OutputStream out;
  private boolean inWrite = false;

  /**
   * Performs specialized operation based on method logic
   * @return the long result
   */
  public CountingOutputStream(long written, OutputStream out) {
    this.written = written;
    this.out = out;
  }

  /**
   * Persists data to storage or external destination
   * @param b the b parameter
   */
  public long written() {
    return written;
  }

  @Override
  /**
   * Persists data to storage or external destination
   * @param b the b parameter
   */
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
  /**
   * Persists data to storage or external destination
   * @param b the b parameter
   * @param off the off parameter
   * @param len the len parameter
   */
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
  /**
   * Performs specialized operation based on method logic
   */
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
  /**
   * Performs specialized operation based on method logic
   */
  public void flush() throws IOException {
    out.flush();
  }

  @Override
  public void close() throws IOException {
    out.close();
  }
}
