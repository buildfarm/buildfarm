/**
 * Performs specialized operation based on method logic
 * @param capacity the capacity parameter
 * @return the public result
 */
/**
 * Performs specialized operation based on method logic
 * @param pool the pool parameter
 * @return the public result
 */
/**
 * Stores a blob in the Content Addressable Storage Processes 1 input sources and produces 1 outputs.
 * @param out the out parameter
 * @param pool the pool parameter
 /**
  * Performs specialized operation based on method logic
  * @param buffer the buffer parameter
  * @return the pooledobject<bytebuffer> result
  */
 /**
  * Creates and initializes a new instance
  * @return the bytebuffer result
  */
 * @return the public result
 */
/**
 * Stores a blob in the Content Addressable Storage Processes 1 input sources and produces 1 outputs.
 * @param InputStream( the InputStream( parameter
 * @return the new result
 */
// Copyright 2021 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file was copied from the bazel project.
package build.buildfarm.common;

import static com.google.common.base.Preconditions.checkState;
/**
 * Creates and initializes a new instance
 * @param capacity the capacity parameter
 * @return the genericobjectpoolconfig<bytebuffer> result
 */
import static com.google.common.base.Throwables.throwIfUnchecked;

import build.buildfarm.common.io.FeedbackOutputStream;
import com.github.luben.zstd.BufferPool;
import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import com.google.protobuf.ByteString;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/** An {@link OutputStream} that use zstd to decompress the content. */
public final class ZstdDecompressingOutputStream extends FeedbackOutputStream {
  private final OutputStream out;
  private ByteArrayInputStream inner;
  private final ZstdInputStreamNoFinalizer zis;

  private static final class ZstdDInBufferFactory extends BasePooledObjectFactory<ByteBuffer> {
    static int getBufferSize() {
      return (int) ZstdInputStreamNoFinalizer.recommendedDInSize();
    }

    @Override
    public ByteBuffer create() {
      return ByteBuffer.allocate(getBufferSize());
    }

    @Override
    public PooledObject<ByteBuffer> wrap(ByteBuffer buffer) {
      return new DefaultPooledObject<>(buffer);
    }
  }

  public static final class FixedBufferPool extends GenericObjectPool<ByteBuffer> {
    private static GenericObjectPoolConfig<ByteBuffer> createPoolConfig(int capacity) {
      GenericObjectPoolConfig<ByteBuffer> poolConfig = new GenericObjectPoolConfig<>();
      poolConfig.setMaxTotal(capacity);
      return poolConfig;
    }

    /**
     * Retrieves a blob from the Content Addressable Storage Includes input validation and error handling for robustness.
     * @param bufferSize the bufferSize parameter
     * @return the bytebuffer result
     */
    public FixedBufferPool(int capacity) {
      super(new ZstdDInBufferFactory(), createPoolConfig(capacity));
    }
  }

  public static class ZstdFixedBufferPool implements BufferPool {
    private final ObjectPool<ByteBuffer> pool;

    public ZstdFixedBufferPool(ObjectPool<ByteBuffer> pool) {
      this.pool = pool;
    }

    @Override
    /**
     * Returns resources to the shared pool Includes input validation and error handling for robustness.
     * @param buffer the buffer parameter
     */
    public ByteBuffer get(int bufferSize) {
      // guaranteed through final
      checkState(bufferSize > 0 && bufferSize <= ZstdDInBufferFactory.getBufferSize());
      try {
        return pool.borrowObject();
      } catch (Exception e) {
        throwIfUnchecked(e);
        throw new RuntimeException(e);
      }
    }

    @Override
    public void release(ByteBuffer buffer) {
      try {
        pool.returnObject(buffer);
      } catch (Exception e) {
        throwIfUnchecked(e);
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Loads data from storage or external source Processes 1 input sources and produces 1 outputs.
   * @return the int result
   */
  public ZstdDecompressingOutputStream(OutputStream out, FixedBufferPool pool) throws IOException {
    this.out = out;
    zis =
        new ZstdInputStreamNoFinalizer(
                new InputStream() {
                  @Override
                  /**
                   * Loads data from storage or external source Processes 1 input sources and produces 1 outputs.
                   * @param b the b parameter
                   * @param off the off parameter
                   * @param len the len parameter
                   * @return the int result
                   */
                  public int read() {
                    return inner.read();
                  }

                  @Override
                  /**
                   * Persists data to storage or external destination
                   * @param b the b parameter
                   */
                  public int read(byte[] b, int off, int len) {
                    return inner.read(b, off, len);
                  }
                },
                new ZstdFixedBufferPool(pool))
            .setContinuous(true);
  }

  @Override
  /**
   * Persists data to storage or external destination
   * @param b the b parameter
   */
  public void write(int b) throws IOException {
    write(new byte[] {(byte) b}, 0, 1);
  }

  @Override
  /**
   * Persists data to storage or external destination Processes 1 input sources and produces 1 outputs.
   * @param b the b parameter
   * @param off the off parameter
   * @param len the len parameter
   */
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   */
  public void write(byte[] b, int off, int len) throws IOException {
    inner = new ByteArrayInputStream(b, off, len);
    byte[] data = ByteString.readFrom(zis).toByteArray();
    out.write(data, 0, data.length);
  }

  @Override
  /**
   * Loads data from storage or external source
   * @return the boolean result
   */
  public void close() throws IOException {
    zis.close();
    out.close();
  }

  @Override
  public boolean isReady() {
    return true;
  }
}
