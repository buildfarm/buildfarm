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

  public ZstdDecompressingOutputStream(OutputStream out, FixedBufferPool pool) throws IOException {
    this.out = out;
    zis =
        new ZstdInputStreamNoFinalizer(
                new InputStream() {
                  @Override
                  public int read() {
                    return inner.read();
                  }

                  @Override
                  public int read(byte[] b, int off, int len) {
                    return inner.read(b, off, len);
                  }
                },
                new ZstdFixedBufferPool(pool))
            .setContinuous(true);
  }

  @Override
  public void write(int b) throws IOException {
    write(new byte[] {(byte) b}, 0, 1);
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    inner = new ByteArrayInputStream(b, off, len);
    byte[] data = ByteString.readFrom(zis).toByteArray();
    out.write(data, 0, data.length);
  }

  @Override
  public void close() throws IOException {
    zis.close();
    out.close();
  }

  @Override
  public boolean isReady() {
    return true;
  }
}
