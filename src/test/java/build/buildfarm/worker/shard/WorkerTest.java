// Copyright 2026 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.worker.shard;

import static build.buildfarm.worker.shard.Worker.zstdDecompressingInputStreamFactory;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import build.bazel.remote.execution.v2.Compressor;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.common.ZstdDecompressingOutputStream.FixedBufferPool;
import build.buildfarm.v1test.Digest;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WorkerTest {
  /**
   * The decompressor takes its pool buffer in the constructor. When that borrow times out, nothing
   * else holds the remote stream, so the factory has to close it. Nothing downstream can, because
   * the factory never returned it.
   */
  @Test
  public void zstdInputStreamFactoryClosesTheBaseStreamOnAnExhaustedPool() throws Exception {
    try (FixedBufferPool pool = new FixedBufferPool(/* capacity= */ 1, Duration.ZERO)) {
      AtomicBoolean closed = new AtomicBoolean(false);
      InputStream base =
          new ByteArrayInputStream(new byte[0]) {
            @Override
            public void close() {
              closed.set(true);
            }
          };
      InputStreamFactory factory =
          zstdDecompressingInputStreamFactory((compressor, digest, offset) -> base, pool);

      pool.borrowObject(); // the only buffer

      assertThrows(
          IOException.class,
          () ->
              factory.newInput(
                  Compressor.Value.IDENTITY, Digest.getDefaultInstance(), /* offset= */ 0));
      assertThat(closed.get()).isTrue();
    }
  }

  /** A compressed read passes the remote stream through, so no buffer is taken. */
  @Test
  public void zstdInputStreamFactoryPassesCompressedReadsThrough() throws Exception {
    try (FixedBufferPool pool = new FixedBufferPool(/* capacity= */ 1)) {
      InputStream base = new ByteArrayInputStream(new byte[0]);
      InputStreamFactory factory =
          zstdDecompressingInputStreamFactory((compressor, digest, offset) -> base, pool);

      assertThat(
              factory.newInput(Compressor.Value.ZSTD, Digest.getDefaultInstance(), /* offset= */ 0))
          .isSameInstanceAs(base);
      assertThat(pool.getNumActive()).isEqualTo(0);
    }
  }
}
