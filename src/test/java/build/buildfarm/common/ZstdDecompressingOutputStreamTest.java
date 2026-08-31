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

package build.buildfarm.common;

import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertThrows;

import build.buildfarm.common.ZstdDecompressingOutputStream.FixedBufferPool;
import com.github.luben.zstd.Zstd;
import com.google.common.base.Stopwatch;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ZstdDecompressingOutputStreamTest {
  private static final Duration BORROW_TIMEOUT = Duration.ofMillis(500);

  private static OutputStream sink() {
    return new ByteArrayOutputStream();
  }

  // A ZstdDecompressingOutputStream takes one buffer for its whole lifetime, so a pool of one lets
  // a single open stream exhaust it.
  private static FixedBufferPool singleBufferPool() {
    return new FixedBufferPool(/* capacity= */ 1, BORROW_TIMEOUT);
  }

  @Test
  public void decompressesWhatZstdCompressed() throws IOException {
    byte[] blob = "the quick brown fox jumps over the lazy dog".getBytes(UTF_8);
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    try (FixedBufferPool pool = singleBufferPool();
        ZstdDecompressingOutputStream zstdOut = new ZstdDecompressingOutputStream(out, pool)) {
      zstdOut.write(Zstd.compress(blob));
    }

    assertThat(out.toByteArray()).isEqualTo(blob);
  }

  /**
   * Without a borrow timeout the second stream waits forever, and every later zstd transfer on the
   * process queues behind it. Nothing else in the stack bounds this wait.
   */
  @Test
  public void borrowFailsOnceTheTimeoutPasses() throws IOException {
    try (FixedBufferPool pool = singleBufferPool()) {
      ZstdDecompressingOutputStream held = new ZstdDecompressingOutputStream(sink(), pool);
      Stopwatch stopwatch = Stopwatch.createStarted();

      // zstd-jni turns the null that the exhausted pool returns into a ZstdIOException.
      assertThrows(IOException.class, () -> new ZstdDecompressingOutputStream(sink(), pool));

      assertThat(stopwatch.elapsed().toMillis()).isAtLeast(BORROW_TIMEOUT.toMillis());
      assertThat(pool.getNumActive()).isEqualTo(1);
      held.close();
    }
  }

  /** A failed borrow must not consume a buffer, or the pool loses one buffer per timeout. */
  @Test
  public void closeReturnsTheBufferAfterATimeout() throws IOException {
    try (FixedBufferPool pool = singleBufferPool()) {
      ZstdDecompressingOutputStream held = new ZstdDecompressingOutputStream(sink(), pool);
      assertThrows(IOException.class, () -> new ZstdDecompressingOutputStream(sink(), pool));
      held.close();

      assertThat(pool.getNumActive()).isEqualTo(0);
      new ZstdDecompressingOutputStream(sink(), pool).close();
      assertThat(pool.getNumActive()).isEqualTo(0);
    }
  }

  /** The capacity-only constructor keeps the wait without a bound, which is the default. */
  @Test
  public void capacityOnlyPoolLeavesTheWaitUnbounded() throws IOException {
    try (FixedBufferPool pool = new FixedBufferPool(/* capacity= */ 1)) {
      assertThat(pool.getMaxWaitDuration().isNegative()).isTrue();

      new ZstdDecompressingOutputStream(sink(), pool).close();
      assertThat(pool.getNumActive()).isEqualTo(0);
      new ZstdDecompressingOutputStream(sink(), pool).close();
      assertThat(pool.getNumActive()).isEqualTo(0);
    }
  }
}
