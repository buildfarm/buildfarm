// Copyright 2018 The Buildfarm Authors. All rights reserved.
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
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RingBufferInputStreamTest {
  @Test
  public void shutdownBufferIsAtEndOfStream() throws IOException {
    RingBufferInputStream buffer = new RingBufferInputStream(1);
    buffer.shutdown();
    assertThat(buffer.read()).isEqualTo(-1);
  }

  @Test
  public void writtenContentIsRead() throws IOException, InterruptedException {
    RingBufferInputStream buffer = new RingBufferInputStream(1);
    byte[] content = new byte[1];
    content[0] = 42;
    buffer.write(content);
    assertThat(buffer.read()).isEqualTo(content[0]);
  }

  @Test
  public void writesUnblockReads() throws ExecutionException, InterruptedException {
    ListeningExecutorService service = listeningDecorator(newSingleThreadExecutor());
    AtomicInteger counter = new AtomicInteger();

    RingBufferInputStream buffer = new RingBufferInputStream(1);
    ListenableFuture<Integer> readFuture =
        service.submit(
            () -> {
              counter.getAndIncrement();
              return buffer.read();
            });
    byte[] content = new byte[1];
    content[0] = 42;
    while (counter.get() != 1) {
      MICROSECONDS.sleep(10);
    }
    assertThat(readFuture.isDone()).isFalse();
    buffer.write(content);
    assertThat(readFuture.get()).isEqualTo(content[0]);
    service.shutdown();
    service.awaitTermination(10, MICROSECONDS);
  }

  @Test
  public void readUnblocksWrite() throws ExecutionException, IOException, InterruptedException {
    ListeningExecutorService service = listeningDecorator(newSingleThreadExecutor());
    AtomicInteger counter = new AtomicInteger();

    RingBufferInputStream buffer = new RingBufferInputStream(1);
    byte[] content = new byte[1];
    content[0] = 42;
    buffer.write(content); // buffer is now full
    ListenableFuture<Void> writeFuture =
        service.submit(
            () -> {
              counter.getAndIncrement();
              buffer.write(content);
              return null;
            });
    while (counter.get() != 1) {
      MICROSECONDS.sleep(10);
    }
    assertThat(writeFuture.isDone()).isFalse();
    buffer.read();
    assertThat(writeFuture.get()).isEqualTo(null);
    service.shutdown();
    service.awaitTermination(10, MICROSECONDS);
  }

  @Test
  public void shutdownUnblocksReadsAndEndsStream() throws ExecutionException, InterruptedException {
    ListeningExecutorService service = listeningDecorator(newSingleThreadExecutor());
    AtomicInteger counter = new AtomicInteger();

    RingBufferInputStream buffer = new RingBufferInputStream(1);
    ListenableFuture<Integer> readFuture =
        service.submit(
            () -> {
              counter.getAndIncrement();
              return buffer.read();
            });
    while (counter.get() != 1) {
      MICROSECONDS.sleep(10);
    }
    assertThat(readFuture.isDone()).isFalse();
    buffer.shutdown();
    assertThat(readFuture.get()).isEqualTo(-1);
  }

  @Test
  public void bufferFlippingContentIsRead() throws IOException, InterruptedException {
    RingBufferInputStream buffer = new RingBufferInputStream(8);

    byte[] content = new byte[7];
    content[0] = 8;
    content[1] = 6;
    content[2] = 7;
    content[3] = 5;
    content[4] = 3;
    content[5] = 0;
    content[6] = 9;
    byte[] readContent = new byte[7];

    buffer.write(content);
    assertThat(buffer.read(readContent)).isEqualTo(readContent.length);
    assertThat(readContent).isEqualTo(content);
    // flips after first byte
    buffer.write(content);
    assertThat(buffer.read(readContent)).isEqualTo(readContent.length);
    assertThat(readContent).isEqualTo(content);
    // flips after second
    buffer.write(content);
    assertThat(buffer.read(readContent)).isEqualTo(readContent.length);
    assertThat(readContent).isEqualTo(content);
  }
}
