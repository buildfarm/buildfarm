// Copyright 2017 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.common.io;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Queues.newLinkedBlockingQueue;
import static com.google.common.truth.Truth.assertThat;
import static com.google.protobuf.ByteString.EMPTY;
import static com.google.protobuf.ByteString.copyFromUtf8;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ByteStringQueueInputStreamTest {
  private ByteStringQueueInputStream newInput(Iterable<ByteString> elements) {
    ByteStringQueueInputStream in =
        new ByteStringQueueInputStream(newLinkedBlockingQueue(elements));
    in.setCompleted();
    return in;
  }

  @Test
  public void readSingleChunk() throws IOException {
    ByteString hello = copyFromUtf8("Hello, World");
    InputStream in = newInput(of(hello));
    byte[] data = new byte[hello.size()];

    assertThat(in.read(data)).isEqualTo(hello.size());
    assertThat(ByteString.copyFrom(data)).isEqualTo(hello);
    assertThat(in.read()).isEqualTo(-1);
  }

  @Test
  public void readEmpty() throws IOException {
    InputStream in = newInput(of());
    assertThat(in.read()).isEqualTo(-1);
  }

  @Test
  public void readSingleByte() throws IOException {
    byte[] single = new byte[1];
    single[0] = 42;
    InputStream in = newInput(of(ByteString.copyFrom(single)));
    assertThat(in.read()).isEqualTo(42);
    assertThat(in.read()).isEqualTo(-1);
  }

  @Test
  public void readSpanningChunks() throws IOException {
    ByteString hello = copyFromUtf8("Hello, ");
    ByteString world = copyFromUtf8("World");
    InputStream in = newInput(of(hello, world));
    ByteString helloWorld = hello.concat(world);
    byte[] data = new byte[helloWorld.size()];

    assertThat(in.read(data)).isEqualTo(helloWorld.size());
    assertThat(ByteString.copyFrom(data)).isEqualTo(helloWorld);
    assertThat(in.read()).isEqualTo(-1);
  }

  @Test
  public void readSpanningChunksWithEmptyChunk() throws IOException {
    ByteString hello = copyFromUtf8("Hello, ");
    ByteString world = copyFromUtf8("World");
    InputStream in = newInput(of(hello, EMPTY, world));
    ByteString helloWorld = hello.concat(world);
    byte[] data = new byte[helloWorld.size()];

    assertThat(in.read(data)).isEqualTo(helloWorld.size());
    assertThat(ByteString.copyFrom(data)).isEqualTo(helloWorld);
    assertThat(in.read()).isEqualTo(-1);
  }

  @Test
  public void readWithOffsetAndLengthSpanningChunks() throws IOException {
    byte[] data1 = new byte[1];

    data1[0] = 1;

    byte[] data2 = new byte[2];
    data2[0] = 2;
    data2[1] = 3;

    byte[] buffer = new byte[6];
    Arrays.fill(buffer, (byte) 42);

    byte[] expected = new byte[6];
    expected[0] = 42;
    expected[1] = 42;
    expected[2] = 1;
    expected[3] = 2;
    expected[4] = 42;
    expected[5] = 42;

    InputStream in = newInput(of(ByteString.copyFrom(data1), ByteString.copyFrom(data2)));
    assertThat(in.read(buffer, 2, 2)).isEqualTo(2);
    assertThat(ByteString.copyFrom(buffer)).isEqualTo(ByteString.copyFrom(expected));
    assertThat(in.read()).isEqualTo(3);
    assertThat(in.read()).isEqualTo(-1);
  }

  @Test
  public void queueWithElementsHasAvailable() throws IOException {
    InputStream in = newInput(of(copyFromUtf8("data")));
    assertThat(in.available()).isNotEqualTo(0);
  }

  @Test
  public void emptyQueueIsUnavailable() throws IOException {
    InputStream in = newInput(of());
    assertThat(in.available()).isEqualTo(0);
  }

  @Test(expected = IOException.class)
  public void closedStreamAvailableThrowsIOException() throws IOException {
    InputStream in = newInput(of(copyFromUtf8("Hello, World")));
    in.close();

    in.available();
  }

  @Test(expected = IOException.class)
  public void closedStreamReadDefaultThrowsIOException() throws IOException {
    InputStream in = newInput(of(copyFromUtf8("Hello, World")));
    in.close();

    in.read();
  }

  @Test(expected = IOException.class)
  public void closedStreamReadThrowsIOException() throws IOException {
    InputStream in = newInput(of(copyFromUtf8("Hello, World")));
    in.close();

    byte[] buffer = new byte[5];
    in.read(buffer, 0, 5);
  }

  @Test(expected = IOException.class)
  public void readWithExceptionThrowsIOExceptionAfterContent() throws IOException {
    ByteStringQueueInputStream in =
        new ByteStringQueueInputStream(newLinkedBlockingQueue(of(copyFromUtf8("Hello, World"))));
    in.setException(new RuntimeException("failed"));
    byte[] buffer = new byte[32]; // more than enough
    try {
      in.read(buffer);
    } catch (IOException e) {
      throw new RuntimeException("unexpected io exception", e);
    }

    in.read();
  }

  @Test(expected = IOException.class)
  public void availableWithExceptionThrowsIOException() throws IOException {
    ByteStringQueueInputStream in = new ByteStringQueueInputStream(newLinkedBlockingQueue(of()));
    in.setException(new RuntimeException("failed"));
    in.available();
  }

  @Test(expected = IOException.class)
  public void availableWithExceptionThrowsIOExceptionAfterContent() throws IOException {
    ByteStringQueueInputStream in =
        new ByteStringQueueInputStream(newLinkedBlockingQueue(of(copyFromUtf8("Hello, World"))));
    in.setException(new RuntimeException("failed"));
    try {
      in.available();

      byte[] buffer = new byte[32]; // more than enough
      in.read(buffer);
    } catch (IOException e) {
      throw new RuntimeException("unexpected io exception", e);
    }
    in.available();
  }
}
