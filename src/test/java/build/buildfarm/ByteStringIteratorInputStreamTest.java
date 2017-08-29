// Copyright 2017 The Bazel Authors. All rights reserved.
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

package build.buildfarm;

import static com.google.common.truth.Truth.assertThat;

import build.buildfarm.instance.stub.ByteStringIteratorInputStream;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.protobuf.ByteString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import java.io.IOException;
import java.io.InputStream;

@RunWith(JUnit4.class)
public class ByteStringIteratorInputStreamTest {
  @Test
  public void readSingleChunk() throws IOException {
    ByteString hello = ByteString.copyFromUtf8("Hello, World");
    InputStream in = new ByteStringIteratorInputStream(
        Iterators.<ByteString>singletonIterator(hello));
    byte[] data = new byte[hello.size()];

    assertThat(in.read(data)).isEqualTo(hello.size());
    assertThat(ByteString.copyFrom(data)).isEqualTo(hello);
    assertThat(in.read()).isEqualTo(-1);
  }

  @Test
  public void readEmpty() throws IOException {
    InputStream in = new ByteStringIteratorInputStream(
        ImmutableList.<ByteString>of().iterator());
    assertThat(in.read()).isEqualTo(-1);
  }

  @Test
  public void readSingleByte() throws IOException {
    byte[] single = new byte[1];
    single[0] = 42;
    InputStream in = new ByteStringIteratorInputStream(
        ImmutableList.<ByteString>of(ByteString.copyFrom(single)).iterator());
    assertThat(in.read()).isEqualTo(42);
    assertThat(in.read()).isEqualTo(-1);
  }

  @Test
  public void readSpanningChunks() throws IOException {
    ByteString hello = ByteString.copyFromUtf8("Hello, ");
    ByteString world = ByteString.copyFromUtf8("World");
    InputStream in = new ByteStringIteratorInputStream(
        ImmutableList.<ByteString>of(hello, world).iterator());
    ByteString helloWorld = hello.concat(world);
    byte[] data = new byte[helloWorld.size()];

    assertThat(in.read(data)).isEqualTo(helloWorld.size());
    assertThat(ByteString.copyFrom(data)).isEqualTo(helloWorld);
    assertThat(in.read()).isEqualTo(-1);
  }

  @Test
  public void readSpanningChunksWithEmptyChunk() throws IOException {
    ByteString hello = ByteString.copyFromUtf8("Hello, ");
    ByteString world = ByteString.copyFromUtf8("World");
    InputStream in = new ByteStringIteratorInputStream(
        ImmutableList.<ByteString>of(hello, ByteString.EMPTY, world).iterator());
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
    for (int i = 0; i < buffer.length; i++) {
      buffer[i] = 42;
    }

    byte[] expected = new byte[6];
    expected[0] = 42;
    expected[1] = 42;
    expected[2] = 1;
    expected[3] = 2;
    expected[4] = 42;
    expected[5] = 42;

    InputStream in = new ByteStringIteratorInputStream(ImmutableList.<ByteString>of(
        ByteString.copyFrom(data1),
        ByteString.copyFrom(data2)).iterator());
    assertThat(in.read(buffer, 2, 2)).isEqualTo(2);
    assertThat(ByteString.copyFrom(buffer)).isEqualTo(ByteString.copyFrom(expected));
    assertThat(in.read()).isEqualTo(3);
    assertThat(in.read()).isEqualTo(-1);
  }
}
