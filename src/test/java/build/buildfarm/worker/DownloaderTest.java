// Copyright 2018 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import build.buildfarm.worker.Downloader.OffsetInputStreamFactory;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.mockito.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DownloaderTest {
  @Test
  public void copyThrowsNonDeadlineExceededCausal() throws IOException, InterruptedException {
    IOException unavailableException = new IOException(Status.UNAVAILABLE.asRuntimeException());
    InputStream in = mock(InputStream.class);
    when(in.available()).thenThrow(unavailableException);
    OffsetInputStreamFactory factory = mock(OffsetInputStreamFactory.class);
    when(factory.newInputAt(0)).thenReturn(in);
    OutputStream out = mock(OutputStream.class);
    IOException ioException = null;
    try {
      Downloader.copy(factory, out);
    } catch (IOException e) {
      ioException = e;
    }
    verify(factory, times(1)).newInputAt(eq(0l));
    verify(in, times(1)).available();
    assertThat(ioException).isEqualTo(unavailableException);
    verifyZeroInteractions(out);
  }

  @Test
  public void copyRestartsOnDeadlineExceededWithProgress() throws IOException, InterruptedException {
    IOException deadlineExceededException = new IOException(Status.DEADLINE_EXCEEDED.asRuntimeException());
    InputStream in = mock(InputStream.class);
    when(in.available())
        .thenReturn(1)
        .thenThrow(deadlineExceededException)
        .thenReturn(0);
    when(in.read(Matchers.<byte[]>any(), eq(0), any(Integer.class)))
        .thenReturn(1)
        .thenReturn(-1);
    OffsetInputStreamFactory factory = mock(OffsetInputStreamFactory.class);
    when(factory.newInputAt(0)).thenReturn(in);
    when(factory.newInputAt(1)).thenReturn(in);
    OutputStream out = mock(OutputStream.class);
    Downloader.copy(factory, out);
    verify(in, times(3)).available();
    verify(out, times(1)).write(Matchers.<byte[]>any(), eq(0), eq(1));
  }

  @Test
  public void copyThrowsOnDeadlineExceededWithoutProgress() throws IOException, InterruptedException {
    IOException deadlineExceededException = new IOException(Status.DEADLINE_EXCEEDED.asRuntimeException());
    InputStream in = mock(InputStream.class);
    when(in.available())
        .thenThrow(deadlineExceededException);
    OffsetInputStreamFactory factory = mock(OffsetInputStreamFactory.class);
    when(factory.newInputAt(0)).thenReturn(in);
    OutputStream out = mock(OutputStream.class);
    IOException ioException = null;
    try {
      Downloader.copy(factory, out);
    } catch (IOException e) {
      ioException = e;
    }
    verify(factory, times(1)).newInputAt(eq(0l));
    verify(in, times(1)).available();
    assertThat(ioException).isEqualTo(deadlineExceededException);
    verifyZeroInteractions(out);
  }
}
