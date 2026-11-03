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

package build.buildfarm.instance.shard;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.Write;
import build.buildfarm.instance.Instance;
import build.buildfarm.v1test.Digest;
import java.io.IOException;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Writes} including its inner {@code InvalidatingWrite}. */
@RunWith(JUnit4.class)
public class WritesTest {
  private Writes makeWritesWith(RecordingWrite delegate) {
    Instance instance = mock(Instance.class);
    try {
      when(instance.getBlobWrite(any(), any(), any(), any())).thenReturn(delegate);
    } catch (Exception e) {
      throw new AssertionError(e);
    }
    return new Writes(() -> instance);
  }

  @Test
  public void invalidatingWrite_cancel_delegatesAndInvalidatesOnce() throws Exception {
    RecordingWrite delegate = new RecordingWrite();
    Writes writes = makeWritesWith(delegate);

    Digest digest =
        Digest.newBuilder()
            .setHash("abc")
            .setSize(100)
            .setDigestFunction(DigestFunction.Value.SHA256)
            .build();
    Write wrapper =
        writes.get(
            Compressor.Value.IDENTITY,
            digest,
            UUID.randomUUID(),
            RequestMetadata.getDefaultInstance());

    Throwable cause = new IOException("test");
    wrapper.cancel("upstream cancel", cause);

    // cancel forwarded to delegate exactly once.
    assertThat(delegate.cancelCount.get()).isEqualTo(1);
    assertThat(delegate.lastCancelMessage).isEqualTo("upstream cancel");
    assertThat(delegate.lastCancelCause).isSameInstanceAs(cause);
    // Wrapper's getFuture() is delegate.getFuture() — it is now done exceptionally.
    assertThat(wrapper.getFuture().isDone()).isTrue();
  }

  @Test
  public void invalidatingWrite_zeroSize_returnsCompleteWrite_noWrapperNeeded() throws Exception {
    Instance instance = mock(Instance.class);
    Writes writes = new Writes(() -> instance);

    Digest digest =
        Digest.newBuilder()
            .setHash("empty")
            .setSize(0)
            .setDigestFunction(DigestFunction.Value.SHA256)
            .build();
    Write w =
        writes.get(
            Compressor.Value.IDENTITY,
            digest,
            UUID.randomUUID(),
            RequestMetadata.getDefaultInstance());

    // CompleteWrite, not an InvalidatingWrite. cancel is the Write.cancel default no-op — safe.
    w.cancel("test", null);
    assertThat(w.isComplete()).isTrue();
  }
}
