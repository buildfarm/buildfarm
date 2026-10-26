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
import static org.junit.Assert.assertThrows;

import io.grpc.Status;
import io.grpc.StatusException;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ReadOnlyAwareWrite#cancel}. */
@RunWith(JUnit4.class)
public class ReadOnlyAwareWriteTest {
  @Test
  public void cancel_completesOwnFutureAndForwards() throws Exception {
    RecordingWrite delegate = new RecordingWrite();
    ReadOnlyAwareWrite write = new ReadOnlyAwareWrite(() -> delegate);

    // Force-realize the delegate via a call that goes through getDelegate().
    write.getCommittedSize();

    Throwable cause = new IOException("test cause");
    write.cancel("upstream cancel", cause);

    // Wrapper's own future is exceptionally done with a CANCELLED StatusException.
    assertThat(write.getFuture().isDone()).isTrue();
    ExecutionException ee = assertThrows(ExecutionException.class, () -> write.getFuture().get());
    assertThat(ee.getCause()).isInstanceOf(StatusException.class);
    assertThat(Status.fromThrowable(ee.getCause()).getCode()).isEqualTo(Status.Code.CANCELLED);

    // Forwarded to the delegate exactly once.
    assertThat(delegate.cancelCount.get()).isEqualTo(1);
  }

  @Test
  public void cancel_isNoOpIfDelegateNeverRealized() throws Exception {
    // Track whether the supplier was invoked. If cancel(...) is called before any delegate-using
    // method realizes it, the wrapper's own future still completes but no delegate work happens.
    AtomicInteger supplierInvocations = new AtomicInteger();
    ReadOnlyAwareWrite write =
        new ReadOnlyAwareWrite(
            () -> {
              supplierInvocations.incrementAndGet();
              return new RecordingWrite();
            });

    write.cancel("never realized", null);

    assertThat(supplierInvocations.get()).isEqualTo(0);
    assertThat(write.getFuture().isDone()).isTrue();
  }

  @Test
  public void cancel_isIdempotent_secondCallDoesNotReforwardToDelegate() throws Exception {
    RecordingWrite delegate = new RecordingWrite();
    ReadOnlyAwareWrite write = new ReadOnlyAwareWrite(() -> delegate);
    write.getCommittedSize();

    write.cancel("first", null);
    write.cancel("second", null);

    // The wrapper's snapshot-and-null pattern means the delegate is forwarded to only on the
    // first cancel. The second sees delegate == null and is a no-op forward.
    assertThat(delegate.cancelCount.get()).isEqualTo(1);
  }

  @Test
  public void getOutput_afterCancel_throwsIOException() throws Exception {
    // After cancel, the wrapper's future is done. A follow-up getOutput must not realize a fresh
    // delegate — that would land writes on a new worker. The future.isDone() guard in getDelegate
    // surfaces the terminal state as an IOException instead.
    ReadOnlyAwareWrite write = new ReadOnlyAwareWrite(RecordingWrite::new);
    write.cancel("terminated", null);

    assertThrows(IOException.class, () -> write.getOutput(0, 1, TimeUnit.SECONDS, () -> {}));
  }
}
