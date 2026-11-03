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

package build.buildfarm.common.grpc.testing;

import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.annotation.Nullable;

/**
 * Test fake {@link ClientCall} that gives tests direct control over {@code isReady} transitions and
 * exposes the sequence of sent messages and termination events.
 *
 * <p>Typical use: install this via a {@code ClientInterceptor} that returns it from {@code
 * interceptCall(...)} instead of forwarding to the real call. Tests then drive the producer by
 * flipping {@link #becomeNotReady()} / {@link #becomeReady()} and inject server responses via
 * {@link #deliverResponse(Object)}, {@link #completeCall()}, or {@link #failCall(Status)}.
 *
 * <p>Threading: producer-side methods ({@link #sendMessage}, {@link #isReady}, {@link #cancel},
 * {@link #halfClose}) may run on a different thread from the test thread that calls {@link
 * #becomeReady} / {@link #deliverResponse} / etc. Cross-thread state lives in volatile fields, a
 * synchronized list, and atomic counters; listener callbacks fire without holding any monitor so
 * the producer can re-enter this call from its {@code onReady} handler without deadlocking.
 */
public final class ControllableClientCall<Q, R> extends ClientCall<Q, R> {
  public final List<Q> sendMessages = Collections.synchronizedList(new ArrayList<>());
  public final AtomicInteger cancelCount = new AtomicInteger();
  public final AtomicInteger halfCloseCount = new AtomicInteger();

  private volatile @Nullable Listener<R> listener;
  private volatile boolean ready = true;

  @Override
  public void start(Listener<R> listener, Metadata headers) {
    this.listener = listener;
    // Fire onReady once at startup so producers using onReady-as-producer enter their loop.
    listener.onReady();
  }

  @Override
  public void request(int numMessages) {
    // No-op: tests deliver responses directly via deliverResponse(...).
  }

  @Override
  public void cancel(@Nullable String message, @Nullable Throwable cause) {
    cancelCount.incrementAndGet();
  }

  @Override
  public void halfClose() {
    halfCloseCount.incrementAndGet();
  }

  @Override
  public void sendMessage(Q message) {
    sendMessages.add(message);
  }

  @Override
  public boolean isReady() {
    return ready;
  }

  public void becomeNotReady() {
    ready = false;
  }

  /**
   * Flip ready and fire {@code onReady}. Blocks until the producer's {@code onReady} handler
   * returns, which is what makes assertions on {@link #sendMessages} deterministic immediately
   * after this call.
   */
  public void becomeReady() {
    ready = true;
    fireListener(Listener::onReady);
  }

  public void deliverResponse(R response) {
    fireListener(l -> l.onMessage(response));
  }

  public void completeCall() {
    failCall(Status.OK);
  }

  public void failCall(Status status) {
    fireListener(l -> l.onClose(status, new Metadata()));
  }

  private void fireListener(Consumer<Listener<R>> action) {
    Listener<R> snapshot = listener;
    if (snapshot != null) {
      action.accept(snapshot);
    }
  }

  /**
   * Returns true iff {@link #halfClose()} or {@link #cancel(String, Throwable)} was observed at
   * least once. Returns false for "stuck mid-stream" shapes — messages sent without a subsequent
   * halfClose or cancel — so generic invariant tests can assert this at the end of every test case
   * to catch leak-shape regressions.
   */
  public boolean terminatedCleanly() {
    return halfCloseCount.get() > 0 || cancelCount.get() > 0;
  }
}
