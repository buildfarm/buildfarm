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

package build.buildfarm.common.grpc;

import build.buildfarm.common.BlobNotFoundException;
import build.buildfarm.common.grpc.Retrier.Backoff;
import build.buildfarm.common.io.ByteStringQueueInputStream;
import com.google.bytestream.ByteStreamGrpc.ByteStreamStub;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.NoSuchFileException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import javax.annotation.Nullable;

public final class ByteStreamHelper {
  private ByteStreamHelper() {}

  @SuppressWarnings("Guava")
  public static InputStream newInput(
      String resourceName,
      long offset,
      String endpoint,
      Supplier<ByteStreamStub> bsStubSupplier,
      Supplier<Backoff> backoffSupplier,
      Predicate<Status> isRetriable,
      @Nullable ListeningScheduledExecutorService retryService)
      throws IOException {
    ReadRequest request =
        ReadRequest.newBuilder().setResourceName(resourceName).setReadOffset(offset).build();
    BlockingQueue<ByteString> queue = new ArrayBlockingQueue<>(1);
    ByteStringQueueInputStream inputStream = new ByteStringQueueInputStream(queue);
    // this interface needs to operate similar to open, where it
    // throws an exception on creation. We will need to wait around
    // for the response to come back in order to supply the stream or
    // throw the exception it receives
    SettableFuture<InputStream> streamReadyFuture = SettableFuture.create();
    StreamObserver<ReadResponse> responseObserver =
        new StreamObserver<ReadResponse>() {
          long requestOffset = offset;
          long currentOffset = offset;
          Backoff backoff = backoffSupplier.get();

          @Override
          public void onNext(ReadResponse response) {
            streamReadyFuture.set(inputStream);
            ByteString data = response.getData();
            try {
              queue.put(data);
              currentOffset += data.size();
            } catch (InterruptedException e) {
              // cancel context?
              inputStream.setException(e);
            }
          }

          private void retryRequest() {
            requestOffset = currentOffset;
            bsStubSupplier
                .get()
                .read(request.toBuilder().setReadOffset(requestOffset).build(), this);
          }

          @Override
          public void onError(Throwable t) {
            Status status = Status.fromThrowable(t);
            long nextDelayMillis = backoff.nextDelayMillis();
            if (status.getCode() == Status.Code.DEADLINE_EXCEEDED
                && currentOffset != requestOffset) {
              backoff = backoffSupplier.get();
              retryRequest();
            } else if (retryService == null || nextDelayMillis < 0 || !isRetriable.test(status)) {
              streamReadyFuture.setException(t);
              inputStream.setException(t);
            } else {
              try {
                ListenableFuture<?> schedulingResult =
                    retryService.schedule(
                        this::retryRequest, nextDelayMillis, TimeUnit.MILLISECONDS);
                schedulingResult.addListener(
                    () -> {
                      try {
                        schedulingResult.get();
                      } catch (ExecutionException e) {
                        inputStream.setException(e.getCause());
                      } catch (InterruptedException e) {
                        inputStream.setException(e);
                      }
                    },
                    MoreExecutors.directExecutor());
              } catch (RejectedExecutionException e) {
                e.addSuppressed(
                    new Exception(String.format("could not retry read on %s", endpoint), t));
                BlobNotFoundException bnfe = new BlobNotFoundException(resourceName, e);
                streamReadyFuture.setException(bnfe);
                inputStream.setException(bnfe);
              }
            }
          }

          @Override
          public void onCompleted() {
            inputStream.setCompleted();
          }
        };
    bsStubSupplier.get().read(request, responseObserver);
    // the interface is technically blocking (not aio) and is
    // perfectly reasonable to be used as a wait point
    try {
      return streamReadyFuture.get();
    } catch (InterruptedException e) {
      try {
        inputStream.close();
      } catch (RuntimeException closeEx) {
        e.addSuppressed(e);
      }
      IOException ioEx = new ClosedByInterruptException();
      ioEx.addSuppressed(e);
      throw ioEx;
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      Status status = Status.fromThrowable(cause);
      if (status.getCode() == Status.Code.NOT_FOUND) {
        IOException ioEx = new NoSuchFileException(resourceName);
        ioEx.addSuppressed(cause);
        throw ioEx;
      }
      Throwables.throwIfInstanceOf(cause, IOException.class);
      throw new IOException(cause);
    }
  }
}
