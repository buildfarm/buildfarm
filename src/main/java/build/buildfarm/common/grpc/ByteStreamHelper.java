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

package build.buildfarm.common.grpc;

import build.buildfarm.common.grpc.Retrier.Backoff;
import build.buildfarm.common.io.ByteStringQueueInputStream;
import com.google.bytestream.ByteStreamGrpc.ByteStreamStub;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import javax.annotation.Nullable;

public final class ByteStreamHelper {
  private ByteStreamHelper() { }

  public static final InputStream newInput(
      String resourceName,
      long offset,
      Supplier<ByteStreamStub> bsStubSupplier,
      Supplier<Backoff> backoffSupplier,
      Predicate<Status> isRetriable,
      @Nullable ListeningScheduledExecutorService retryService) {
    ReadRequest request = ReadRequest.newBuilder()
        .setResourceName(resourceName)
        .setReadOffset(offset)
        .build();
    BlockingQueue<ByteString> queue = new LinkedBlockingQueue<ByteString>();
    ByteStringQueueInputStream inputStream = new ByteStringQueueInputStream(queue);
    StreamObserver<ReadResponse> responseObserver = new StreamObserver<ReadResponse>() {
      long requestOffset = offset;
      long currentOffset = offset;
      Backoff backoff = backoffSupplier.get();

      @Override
      public void onNext(ReadResponse response) {
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
        bsStubSupplier.get().read(
            request.toBuilder()
                .setReadOffset(requestOffset)
                .build(),
            this);
      }

      @Override
      public void onError(Throwable t) {
        Status status = Status.fromThrowable(t);
        long nextDelayMillis = backoff.nextDelayMillis();
        if (status.getCode() == Status.Code.DEADLINE_EXCEEDED && currentOffset != requestOffset) {
          backoff = backoffSupplier.get();
          retryRequest();
        } else if (retryService == null || nextDelayMillis < 0 || !isRetriable.test(status)) {
          inputStream.setException(t);
        } else {
          ListenableFuture<?> schedulingResult =
              retryService.schedule(
                  this::retryRequest,
                  nextDelayMillis,
                  TimeUnit.MILLISECONDS);
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
        }
      }

      @Override
      public void onCompleted() {
        inputStream.setCompleted();
      }
    };
    bsStubSupplier.get().read(request, responseObserver);
    return inputStream;
  }
}
