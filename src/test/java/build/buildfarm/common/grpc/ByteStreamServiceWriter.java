// Copyright 2019 The Bazel Authors. All rights reserved.
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

import static build.buildfarm.common.grpc.Retrier.DEFAULT_IS_RETRIABLE;
import static com.google.common.base.Preconditions.checkState;

import com.google.bytestream.ByteStreamGrpc.ByteStreamImplBase;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;

public class ByteStreamServiceWriter extends ByteStreamImplBase {
  private final String resourceName;
  private final SettableFuture<ByteString> content;
  private final ByteString.Output out;

  public ByteStreamServiceWriter(String resourceName, SettableFuture<ByteString> content) {
    this(resourceName, content, /* expectedSize= */ 0);
  }

  public ByteStreamServiceWriter(
      String resourceName, SettableFuture<ByteString> content, int initialCapacity) {
    this.resourceName = resourceName;
    this.content = content;
    out = ByteString.newOutput(initialCapacity);
  }

  class WriteStreamObserver implements StreamObserver<WriteRequest> {
    private final StreamObserver<WriteResponse> responseObserver;
    private boolean finished = false;
    private boolean hasSeenResourceName = false;

    WriteStreamObserver(StreamObserver<WriteResponse> responseObserver) {
      this.responseObserver = responseObserver;
    }

    @Override
    public void onNext(WriteRequest request) {
      checkState(
          (hasSeenResourceName && request.getResourceName().isEmpty())
              || request.getResourceName().equals(resourceName));
      hasSeenResourceName = true;
      checkState(!finished);
      ByteString data = request.getData();
      if (data.isEmpty() || request.getWriteOffset() == out.size()) {
        try {
          request.getData().writeTo(out);
          finished = request.getFinishWrite();
          if (finished) {
            long committedSize = out.size();
            content.set(out.toByteString());
            responseObserver.onNext(
                WriteResponse.newBuilder().setCommittedSize(committedSize).build());
          }
        } catch (IOException e) {
          responseObserver.onError(Status.fromThrowable(e).asException());
        }
      } else {
        responseObserver.onError(Status.INVALID_ARGUMENT.asException());
      }
    }

    @Override
    public void onError(Throwable t) {
      if (!DEFAULT_IS_RETRIABLE.test(Status.fromThrowable(t))) {
        content.setException(t);
      }
    }

    @Override
    public void onCompleted() {
      responseObserver.onCompleted();
    }
  }

  @Override
  public StreamObserver<WriteRequest> write(StreamObserver<WriteResponse> responseObserver) {
    return new WriteStreamObserver(responseObserver);
  }

  @Override
  public void queryWriteStatus(
      QueryWriteStatusRequest request, StreamObserver<QueryWriteStatusResponse> responseObserver) {
    if (request.getResourceName().equals(resourceName)) {
      responseObserver.onNext(
          QueryWriteStatusResponse.newBuilder()
              .setCommittedSize(out.size())
              .setComplete(content.isDone())
              .build());
      responseObserver.onCompleted();
    } else {
      responseObserver.onError(Status.NOT_FOUND.asException());
    }
  }
}
