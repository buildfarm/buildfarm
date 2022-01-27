// Copyright 2021 The Bazel Authors. All rights reserved.
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

package build.buildfarm.server;

import build.buildfarm.v1test.BuildEventConfig;
import com.google.devtools.build.v1.PublishBuildEventGrpc.PublishBuildEventImplBase;
import com.google.devtools.build.v1.PublishBuildToolEventStreamRequest;
import com.google.devtools.build.v1.PublishBuildToolEventStreamResponse;
import com.google.devtools.build.v1.PublishLifecycleEventRequest;
import com.google.devtools.build.v1.StreamId;
import com.google.protobuf.Empty;
import com.google.protobuf.TextFormat;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PublishBuildEventService extends PublishBuildEventImplBase {
  public static final Logger logger = Logger.getLogger(PublishBuildEventService.class.getName());
  private final BuildEventConfig config;

  public PublishBuildEventService(BuildEventConfig config) {
    this.config = config;
  }

  @Override
  public void publishLifecycleEvent(
      PublishLifecycleEventRequest request, StreamObserver<Empty> responseObserver) {
    responseObserver.onNext(Empty.getDefaultInstance());
    responseObserver.onCompleted();
  }

  @Override
  public StreamObserver<PublishBuildToolEventStreamRequest> publishBuildToolEventStream(
      StreamObserver<PublishBuildToolEventStreamResponse> responseObserver) {
    return new StreamObserver<PublishBuildToolEventStreamRequest>() {
      @Override
      public void onNext(PublishBuildToolEventStreamRequest in) {
        StreamId streamId = in.getOrderedBuildEvent().getStreamId();
        long sequenceNumber = in.getOrderedBuildEvent().getSequenceNumber();
        recordEvent(in);
        responseObserver.onNext(
            PublishBuildToolEventStreamResponse.newBuilder()
                .setStreamId(streamId)
                .setSequenceNumber(sequenceNumber)
                .build());
      }

      @Override
      public void onError(Throwable err) {
        responseObserver.onError(new StatusException(Status.fromThrowable(err)));
      }

      @Override
      public void onCompleted() {
        responseObserver.onCompleted();
      }
    };
  }

  private void recordEvent(PublishBuildToolEventStreamRequest in) {
    if (config.getRecordEvents() && in.hasOrderedBuildEvent()) {
      logger.log(Level.INFO, TextFormat.shortDebugString(in.getOrderedBuildEvent()));
    }
  }
}
