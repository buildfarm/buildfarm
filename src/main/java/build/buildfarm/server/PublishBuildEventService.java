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

package build.buildfarm.server;

import static build.buildfarm.common.UrlPath.detectResourceOperation;
import static build.buildfarm.common.UrlPath.parseBlobDigest;
import static build.buildfarm.common.UrlPath.parseUploadBlobDigest;
import static build.buildfarm.common.UrlPath.parseUploadBlobUUID;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static io.grpc.Status.INVALID_ARGUMENT;
import static io.grpc.Status.NOT_FOUND;
import static io.grpc.Status.OUT_OF_RANGE;
import static java.lang.String.format;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.EntryLimitException;
import build.buildfarm.common.UrlPath.InvalidResourceNameException;
import build.buildfarm.common.Write;
import build.buildfarm.common.Write.CompleteWrite;
import build.buildfarm.common.grpc.DelegateServerCallStreamObserver;
import build.buildfarm.common.grpc.TracingMetadataUtils;
import build.buildfarm.common.grpc.UniformDelegateServerCallStreamObserver;
import build.buildfarm.common.io.FeedbackOutputStream;
import build.buildfarm.instance.Instance;
import com.google.bytestream.ByteStreamGrpc.ByteStreamImplBase;
import com.google.devtools.build.v1.PublishBuildEventGrpc.PublishBuildEventImplBase;
import com.google.devtools.build.v1.PublishBuildToolEventStreamRequest;
import com.google.devtools.build.v1.PublishBuildToolEventStreamResponse;
import com.google.devtools.build.v1.PublishLifecycleEventRequest;
import com.google.devtools.build.v1.StreamId;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusResponse;
import com.google.protobuf.Empty;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PublishBuildEventService extends PublishBuildEventImplBase {
    
  public static final Logger logger = Logger.getLogger(PublishBuildEventService.class.getName());
    
  @Override
  public void publishLifecycleEvent(PublishLifecycleEventRequest request,
                                    StreamObserver<Empty> responseObserver) {
    //TODO
    responseObserver.onNext(Empty.getDefaultInstance());
    responseObserver.onCompleted();
  }
  
  @Override
  public StreamObserver<PublishBuildToolEventStreamResponse> publishBuildToolEventStream(
      StreamObserver<PublishBuildToolEventStreamRequest> responseObserver) {
    return new StreamObserver<PublishBuildToolEventStreamRequest>() {
      @Override
      public void onNext(PublishBuildToolEventStreamRequest in) {
        StreamId streamId = in.getOrderedBuildEvent().getStreamId();
        long sequenceNumber = in.getOrderedBuildEvent().getSequenceNumber();
        recordEvent(in);
        responseObserver.onNext(PublishBuildToolEventStreamResponse.newBuilder().setStreamId(streamId).setSequenceNumber(sequenceNumber).build());
      }

      @Override
      public void onError(Throwable err) {
        //logger.error("Could not publish Build Tool Event", err);
        responseObserver.onError(new StatusException(Status.fromThrowable(err)));
      }

      @Override
      public void onCompleted() {
        responseObserver.onCompleted();
      }
    };
  }

  private void recordEvent(PublishBuildToolEventStreamRequest in) {
    //TODO
    //logger.info("BES Data: {}", in.getOrderedBuildEvent().toString());
    in.getOrderedBuildEvent().getEvent();
  }
  
}