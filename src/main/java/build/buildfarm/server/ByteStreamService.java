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

import build.buildfarm.common.DigestUtil;
import build.buildfarm.instance.Instance;
import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusResponse;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ByteStreamService extends ByteStreamGrpc.ByteStreamImplBase {
  private static final long DEFAULT_CHUNK_SIZE = 1024 * 16;

  private final Map<String, ByteString> active_write_requests;
  private final Instances instances;

  public ByteStreamService(Instances instances) {
    active_write_requests = new HashMap<String, ByteString>();
    this.instances = instances;
  }

  private void readBlob(
      ReadRequest request,
      StreamObserver<ReadResponse> responseObserver) {
    try {
      String resourceName = request.getResourceName();
      Instance instance = instances.getFromBlob(resourceName);

      Digest digest = UrlPath.parseBlobDigest(resourceName, instance.getDigestUtil());

      ByteString blob = instance.getBlob(
          digest, request.getReadOffset(), request.getReadLimit());
      if (blob == null) {
        responseObserver.onError(new StatusException(Status.NOT_FOUND));
        return;
      }

      while (!blob.isEmpty()) {
        ByteString chunk;
        if (blob.size() < DEFAULT_CHUNK_SIZE) {
          chunk = blob;
          blob = ByteString.EMPTY;
        } else {
          chunk = blob.substring(0, (int) DEFAULT_CHUNK_SIZE);
          blob = blob.substring((int) DEFAULT_CHUNK_SIZE);
        }
        responseObserver.onNext(ReadResponse.newBuilder()
            .setData(chunk)
            .build());
      }

      responseObserver.onCompleted();
    } catch (InstanceNotFoundException ex) {
      responseObserver.onError(BuildFarmInstances.toStatusException(ex));
    }
  }

  private void readOperationStream(
      ReadRequest request,
      StreamObserver<ReadResponse> responseObserver) throws IOException {
    String resourceName = request.getResourceName();

    Instance instance;
    try {
      instance = instances.getFromBlob(resourceName);
    } catch (InstanceNotFoundException ex) {
      responseObserver.onError(BuildFarmInstances.toStatusException(ex));
      return;
    }

    String operationStream = UrlPath.parseOperationStream(resourceName);

    InputStream input = instance.newStreamInput(operationStream);
    long readLimit = request.getReadLimit();
    long readOffset = request.getReadOffset();
    while (readOffset > 0) {
      long n = input.skip(readOffset);
      if (n == 0) {
        responseObserver.onError(new StatusException(Status.OUT_OF_RANGE));
        return;
      }
      readOffset -= n;
    }
    boolean unlimitedReadLimit = readLimit == 0;
    byte[] buffer = new byte[(int) Math.min(readLimit, DEFAULT_CHUNK_SIZE)];
    int len;
    while ((unlimitedReadLimit || readLimit > 0) &&
           (len = input.read(buffer, 0, (int) Math.min(buffer.length, readLimit))) >= 0) {
      if (len == 0)
        continue;
      if (!unlimitedReadLimit) {
        readLimit -= len;
      }
      responseObserver.onNext(ReadResponse.newBuilder()
          .setData(ByteString.copyFrom(buffer, 0, len))
          .build());
    }
    responseObserver.onCompleted();
  }

  @Override
  public void read(
      ReadRequest request,
      StreamObserver<ReadResponse> responseObserver) {
    String resourceName = request.getResourceName();

    long readLimit = request.getReadLimit();
    long readOffset = request.getReadOffset();
    if (readLimit < 0 || readOffset < 0) {
      responseObserver.onError(new StatusException(Status.OUT_OF_RANGE));
      return;
    }

    try {
      Optional<UrlPath.ResourceOperation> resourceOperation = Optional.empty();
      try {
        resourceOperation = Optional.of(UrlPath.detectResourceOperation(resourceName));
      } catch (IllegalArgumentException ex) {
        String description = ex.getLocalizedMessage();
        responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT.withDescription(description)));
        return;
      }
      switch (resourceOperation.get()) {
      case Blob:
        readBlob(request, responseObserver);
        break;
      case OperationStream:
        readOperationStream(request, responseObserver);
        break;
      default:
        String description = "Invalid service";
        responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT.withDescription(description)));
        break;
      }
    } catch(IOException ex) {
      responseObserver.onError(new StatusException(Status.fromThrowable(ex)));
    }
  }

  @Override
  public void queryWriteStatus(
      QueryWriteStatusRequest request,
      StreamObserver<QueryWriteStatusResponse> responseObserver) {
    String resourceName = request.getResourceName();

    Optional<UrlPath.ResourceOperation> resourceOperation = Optional.empty();
    try {
      resourceOperation = Optional.of(UrlPath.detectResourceOperation(resourceName));
    } catch (IllegalArgumentException ex) {
      String description = ex.getLocalizedMessage();
      responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT.withDescription(description)));
      return;
    }

    switch (resourceOperation.get()) {
    case UploadBlob:
      responseObserver.onError(new StatusException(Status.UNIMPLEMENTED));
      break;
    case OperationStream:
      responseObserver.onError(new StatusException(Status.UNIMPLEMENTED));
      break;
    default:
      String description = "Invalid service";
      responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT.withDescription(description)));
      break;
    }
  }

  @Override
  public StreamObserver<WriteRequest> write(
      final StreamObserver<WriteResponse> responseObserver) {
    return new StreamObserver<WriteRequest>() {
      long committed_size = 0;
      ByteString data = null;
      boolean finished = false;
      boolean failed = false;
      String writeResourceName = null;

      Digest digest;

      private void writeBlob(
          WriteRequest request, StreamObserver<WriteResponse> responseObserver)
          throws InterruptedException {
        try {
          Instance instance = instances.getFromUploadBlob(writeResourceName);

          writeInstanceBlob(request, responseObserver, instance);
        } catch (InstanceNotFoundException ex) {
          responseObserver.onError(BuildFarmInstances.toStatusException(ex));
          failed = true;
        }
      }

      private void writeInstanceBlob(
          WriteRequest request, StreamObserver<WriteResponse> responseObserver,
          Instance instance)
          throws InterruptedException {
        if (data == null) {
          digest = UrlPath.parseUploadBlobDigest(writeResourceName, instance.getDigestUtil());
          if (digest == null) {
            String description = "Could not parse digest of: " + writeResourceName;
            responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT.withDescription(description)));
            failed = true;
            return;
          }
          data = active_write_requests.get(writeResourceName);
          if (data != null) {
            committed_size = data.size();
          }
        }
        if (request.getWriteOffset() != committed_size) {
          String description = "Write offset invalid: " + request.getWriteOffset();
          responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT.withDescription(description)));
          failed = true;
          return;
        }
        ByteString chunk = request.getData();
        if (data == null) {
          data = chunk;
          committed_size = data.size();
          active_write_requests.put(writeResourceName, data);
        } else {
          data = data.concat(chunk);
          committed_size += chunk.size();
        }
        if (request.getFinishWrite()) {
          active_write_requests.remove(writeResourceName);
          Digest blobDigest = instance.getDigestUtil().compute(data);
          if (!blobDigest.equals(digest)) {
            String description = String.format("Digest mismatch %s <-> %s", DigestUtil.toString(blobDigest), DigestUtil.toString(digest));
            responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT.withDescription(description)));
            failed = true;
          } else {
            try {
              instance.putBlob(data);
            } catch (IOException ex) {
              responseObserver.onError(new StatusException(Status.fromThrowable(ex)));
            } catch (StatusException ex) {
              responseObserver.onError(ex);
              failed = true;
            }
          }
        }
      }

      private void writeOperationStream(
          WriteRequest request, StreamObserver<WriteResponse> responseObserver) throws IOException {
        try {
          Instance instance = instances.getFromOperationStream(writeResourceName);

          String operationStream = UrlPath.parseOperationStream(writeResourceName);

          OutputStream outputStream = instance.getStreamOutput(operationStream);
          request.getData().writeTo(outputStream);
          if (request.getFinishWrite()) {
            outputStream.close();
          }
        } catch (InstanceNotFoundException ex) {
          responseObserver.onError(BuildFarmInstances.toStatusException(ex));
        }
      }

      @Override
      public void onNext(WriteRequest request) {
        if (finished) {
          // FIXME does bytestream have a standard status for this invalid request?
          responseObserver.onError(new StatusException(Status.OUT_OF_RANGE));
          failed = true;
        } else {
          String resourceName = request.getResourceName();
          if (resourceName.isEmpty()) {
            if (writeResourceName == null) {
              String description = "Missing resource name in request";
              responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT.withDescription(description)));
              failed = true;
            } else {
              resourceName = writeResourceName;
            }
          } else if (writeResourceName == null) {
            writeResourceName = resourceName;
          } else if (!writeResourceName.equals(resourceName)) {
            String description = String.format("Previous resource name changed while handling request. %s -> %s", writeResourceName, resourceName);
            responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT.withDescription(description)));
            failed = true;
          }
        }

        if (failed) {
          return;
        }
        
        Optional<UrlPath.ResourceOperation> resourceOperation = Optional.empty();
        try {
          resourceOperation = Optional.of(UrlPath.detectResourceOperation(writeResourceName));
        } catch (IllegalArgumentException ex) {
          String description = ex.getLocalizedMessage();
          responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT.withDescription(description)));
          failed = true;
        }

        if (failed) {
          return;
        }

        try {
          switch (resourceOperation.get()) {
          case UploadBlob:
            writeBlob(request, responseObserver);
            finished = request.getFinishWrite();
            break;
          case OperationStream:
            writeOperationStream(request, responseObserver);
            finished = request.getFinishWrite();
            break;
          default:
            responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
            failed = true;
            break;
          }
        } catch(InterruptedException ex) {
          responseObserver.onError(new StatusException(Status.fromThrowable(ex)));
          failed = true;
        } catch(IOException ex) {
          responseObserver.onError(new StatusException(Status.fromThrowable(ex)));
          failed = true;
        }
      }

      @Override
      public void onError(Throwable t) {
        // has the connection closed at this point?
        failed = true;
      }

      @Override
      public void onCompleted() {
        if (failed)
          return;

        responseObserver.onNext(WriteResponse.newBuilder()
            .setCommittedSize(committed_size)
            .build());
        responseObserver.onCompleted();
      }
    };
  }
}
