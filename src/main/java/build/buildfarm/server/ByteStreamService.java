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

import build.buildfarm.common.Digests;
import build.buildfarm.instance.Instance;
import com.google.common.collect.Iterables;
import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusResponse;
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

public class ByteStreamService extends ByteStreamGrpc.ByteStreamImplBase {
  private static final long DEFAULT_CHUNK_SIZE = 1024 * 16;

  private final Map<String, ByteString> active_write_requests;
  private final BuildFarmServer server;

  public ByteStreamService(BuildFarmServer server) {
    active_write_requests = new HashMap<String, ByteString>();
    this.server = server;
  }

  private static boolean isBlob(String resourceName) {
    // {instance_name=**}/blobs/{hash}/{size}
    String[] components = resourceName.split("/");
    return components.length >= 3 &&
      components[components.length - 3].equals("blobs");
  }

  private static boolean isUploadBlob(String resourceName) {
    // {instance_name=**}/uploads/{uuid}/blobs/{hash}/{size}
    String[] components = resourceName.split("/");
    return components.length >= 5 &&
      components[components.length - 3].equals("blobs") &&
      components[components.length - 5].equals("uploads");
  }

  private static boolean isOperationStream(String resourceName) {
    // {instance_name=**}/operations/{uuid}/streams/{stream}
    String[] components = resourceName.split("/");
    return components.length >= 4 &&
        components[components.length - 2].equals("streams") &&
        components[components.length - 4].equals("operations");
  }

  private static Digest parseBlobDigest(String resourceName)
      throws IllegalArgumentException {
    String[] components = resourceName.split("/");
    String hash = components[components.length - 2];
    long size = Long.parseLong(components[components.length - 1]);
    return Digests.buildDigest(hash, size);
  }

  private static Digest parseUploadBlobDigest(String resourceName)
      throws IllegalArgumentException {
    String[] components = resourceName.split("/");
    String hash = components[components.length - 2];
    long size = Long.parseLong(components[components.length - 1]);
    return Digests.buildDigest(hash, size);
  }

  public String parseOperationStream(String resourceName) {
    String[] components = resourceName.split("/");
    return String.join("/", Arrays.asList(components).subList(components.length - 4, components.length));
  }

  private void readBlob(
      ReadRequest request,
      StreamObserver<ReadResponse> responseObserver) {
    String resourceName = request.getResourceName();

    Instance instance = server.getInstanceFromBlob(resourceName);
    Digest digest = parseBlobDigest(resourceName);

    ByteString blob = instance.getBlob(
        digest, request.getReadOffset(), request.getReadLimit());
    if (blob == null) {
      responseObserver.onError(new StatusException(Status.NOT_FOUND));
      return;
    }

    responseObserver.onNext(ReadResponse.newBuilder()
        .setData(blob)
        .build());
    responseObserver.onCompleted();
  }

  private void readOperationStream(
      ReadRequest request,
      StreamObserver<ReadResponse> responseObserver) throws IOException {
    long readLimit = request.getReadLimit();
    long readOffset = request.getReadOffset();
    if (readLimit < 0 || readOffset < 0) {
      responseObserver.onError(new StatusException(Status.OUT_OF_RANGE));
      return;
    }

    String resourceName = request.getResourceName();

    Instance instance = server.getInstanceFromBlob(resourceName);
    String operationStream = parseOperationStream(resourceName);

    InputStream input = instance.newStreamInput(operationStream);
    while (readOffset > 0) {
      long n = input.skip(readOffset);
      if (n == 0) {
        responseObserver.onError(new StatusException(Status.OUT_OF_RANGE));
        return;
      }
      readOffset -= n;
    }
    boolean unlimitedReadLimit = readLimit == 0;
    if (unlimitedReadLimit) {
      readLimit = DEFAULT_CHUNK_SIZE;
    }
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

    try {
      if (isBlob(resourceName)) {
        readBlob(request, responseObserver);
      } else if (isOperationStream(resourceName)) {
        readOperationStream(request, responseObserver);
      } else {
        responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
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

    if (isUploadBlob(resourceName)) {
      responseObserver.onError(new StatusException(Status.UNIMPLEMENTED));
    } else if (isOperationStream(resourceName)) {
      responseObserver.onError(new StatusException(Status.UNIMPLEMENTED));
    } else {
      responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
    }
  }

  @Override
  public StreamObserver<WriteRequest> write(
      final StreamObserver<WriteResponse> responseObserver) {
    return new StreamObserver<WriteRequest>() {
      long committed_size = 0;
      ByteString data = null;
      boolean failed = false;

      Digest digest;

      private void writeBlob(
          WriteRequest request, StreamObserver<WriteResponse> responseObserver)
          throws InterruptedException {
        if (failed) {
          return;
        }

        String resourceName = request.getResourceName();
        if( data == null ) {
          digest = parseUploadBlobDigest(resourceName);
          if (digest == null) {
            responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
            failed = true;
            return;
          }
          data = active_write_requests.get(resourceName);
          if (data != null) {
            committed_size = data.size();
          }
          /* should we support independent stream writes??
        } else if (this.resourceName != resourceName) {
          responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
          failed = true;
          return;
          */
        }
        if( request.getWriteOffset() != committed_size ) {
          responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
          failed = true;
          return;
        }
        ByteString chunk = request.getData();
        if (data == null) {
          data = chunk;
          committed_size = data.size();
          active_write_requests.put(resourceName, data);
        } else {
          data = data.concat(chunk);
          committed_size += chunk.size();
        }
        if( request.getFinishWrite() ) {
          active_write_requests.remove(resourceName);
          Instance instance = server.getInstanceFromUploadBlob(resourceName);
          Digest blobDigest = Digests.computeDigest(data);
          if (!blobDigest.equals(digest)) {
            responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
            failed = true;
          } else {
            instance.putBlob(data);
          }
        }
      }

      private void writeOperationStream(
          WriteRequest request, StreamObserver<WriteResponse> responseObserver) throws IOException {
        String resourceName = request.getResourceName();
        Instance instance =
          server.getInstanceFromOperationStream(resourceName);

        String operationStream = parseOperationStream(resourceName);

        OutputStream outputStream = instance.getStreamOutput(operationStream);
        outputStream.write(request.getData().toByteArray());
        if (request.getFinishWrite()) {
          outputStream.close();
        }
      }

      @Override
      public void onNext(WriteRequest request) {
        String resourceName = request.getResourceName();

        try {
          if (isUploadBlob(resourceName)) {
            writeBlob(request, responseObserver);
          } else if (isOperationStream(resourceName)) {
            writeOperationStream(request, responseObserver);
          } else {
            responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
          }
        } catch(InterruptedException ex) {
          responseObserver.onError(new StatusException(Status.fromThrowable(ex)));
        } catch(IOException ex) {
          responseObserver.onError(new StatusException(Status.fromThrowable(ex)));
        }
      }

      @Override
      public void onError(Throwable t) {
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
