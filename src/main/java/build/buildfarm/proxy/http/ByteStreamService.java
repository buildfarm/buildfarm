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

package build.buildfarm.proxy.http;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.UrlPath;
import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusResponse;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
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
  private final SimpleBlobStore simpleBlobStore;

  public ByteStreamService(SimpleBlobStore simpleBlobStore) {
    active_write_requests = new HashMap<String, ByteString>();
    this.simpleBlobStore = simpleBlobStore;
  }

  private ByteString getBlob(Digest blobDigest, long offset, long limit)
      throws IOException, IndexOutOfBoundsException, InterruptedException {
    int size = (int) blobDigest.getSizeBytes();
    ByteArrayOutputStream stream = new ByteArrayOutputStream(size);
    if (!simpleBlobStore.get(blobDigest.getHash(), stream)) {
      return null;
    }

    ByteString blob = ByteString.copyFrom(stream.toByteArray());

    if (offset < 0
        || (blob.isEmpty() && offset > 0)
        || (!blob.isEmpty() && offset >= size)
        || limit < 0) {
      throw new IndexOutOfBoundsException();
    }

    long endIndex = offset + (limit > 0 ? limit : (size - offset));

    return blob.substring(
        (int) offset,
        (int) (endIndex > size ? size : endIndex));
  }

  private void readBlob(
      ReadRequest request,
      StreamObserver<ReadResponse> responseObserver)
      throws IOException, InterruptedException {
    String resourceName = request.getResourceName();

    Digest digest = UrlPath.parseBlobDigest(resourceName);

    ByteString blob = getBlob(digest, request.getReadOffset(), request.getReadLimit());
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
      } catch (IllegalArgumentException e) {
        String description = e.getLocalizedMessage();
        responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT.withDescription(description)));
        return;
      }
      switch (resourceOperation.get()) {
      case Blob:
        readBlob(request, responseObserver);
        break;
      default:
        String description = "Invalid service";
        responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT.withDescription(description)));
        break;
      }
    } catch (IOException e) {
      responseObserver.onError(new StatusException(Status.fromThrowable(e)));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
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
    } catch (IllegalArgumentException e) {
      String description = e.getLocalizedMessage();
      responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT.withDescription(description)));
      return;
    }

    switch (resourceOperation.get()) {
    case UploadBlob:
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
          WriteRequest request,
          StreamObserver<WriteResponse> responseObserver)
          throws InterruptedException {
        if (data == null) {
          digest = UrlPath.parseUploadBlobDigest(writeResourceName);
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
          try {
            simpleBlobStore.put(digest.getHash(), data.size(), data.newInput());
          } catch (IOException e) {
            responseObserver.onError(new StatusException(Status.fromThrowable(e)));
            failed = true;
          }
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
        } catch (IllegalArgumentException e) {
          String description = e.getLocalizedMessage();
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
          default:
            responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
            failed = true;
            break;
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
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
