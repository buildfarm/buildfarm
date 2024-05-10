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

import static build.buildfarm.common.UrlPath.detectResourceOperation;
import static build.buildfarm.common.UrlPath.parseBlobDigest;
import static build.buildfarm.common.UrlPath.parseUploadBlobDigest;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.UrlPath.InvalidResourceNameException;
import build.buildfarm.common.resources.Resource;
import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusResponse;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

public class ByteStreamService extends ByteStreamGrpc.ByteStreamImplBase {
  private static final int DEFAULT_CHUNK_SIZE = 1024 * 16;

  private final Map<String, WriteObserver> writeObservers = Maps.newConcurrentMap();
  private final SimpleBlobStore simpleBlobStore;

  public ByteStreamService(SimpleBlobStore simpleBlobStore) {
    this.simpleBlobStore = simpleBlobStore;
  }

  private ListenableFuture<Boolean> getBlob(
      Digest blobDigest, long offset, long limit, OutputStream out) {
    int size = (int) blobDigest.getSizeBytes();
    if (offset < 0
        || size < 0
        || (size == 0 && offset > 0)
        || (size > 0 && offset >= size)
        || limit < 0) {
      return immediateFailedFuture(new IndexOutOfBoundsException());
    }

    return simpleBlobStore.get(
        blobDigest.getHash(),
        new SkipLimitOutputStream(out, offset, limit <= 0 ? size - offset : limit));
  }

  private void readBlob(ReadRequest request, StreamObserver<ReadResponse> responseObserver)
      throws InvalidResourceNameException {
    String resourceName = request.getResourceName();

    Digest digest = parseBlobDigest(resourceName);

    OutputStream responseOut =
        new ChunkOutputStream(DEFAULT_CHUNK_SIZE) {
          @Override
          public void onChunk(byte[] b, int len) {
            responseObserver.onNext(
                ReadResponse.newBuilder().setData(ByteString.copyFrom(b, 0, len)).build());
          }
        };

    addCallback(
        getBlob(digest, request.getReadOffset(), request.getReadLimit(), responseOut),
        new FutureCallback<Boolean>() {
          private void onError(Status status) {
            responseObserver.onError(status.asException());
          }

          @Override
          public void onSuccess(Boolean success) {
            if (success) {
              try {
                responseOut.close();
                responseObserver.onCompleted();
              } catch (IOException e) {
                onError(Status.fromThrowable(e));
              }
            } else {
              onError(Status.NOT_FOUND);
            }
          }

          @SuppressWarnings("NullableProblems")
          @Override
          public void onFailure(Throwable t) {
            try {
              responseOut.close();
            } catch (IOException ioException) {
              ioException.printStackTrace();
            }
            onError(Status.fromThrowable(t));
          }
        },
        directExecutor());
  }

  @Override
  public void read(ReadRequest request, StreamObserver<ReadResponse> responseObserver) {
    String resourceName = request.getResourceName();

    long readLimit = request.getReadLimit();
    long readOffset = request.getReadOffset();
    if (readLimit < 0 || readOffset < 0) {
      responseObserver.onError(Status.OUT_OF_RANGE.asException());
      return;
    }

    try {
      Resource.TypeCase resourceOperation = detectResourceOperation(resourceName);
      if (resourceOperation == Resource.TypeCase.DOWNLOAD_BLOB_REQUEST) {
        readBlob(request, responseObserver);
      } else {
        throw new InvalidResourceNameException(resourceName, "Unsupported service");
      }
    } catch (IllegalArgumentException | InvalidResourceNameException e) {
      String description = e.getLocalizedMessage();
      responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(description).asException());
    }
  }

  private Write findBlobWrite(String resourceName)
      throws IOException, InterruptedException, InvalidResourceNameException {
    Digest digest = parseUploadBlobDigest(resourceName);
    if (!simpleBlobStore.containsKey(digest.getHash())) {
      return null;
    }

    return new CompleteWrite() {
      final long committedSize = digest.getSizeBytes();

      @Override
      public long getCommittedSize() {
        return committedSize;
      }
    };
  }

  private Write findWrite(String resourceName)
      throws IOException, InterruptedException, InvalidResourceNameException {
    Write write = writeObservers.get(resourceName);
    if (write != null) {
      return write;
    }

    Resource.TypeCase resourceOperation = detectResourceOperation(resourceName);
    switch (resourceOperation) {
      case UPLOAD_BLOB_REQUEST:
        return findBlobWrite(resourceName);
      case STREAM_OPERATION_REQUEST:
      default:
        throw Status.INVALID_ARGUMENT.asRuntimeException();
    }
  }

  @Override
  public void queryWriteStatus(
      QueryWriteStatusRequest request, StreamObserver<QueryWriteStatusResponse> responseObserver) {
    String resourceName = request.getResourceName();

    Write write;
    try {
      write = findWrite(resourceName);
    } catch (InvalidResourceNameException e) {
      String description = e.getLocalizedMessage();
      responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(description).asException());
      return;
    } catch (IOException e) {
      responseObserver.onError(Status.fromThrowable(e).asException());
      return;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
    }

    if (write != null) {
      responseObserver.onNext(
          QueryWriteStatusResponse.newBuilder()
              .setCommittedSize(write.getCommittedSize())
              .setComplete(write.isComplete())
              .build());
      responseObserver.onCompleted();
    } else {
      responseObserver.onError(Status.NOT_FOUND.asException());
    }
  }

  private WriteObserver createWriteObserver(String resourceName)
      throws InvalidResourceNameException {
    Resource.TypeCase resourceOperation = detectResourceOperation(resourceName);
    switch (resourceOperation) {
      case UPLOAD_BLOB_REQUEST:
        return new BlobWriteObserver(resourceName, simpleBlobStore);
      case STREAM_OPERATION_REQUEST:
      default:
        throw Status.INVALID_ARGUMENT.asRuntimeException();
    }
  }

  private WriteObserver getOrCreateWriteObserver(String resourceName)
      throws InvalidResourceNameException {
    WriteObserver writeObserver = writeObservers.get(resourceName);
    if (writeObserver == null) {
      writeObserver = createWriteObserver(resourceName);
      writeObservers.put(resourceName, writeObserver);
    }
    return writeObserver;
  }

  @Override
  public StreamObserver<WriteRequest> write(StreamObserver<WriteResponse> responseObserver) {
    return new WriteStreamObserver(
        responseObserver,
        new WriteObserverSource() {
          @Override
          public WriteObserver get(String resourceName) throws InvalidResourceNameException {
            return getOrCreateWriteObserver(resourceName);
          }

          @Override
          public void remove(String resourceName) {
            writeObservers.remove(resourceName);
          }
        });
  }
}
