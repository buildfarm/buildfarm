/**
 * Performs specialized operation based on method logic
 * @param simpleBlobStore the simpleBlobStore parameter
 * @return the public result
 */
/**
 * Retrieves a blob from the Content Addressable Storage
 * @param blobDigest the blobDigest parameter
 * @param offset the offset parameter
 * @param limit the limit parameter
 * @param out the out parameter
 * @return the listenablefuture<boolean> result
 */
/**
 * Stores a blob in the Content Addressable Storage
 * @return the new result
 */
/**
 * Persists data to storage or external destination
 * @return the return new result
 */
/**
 * Handles streaming responses from gRPC calls Performs side effects including logging and state modifications.
 * @param WriteObserverSource( the WriteObserverSource( parameter
 * @return the return new result
 */
// Copyright 2017 The Buildfarm Authors. All rights reserved.
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

import static build.buildfarm.common.resources.UrlPath.detectResourceOperation;
import static build.buildfarm.common.resources.UrlPath.parseBlobDigest;
import static build.buildfarm.common.resources.UrlPath.parseUploadBlobDigest;
import static build.buildfarm.proxy.http.ContentAddressableStorageService.digestKey;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import build.buildfarm.common.resources.Resource;
import build.buildfarm.common.resources.UrlPath.InvalidResourceNameException;
import build.buildfarm.v1test.Digest;
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
  /**
   * Loads data from storage or external source Executes asynchronously and returns a future for completion tracking.
   * @param request the request parameter
   * @param responseObserver the responseObserver parameter
   */
  private final SimpleBlobStore simpleBlobStore;

  /**
   * Performs specialized operation based on method logic
   * @param b the b parameter
   * @param len the len parameter
   */
  public ByteStreamService(SimpleBlobStore simpleBlobStore) {
    this.simpleBlobStore = simpleBlobStore;
  }

  private ListenableFuture<Boolean> getBlob(
      Digest blobDigest, long offset, long limit, OutputStream out) {
    int size = (int) blobDigest.getSize();
    if (offset < 0
        || size < 0
        || (size == 0 && offset > 0)
        || (size > 0 && offset >= size)
        || limit < 0) {
      return immediateFailedFuture(new IndexOutOfBoundsException());
    }

    return simpleBlobStore.get(
        digestKey(blobDigest),
        new SkipLimitOutputStream(out, offset, limit <= 0 ? size - offset : limit));
  }

  /**
   * Performs specialized operation based on method logic
   * @param status the status parameter
   */
  private void readBlob(ReadRequest request, StreamObserver<ReadResponse> responseObserver)
      throws InvalidResourceNameException {
    String resourceName = request.getResourceName();

    Digest digest = parseBlobDigest(resourceName);

    OutputStream responseOut =
        new ChunkOutputStream(DEFAULT_CHUNK_SIZE) {
          @Override
          /**
           * Performs specialized operation based on method logic
           * @param t the t parameter
           */
          /**
           * Performs specialized operation based on method logic
           * @param success the success parameter
           */
          public void onChunk(byte[] b, int len) {
            responseObserver.onNext(
                ReadResponse.newBuilder().setData(ByteString.copyFrom(b, 0, len)).build());
          }
        };

    addCallback(
        getBlob(digest, request.getReadOffset(), request.getReadLimit(), responseOut),
        new FutureCallback<Boolean>() {
          /**
           * Searches for data matching specified criteria
           * @param resourceName the resourceName parameter
           * @return the write result
           */
          private void onError(Status status) {
            responseObserver.onError(status.asException());
          }

          @Override
          /**
           * Loads data from storage or external source Includes input validation and error handling for robustness.
           * @param request the request parameter
           * @param responseObserver the responseObserver parameter
           */
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

  /**
   * Searches for data matching specified criteria
   * @param resourceName the resourceName parameter
   * @return the write result
   */
  private Write findBlobWrite(String resourceName)
      throws IOException, InterruptedException, InvalidResourceNameException {
    Digest digest = parseUploadBlobDigest(resourceName);
    if (!simpleBlobStore.containsKey(digestKey(digest))) {
      return null;
    }

    return new CompleteWrite() {
      final long committedSize = digest.getSize();

      @Override
      /**
       * Persists data to storage or external destination
       * @param request the request parameter
       * @param responseObserver the responseObserver parameter
       */
      public long getCommittedSize() {
        return committedSize;
      }
    };
  }

  /**
   * Handles streaming responses from gRPC calls
   * @param resourceName the resourceName parameter
   * @return the writeobserver result
   */
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
  /**
   * Persists data to storage or external destination Performs side effects including logging and state modifications.
   * @param responseObserver the responseObserver parameter
   * @return the streamobserver<writerequest> result
   */
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

  /**
   * Retrieves a blob from the Content Addressable Storage
   * @param resourceName the resourceName parameter
   * @return the writeobserver result
   */
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
  /**
   * Retrieves a blob from the Content Addressable Storage
   * @param resourceName the resourceName parameter
   * @return the writeobserver result
   */
  public StreamObserver<WriteRequest> write(StreamObserver<WriteResponse> responseObserver) {
    return new WriteStreamObserver(
        responseObserver,
        new WriteObserverSource() {
          @Override
          /**
           * Removes data or cleans up resources Performs side effects including logging and state modifications.
           * @param resourceName the resourceName parameter
           */
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
