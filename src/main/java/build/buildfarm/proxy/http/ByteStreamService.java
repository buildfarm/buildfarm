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

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.UrlPath;
import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusResponse;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
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
  private static final int DEFAULT_CHUNK_SIZE = 1024 * 16;

  private final Map<String, ByteString> active_write_requests;
  private final SimpleBlobStore simpleBlobStore;

  public ByteStreamService(SimpleBlobStore simpleBlobStore) {
    active_write_requests = new HashMap<String, ByteString>();
    this.simpleBlobStore = simpleBlobStore;
  }

  private ListenableFuture<Boolean> getBlob(
      Digest blobDigest,
      long offset,
      long limit,
      OutputStream out) {
    int size = (int) blobDigest.getSizeBytes();
    if (offset < 0 || size < 0
        || (size == 0 && offset > 0)
        || (size > 0 && offset >= size)
        || limit < 0) {
      return immediateFailedFuture(new IndexOutOfBoundsException());
    }

    OutputStream skipLimitOut = new OutputStream() {
      long skipBytesRemaining = offset;
      long writeBytesRemaining = limit > 0 ? limit : (size - offset);

      @Override
      public void close() throws IOException {
        writeBytesRemaining = 0;

        out.close();
      }

      @Override
      public void flush() throws IOException {
        out.flush();
      }

      @Override
      public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        if (skipBytesRemaining >= len) {
          skipBytesRemaining -= len;
        } else if (writeBytesRemaining > 0) {
          off += skipBytesRemaining;
          // technically an error to int-cast
          len = Math.min((int) writeBytesRemaining, (int) (len - skipBytesRemaining));
          out.write(b, off, len);
          skipBytesRemaining = 0;
          writeBytesRemaining -= len;
        }
      }

      @Override
      public void write(int b) throws IOException {
        if (skipBytesRemaining > 0) {
          skipBytesRemaining--;
        } else if (writeBytesRemaining > 0) {
          out.write(b);
          writeBytesRemaining--;
        }
      }
    };

    return simpleBlobStore.get(blobDigest.getHash(), skipLimitOut);
  }

  private void readBlob(
      ReadRequest request,
      StreamObserver<ReadResponse> responseObserver)
      throws IOException, InterruptedException {
    String resourceName = request.getResourceName();

    Digest digest = UrlPath.parseBlobDigest(resourceName);

    OutputStream responseOut = new OutputStream() {
      byte[] buffer = new byte[DEFAULT_CHUNK_SIZE];
      int buflen = 0;

      @Override
      public void close() {
        flush();
      }

      @Override
      public void flush() {
        if (buflen > 0) {
          responseObserver.onNext(ReadResponse.newBuilder()
              .setData(ByteString.copyFrom(buffer, 0, buflen))
              .build());
          buflen = 0;
        }
      }

      @Override
      public void write(byte[] b) {
        write(b, 0, b.length);
      }

      @Override
      public void write(byte[] b, int off, int len) {
        while (buflen + len >= buffer.length) {
          int copylen = buffer.length - buflen;
          System.arraycopy(b, off, buffer, buflen, copylen);
          buflen = buffer.length;
          flush();
          len -= copylen;
          off += copylen;
          if (len == 0) {
            return;
          }
        }
        System.arraycopy(b, off, buffer, buflen, len);
        buflen += len;
      }

      @Override
      public void write(int b) {
        buffer[buflen++] = (byte) b;
        if (buflen == buffer.length) {
          flush();
        }
      }
    };

    addCallback(
        getBlob(
            digest,
            request.getReadOffset(),
            request.getReadLimit(),
            responseOut),
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

          @Override
          public void onFailure(Throwable t) {
            try {
              responseOut.close();
            } catch (IOException ioException) {
              ioException.printStackTrace();
            }
            onError(Status.fromThrowable(t));
          }
        });
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
