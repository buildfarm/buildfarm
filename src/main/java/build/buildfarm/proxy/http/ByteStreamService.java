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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static build.buildfarm.common.UrlPath.detectResourceOperation;
import static build.buildfarm.common.UrlPath.parseUploadBlobDigest;
import static build.buildfarm.common.UrlPath.parseBlobDigest;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.RingBufferInputStream;
import build.buildfarm.common.UrlPath.InvalidResourceNameException;
import build.buildfarm.common.UrlPath.ResourceOperation;
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
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Logger;

public class ByteStreamService extends ByteStreamGrpc.ByteStreamImplBase {
  private static final Logger logger = Logger.getLogger(ByteStreamService.class.getName());
  private static final int DEFAULT_CHUNK_SIZE = 1024 * 16;

  private interface Write extends StreamObserver<WriteRequest> {
    long getCommittedSize();
    boolean getComplete();
  }

  private final Map<String, Write> writes = Maps.newConcurrentMap();
  private final SimpleBlobStore simpleBlobStore;

  public ByteStreamService(SimpleBlobStore simpleBlobStore) {
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
      throws IOException, InterruptedException, InvalidResourceNameException {
    String resourceName = request.getResourceName();

    Digest digest = parseBlobDigest(resourceName);

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
      responseObserver.onError(Status.OUT_OF_RANGE.asException());
      return;
    }

    try {
      Optional<ResourceOperation> resourceOperation = Optional.empty();
      try {
        resourceOperation = Optional.of(detectResourceOperation(resourceName));
      } catch (IllegalArgumentException e) {
        String description = e.getLocalizedMessage();
        responseObserver.onError(Status.INVALID_ARGUMENT
            .withDescription(description)
            .asException());
        return;
      }
      switch (resourceOperation.get()) {
      case Blob:
        readBlob(request, responseObserver);
        break;
      default:
        String description = "Invalid service";
        responseObserver.onError(Status.INVALID_ARGUMENT
            .withDescription(description)
            .asException());
        break;
      }
    } catch (InvalidResourceNameException e) {
      String description = e.getLocalizedMessage();
      responseObserver.onError(Status.INVALID_ARGUMENT
          .withDescription(description)
          .asException());
    } catch (IOException e) {
      responseObserver.onError(Status.fromThrowable(e).asException());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void queryWriteStatus(
      QueryWriteStatusRequest request,
      StreamObserver<QueryWriteStatusResponse> responseObserver) {
    String resourceName = request.getResourceName();

    Write write = writes.get(resourceName);
    if (write != null) {
      responseObserver.onNext(QueryWriteStatusResponse.newBuilder()
          .setCommittedSize(write.getCommittedSize())
          .setComplete(write.getComplete())
          .build());
      responseObserver.onCompleted();
    } else {
      responseObserver.onError(Status.NOT_FOUND.asException());
    }
  }

  private static final int BLOB_BUFFER_SIZE = 64 * 1024;

  private final class BlobWrite implements Write {
    private final String resourceName;
    private final long size;
    private final RingBufferInputStream buffer;
    private final Thread putThread;
    private long committedSize = 0;
    private boolean complete = false;
    private Throwable error = null;

    BlobWrite(String resourceName) throws InvalidResourceNameException {
      Digest digest = parseUploadBlobDigest(resourceName);
      this.resourceName = resourceName;
      this.size = digest.getSizeBytes();
      buffer = new RingBufferInputStream((int) Math.min(size, BLOB_BUFFER_SIZE));
      putThread = new Thread(() -> {
        try {
          simpleBlobStore.put(digest.getHash(), size, buffer);
        } catch (IOException e) {
          buffer.shutdown();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      putThread.start();
    }

    private void validateRequest(WriteRequest request) {
      String requestResourceName = request.getResourceName();
      if (!requestResourceName.isEmpty() && !resourceName.equals(requestResourceName)) {
        logger.warning(
            String.format(
                "ByteStreamServer:write:%s: resource name %s does not match first request",
                resourceName,
                requestResourceName));
        throw new IllegalArgumentException(
            String.format(
                "Previous resource name changed while handling request. %s -> %s",
                resourceName,
                requestResourceName));
      }
      if (complete) {
        logger.warning(
            String.format(
                "ByteStreamServer:write:%s: write received after finish_write specified",
                resourceName));
        throw new IllegalArgumentException(
            String.format(
                "request sent after finish_write request"));
      }
      long committedSize = getCommittedSize();
      if (request.getWriteOffset() != committedSize) {
        logger.warning(
            String.format(
                "ByteStreamServer:write:%s: offset %d != committed_size %d",
                resourceName,
                request.getWriteOffset(),
                getCommittedSize()));
        throw new IllegalArgumentException("Write offset invalid: " + request.getWriteOffset());
      }
      long sizeAfterWrite = committedSize + request.getData().size();
      if (request.getFinishWrite() && sizeAfterWrite != size) {
        logger.warning(
            String.format(
                "ByteStreamServer:write:%s: finish_write request of size %d for write size %d != expected %d",
                resourceName,
                request.getData().size(),
                sizeAfterWrite,
                size));
        throw new IllegalArgumentException("Write size invalid: " + sizeAfterWrite);
      }
    }

    @Override
    public void onNext(WriteRequest request) {
      boolean shutdownBuffer = true;
      try {
        validateRequest(request);
        ByteString data = request.getData();
        buffer.write(data.toByteArray());
        committedSize += data.size();
        shutdownBuffer = false;
        if (request.getFinishWrite()) {
          complete = true;
        }
      } catch (InterruptedException e) {
        // prevent buffer mitigation
        shutdownBuffer = false;
        Thread.currentThread().interrupt();
      } finally {
        if (shutdownBuffer) {
          buffer.shutdown();
        }
      }
    }

    @Override
    public void onError(Throwable t) {
      buffer.shutdown();
      try {
        putThread.join();
        error = t;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public void onCompleted() {
      try {
        putThread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public long getCommittedSize() {
      return committedSize;
    }

    @Override
    public boolean getComplete() {
      return complete;
    }
  }

  private final class WriteStreamObserver implements StreamObserver<WriteRequest> {
    private final StreamObserver<WriteResponse> responseObserver;
    private Throwable error = null;
    private Write write = null;

    WriteStreamObserver(StreamObserver<WriteResponse> responseObserver) {
      this.responseObserver = responseObserver;
    }

    private Write createWrite(String resourceName) throws InvalidResourceNameException {
      ResourceOperation resourceOperation = detectResourceOperation(resourceName);
      switch (resourceOperation) {
      case UploadBlob:
        return new BlobWrite(resourceName);
      case OperationStream:
      default:
        throw Status.INVALID_ARGUMENT.asRuntimeException();
      }
    }

    private Write getOrCreateWrite(String resourceName) throws InvalidResourceNameException {
      Write write = writes.get(resourceName);
      if (write == null) {
        write = createWrite(resourceName);
        writes.put(resourceName, write);
      }
      return write;
    }

    private void writeOnNext(WriteRequest request) throws InvalidResourceNameException {
      if (write == null) {
        write = getOrCreateWrite(request.getResourceName());
      }
      write.onNext(request);
    }

    @Override
    public void onNext(WriteRequest request) {
      checkState(
          request.getFinishWrite() || request.getData().size() != 0,
          String.format(
              "write onNext supplied with empty WriteRequest for %s at %d",
              request.getResourceName(),
              request.getWriteOffset()));
      if (request.getData().size() != 0) {
        try {
          writeOnNext(request);
        } catch (Throwable t) {
          onError(t);
        }
      }
    }

    @Override
    public void onError(Throwable t) {
      error = t;
      responseObserver.onError(t);
    }

    @Override
    public void onCompleted() {
      if (write != null) {
        write.onCompleted();
      }
      responseObserver.onCompleted();
    }
  }

  @Override
  public StreamObserver<WriteRequest> write(
      StreamObserver<WriteResponse> responseObserver) {
    return new WriteStreamObserver(responseObserver);
  }
}
