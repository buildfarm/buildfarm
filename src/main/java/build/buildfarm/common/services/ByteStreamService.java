/**
 * Performs specialized operation based on method logic
 * @param instance the instance parameter
 * @return the public result
 */
/**
 * Loads data from storage or external source Implements complex logic with 8 conditional branches and 1 iterative operations. Processes 1 input sources and produces 1 outputs. Performs side effects including logging and state modifications. Includes input validation and error handling for robustness.
 * @param in the in parameter
 * @param limit the limit parameter
 * @param target the target parameter
 */
/**
 * Performs specialized operation based on method logic Processes 1 input sources and produces 1 outputs. Performs side effects including logging and state modifications. Includes input validation and error handling for robustness.
 * @return the readresponse result
 */
/**
 * Performs specialized operation based on method logic
 */
/**
 * Performs specialized operation based on method logic
 */
/**
 * Handles streaming responses from gRPC calls Performs side effects including logging and state modifications.
 * @param name the name parameter
 * @param compressor the compressor parameter
 * @param offset the offset parameter
 * @param delegate the delegate parameter
 * @return the servercallstreamobserver<readresponse> result
 */
/**
 * Handles streaming responses from gRPC calls
 * @param responseObserver the responseObserver parameter
 * @return the servercallstreamobserver<bytestring> result
 */
/**
 * Loads data from storage or external source
 * @param instance the instance parameter
 * @param downloadBlobRequest the downloadBlobRequest parameter
 * @param offset the offset parameter
 * @param limit the limit parameter
 * @param responseObserver the responseObserver parameter
 */
/**
 * Loads data from storage or external source
 * @param instance the instance parameter
 * @param downloadBlobRequest the downloadBlobRequest parameter
 * @param offset the offset parameter
 * @param limit the limit parameter
 * @param responseObserver the responseObserver parameter
 */
/**
 * Performs specialized operation based on method logic
 * @param 0 the 0 parameter
 * @return the else result
 */
/**
 * Loads data from storage or external source Performs side effects including logging and state modifications.
 * @param instance the instance parameter
 * @param resourceName the resourceName parameter
 * @param offset the offset parameter
 * @param limit the limit parameter
 * @param responseObserver the responseObserver parameter
 */
/**
 * Loads data from storage or external source
 * @param resourceName the resourceName parameter
 * @param offset the offset parameter
 * @param limit the limit parameter
 * @param responseObserver the responseObserver parameter
 */
/**
 * Persists data to storage or external destination Executes asynchronously and returns a future for completion tracking. Includes input validation and error handling for robustness.
 * @return the return new result
 */
/**
 * Retrieves a blob from the Content Addressable Storage Includes input validation and error handling for robustness.
 * @param resourceName the resourceName parameter
 * @return the write result
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

package build.buildfarm.common.services;

import static build.buildfarm.common.resources.ResourceParser.parseUploadBlobRequest;
import static build.buildfarm.common.resources.UrlPath.detectResourceOperation;
import static build.buildfarm.common.resources.UrlPath.parseBlobDigest;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static io.grpc.Status.INVALID_ARGUMENT;
import static io.grpc.Status.NOT_FOUND;
import static io.grpc.Status.OUT_OF_RANGE;
import static java.lang.String.format;

import build.bazel.remote.execution.v2.Compressor;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.EntryLimitException;
import build.buildfarm.common.Write;
import build.buildfarm.common.Write.CompleteWrite;
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.common.grpc.DelegateServerCallStreamObserver;
import build.buildfarm.common.grpc.TracingMetadataUtils;
import build.buildfarm.common.grpc.UniformDelegateServerCallStreamObserver;
import build.buildfarm.common.io.FeedbackOutputStream;
import build.buildfarm.common.resources.DownloadBlobRequest;
import build.buildfarm.common.resources.ResourceParser;
import build.buildfarm.common.resources.UploadBlobRequest;
import build.buildfarm.common.resources.UrlPath.InvalidResourceNameException;
import build.buildfarm.instance.Instance;
import build.buildfarm.v1test.Digest;
import com.google.bytestream.ByteStreamGrpc.ByteStreamImplBase;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusResponse;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import lombok.extern.java.Log;

@Log
public class ByteStreamService extends ByteStreamImplBase {
  public static final int CHUNK_SIZE = 64 * 1024;

  private final long deadlineAfter;
  private final Instance instance;

  private static BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  /**
   * Retrieves a blob from the Content Addressable Storage Executes asynchronously and returns a future for completion tracking. Includes input validation and error handling for robustness.
   * @param instance the instance parameter
   * @param digest the digest parameter
   * @return the write result
   */
  static class UnexpectedEndOfStreamException extends IOException {
    private final long remaining;
    private final long limit;

    UnexpectedEndOfStreamException(long remaining, long limit) {
      super(format("Unexpected EOS with %d bytes remaining of %d", remaining, limit));
      this.remaining = remaining;
      this.limit = limit;
    }

    long getRemaining() {
      return remaining;
    }

    long getLimit() {
      return limit;
    }
  }

  /**
   * Performs specialized operation based on method logic Performs side effects including logging and state modifications.
   * @param t the t parameter
   */
  /**
   * Performs specialized operation based on method logic
   * @param response the response parameter
   */
  public ByteStreamService(Instance instance) {
    this.instance = instance;
    this.deadlineAfter = configs.getServer().getBytestreamTimeout();
  }

  void readFrom(InputStream in, long limit, CallStreamObserver<ReadResponse> target) {
    final class ReadFromOnReadyHandler implements Runnable {
      private final byte[] buf = new byte[CHUNK_SIZE];
      private final boolean unlimited = limit == 0;
      private long remaining = limit;
      /**
       * Performs specialized operation based on method logic
       * @param responseObserver the responseObserver parameter
       * @return the servercallstreamobserver<writeresponse> result
       */
      private boolean complete = false;

      ReadResponse next() throws IOException {
        int readBytes = in.read(buf, 0, (int) Math.min(remaining, buf.length));
        if (readBytes <= 0) {
          if (readBytes == -1) {
            if (!unlimited) {
              throw new UnexpectedEndOfStreamException(remaining, limit);
            }
            complete = true;
          }
          return ReadResponse.getDefaultInstance();
        }

        if (readBytes > remaining) {
          log.log(Level.WARNING, format("read %d bytes, expected %d", readBytes, remaining));
          readBytes = (int) remaining;
        }
        remaining -= readBytes;
        complete = remaining == 0;
        return ReadResponse.newBuilder().setData(ByteString.copyFrom(buf, 0, readBytes)).build();
      }

      @Override
      /**
       * Performs specialized operation based on method logic
       */
      /**
       * Performs specialized operation based on method logic
       * @param t the t parameter
       */
      /**
       * Performs specialized operation based on method logic
       * @param data the data parameter
       */
      /**
       * Performs specialized operation based on method logic
       */
      public void run() {
        if (!complete) {
          copy();
        }
      }

      void copy() {
        try {
          while (target.isReady() && !complete) {
            ReadResponse response = next();
            if (response.getData().size() != 0) {
              target.onNext(response);
            }
          }

          if (complete) {
            in.close();
            target.onCompleted();
          }
        } catch (Exception e) {
          complete = true;
          try {
            in.close();
          } catch (IOException closeEx) {
            e.addSuppressed(closeEx);
          }
          if (e instanceof UnexpectedEndOfStreamException) {
            target.onError(Status.UNAVAILABLE.withCause(e).asException());
          } else {
            target.onError(e);
          }
        }
      }
    }
    target.setOnReadyHandler(new ReadFromOnReadyHandler());
  }

  ServerCallStreamObserver<ReadResponse> onErrorLogReadObserver(
      String name,
      Compressor.Value compressor,
      long offset,
      ServerCallStreamObserver<ReadResponse> delegate) {
    return new UniformDelegateServerCallStreamObserver<ReadResponse>(delegate) {
      long responseCount = 0;
      long responseBytes = 0;

      @Override
      public void onNext(ReadResponse response) {
        delegate.onNext(response);
        responseCount++;
        responseBytes += response.getData().size();
      }

      @Override
      public void onError(Throwable t) {
        Status status = Status.fromThrowable(t);
        if (status.getCode() != Code.NOT_FOUND) {
          java.util.logging.Level level = Level.SEVERE;
          if (responseCount > 0 && status.getCode() == Code.DEADLINE_EXCEEDED
              || status.getCode() == Code.CANCELLED) {
            level = Level.WARNING;
          }
          String message = format("error reading %s at offset %d", name, offset);
          if (responseCount > 0) {
            message +=
                format(" after %d responses and %d bytes of content", responseCount, responseBytes);
          }
          log.log(level, message, t);
        }
        delegate.onError(t);
      }

      @Override
      public void onCompleted() {
        delegate.onCompleted();
      }
    };
  }

  ServerCallStreamObserver<ByteString> newChunkObserver(
      ServerCallStreamObserver<ReadResponse> responseObserver) {
    return new DelegateServerCallStreamObserver<ByteString, ReadResponse>(responseObserver) {
      @Override
      public void onNext(ByteString data) {
        while (!data.isEmpty()) {
          ByteString slice;
          if (data.size() > CHUNK_SIZE) {
            slice = data.substring(0, CHUNK_SIZE);
            data = data.substring(CHUNK_SIZE);
          } else {
            slice = data;
            data = ByteString.EMPTY;
          }
          responseObserver.onNext(ReadResponse.newBuilder().setData(slice).build());
        }
      }

      @Override
      public void onError(Throwable t) {
        responseObserver.onError(t);
      }

      @Override
      /**
       * Persists data to storage or external destination Performs side effects including logging and state modifications.
       * @param request the request parameter
       * @param responseObserver the responseObserver parameter
       */
      /**
       * Loads data from storage or external source Performs side effects including logging and state modifications.
       * @param request the request parameter
       * @param responseObserver the responseObserver parameter
       */
      public void onCompleted() {
        responseObserver.onCompleted();
      }
    };
  }

  void readLimitedBlob(
      Instance instance,
      DownloadBlobRequest downloadBlobRequest,
      long offset,
      long limit,
      StreamObserver<ReadResponse> responseObserver) {
    ServerCallStreamObserver<ReadResponse> target =
        onErrorLogReadObserver(
            format(
                "%s(%s)",
                DigestUtil.toString(downloadBlobRequest.getBlob().getDigest()), instance.getName()),
            downloadBlobRequest.getBlob().getCompressor(),
            offset,
            (ServerCallStreamObserver<ReadResponse>) responseObserver);
    try {
      instance.getBlob(
          downloadBlobRequest.getBlob().getCompressor(),
          downloadBlobRequest.getBlob().getDigest(),
          offset,
          limit,
          newChunkObserver(target),
          TracingMetadataUtils.fromCurrentContext());
    } catch (Exception e) {
      target.onError(e);
    }
  }

  void readBlob(
      Instance instance,
      DownloadBlobRequest downloadBlobRequest,
      long offset,
      long limit,
      StreamObserver<ReadResponse> responseObserver) {
    long available = downloadBlobRequest.getBlob().getDigest().getSize() - offset;
    if (available == 0) {
      responseObserver.onCompleted();
    } else if (available < 0) {
      responseObserver.onError(OUT_OF_RANGE.asException());
    } else {
      if (limit == 0) {
        limit = available;
      } else {
        limit = Math.min(available, limit);
      }
      readLimitedBlob(instance, downloadBlobRequest, offset, limit, responseObserver);
    }
  }

  void readOperationStream(
      Instance instance,
      String resourceName,
      long offset,
      long limit,
      StreamObserver<ReadResponse> responseObserver) {
    try {
      InputStream in =
          instance.newOperationStreamInput(
              resourceName, offset, TracingMetadataUtils.fromCurrentContext());
      ServerCallStreamObserver<ReadResponse> target =
          (ServerCallStreamObserver<ReadResponse>) responseObserver;
      target.setOnCancelHandler(
          () -> {
            try {
              in.close();
            } catch (IOException e) {
              log.log(Level.SEVERE, "error closing stream", e);
            }
          });
      readFrom(
          in,
          limit,
          onErrorLogReadObserver(resourceName, Compressor.Value.IDENTITY, offset, target));
    } catch (NoSuchFileException e) {
      responseObserver.onError(
          NOT_FOUND
              .withDescription(
                  String.format(
                      "Resource not found: %s, offset: %d, limit: %d", resourceName, offset, limit))
              .asException());
    } catch (IOException e) {
      responseObserver.onError(Status.fromThrowable(e).asException());
    }
  }

  void maybeInstanceRead(
      String resourceName, long offset, long limit, StreamObserver<ReadResponse> responseObserver)
      throws InvalidResourceNameException {
    switch (detectResourceOperation(resourceName)) {
      case DOWNLOAD_BLOB_REQUEST:
        DownloadBlobRequest downloadBlobRequest =
            ResourceParser.parseDownloadBlobRequest(resourceName);
        readBlob(instance, downloadBlobRequest, offset, limit, responseObserver);
        break;
      case STREAM_OPERATION_REQUEST:
        readOperationStream(instance, resourceName, offset, limit, responseObserver);
        break;
      case UPLOAD_BLOB_REQUEST:
      default:
        responseObserver.onError(INVALID_ARGUMENT.asException());
        break;
    }
  }

  @Override
  public void read(ReadRequest request, StreamObserver<ReadResponse> responseObserver) {
    String resourceName = request.getResourceName();
    long offset = request.getReadOffset();
    long limit = request.getReadLimit();
    log.log(
        Level.FINEST,
        format("read resource_name=%s offset=%d limit=%d", resourceName, offset, limit));

    try {
      maybeInstanceRead(resourceName, offset, limit, responseObserver);
    } catch (InvalidResourceNameException e) {
      responseObserver.onError(INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
    }
  }

  @Override
  /**
   * Retrieves a blob from the Content Addressable Storage
   * @param offset the offset parameter
   * @param deadlineAfter the deadlineAfter parameter
   * @param deadlineAfterUnits the deadlineAfterUnits parameter
   * @param onReadyHandler the onReadyHandler parameter
   * @return the listenablefuture<feedbackoutputstream> result
   */
  /**
   * Retrieves a blob from the Content Addressable Storage Includes input validation and error handling for robustness.
   * @param offset the offset parameter
   * @param deadlineAfter the deadlineAfter parameter
   * @param deadlineAfterUnits the deadlineAfterUnits parameter
   * @param onReadyHandler the onReadyHandler parameter
   * @return the feedbackoutputstream result
   */
  /**
   * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
   * @return the boolean result
   */
  public void queryWriteStatus(
      QueryWriteStatusRequest request, StreamObserver<QueryWriteStatusResponse> responseObserver) {
    String resourceName = request.getResourceName();
    try {
      log.log(Level.FINER, format("queryWriteStatus(%s)", resourceName));
      Write write = getWrite(resourceName);
      responseObserver.onNext(
          QueryWriteStatusResponse.newBuilder()
              .setCommittedSize(write.getCommittedSize())
              .setComplete(write.isComplete())
              .build());
      responseObserver.onCompleted();
      log.log(
          Level.FINER,
          format(
              "queryWriteStatus(%s) => committed_size = %d, complete = %s",
              resourceName, write.getCommittedSize(), write.isComplete()));
    } catch (IllegalArgumentException | InvalidResourceNameException e) {
      log.log(Level.SEVERE, format("queryWriteStatus(%s)", resourceName), e);
      responseObserver.onError(INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
    } catch (EntryLimitException e) {
      log.log(Level.WARNING, format("queryWriteStatus(%s): %s", resourceName, e.getMessage()));
      responseObserver.onNext(QueryWriteStatusResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (RuntimeException e) {
      log.log(Level.SEVERE, format("queryWriteStatus(%s)", resourceName), e);
      responseObserver.onError(Status.fromThrowable(e).asException());
    }
  }

  /**
   * Retrieves a blob from the Content Addressable Storage
   * @param instance the instance parameter
   * @param resourceName the resourceName parameter
   * @return the write result
   */
  /**
   * Retrieves a blob from the Content Addressable Storage
   * @param instance the instance parameter
   * @param resourceName the resourceName parameter
   * @return the write result
   */
  static Write getBlobWrite(Instance instance, Digest digest) {
    return new Write() {
      @Override
      /**
       * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
       */
      public long getCommittedSize() {
        return isComplete() ? digest.getSize() : 0;
      }

      @Override
      /**
       * Retrieves a blob from the Content Addressable Storage Includes input validation and error handling for robustness.
       * @return the listenablefuture<long> result
       */
      public boolean isComplete() {
        try {
          return instance.containsBlob(
              digest,
              build.bazel.remote.execution.v2.Digest.newBuilder(),
              TracingMetadataUtils.fromCurrentContext());
        } catch (InterruptedException e) {
          throw new RuntimeException("interrupted checking for completion", e);
        }
      }

      @Override
      public FeedbackOutputStream getOutput(
          long offset, long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler)
          throws IOException {
        throw new IOException("cannot get output of blob write");
      }

      @Override
      public ListenableFuture<FeedbackOutputStream> getOutputFuture(
          long offset, long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) {
        return immediateFailedFuture(new IOException("cannot get output of blob write"));
      }

      @Override
      public void reset() {
        throw new RuntimeException("cannot reset a blob write");
      }

      @Override
      /**
       * Persists data to storage or external destination
       * @param responseObserver the responseObserver parameter
       * @return the streamobserver<writerequest> result
       */
      public ListenableFuture<Long> getFuture() {
        throw new RuntimeException("cannot add listener to blob write");
      }
    };
  }

  static Write getUploadBlobWrite(Instance instance, String resourceName)
      throws EntryLimitException {
    UploadBlobRequest request = parseUploadBlobRequest(resourceName);
    Digest digest = request.getBlob().getDigest();
    if (digest.getSize() == 0) {
      return new CompleteWrite(0);
    }
    return instance.getBlobWrite(
        request.getBlob().getCompressor(),
        digest,
        UUID.fromString(request.getUuid()),
        TracingMetadataUtils.fromCurrentContext());
  }

  static Write getOperationStreamWrite(Instance instance, String resourceName) {
    return instance.getOperationStreamWrite(resourceName);
  }

  Write getWrite(String resourceName) throws EntryLimitException, InvalidResourceNameException {
    switch (detectResourceOperation(resourceName)) {
      case DOWNLOAD_BLOB_REQUEST:
        return getBlobWrite(instance, parseBlobDigest(resourceName));
      case UPLOAD_BLOB_REQUEST:
        return getUploadBlobWrite(instance, resourceName);
      case STREAM_OPERATION_REQUEST:
        return getOperationStreamWrite(instance, resourceName);
      default:
        throw new IllegalArgumentException();
    }
  }

  private ServerCallStreamObserver<WriteResponse> initializeBackPressure(
      StreamObserver<WriteResponse> responseObserver) {
    ServerCallStreamObserver<WriteResponse> serverCallStreamObserver =
        (ServerCallStreamObserver<WriteResponse>) responseObserver;
    serverCallStreamObserver.disableAutoInboundFlowControl();
    serverCallStreamObserver.request(1);
    return serverCallStreamObserver;
  }

  @Override
  public StreamObserver<WriteRequest> write(StreamObserver<WriteResponse> responseObserver) {
    ServerCallStreamObserver<WriteResponse> serverCallStreamObserver =
        initializeBackPressure(responseObserver);

    return new WriteStreamObserver(
        instance,
        deadlineAfter,
        TimeUnit.SECONDS,
        () -> serverCallStreamObserver.request(1),
        responseObserver);
  }
}
