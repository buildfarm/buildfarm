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
import static build.buildfarm.common.grpc.Retrier.DEFAULT_IS_RETRIABLE;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.grpc.Context.currentContextExecutor;
import static io.grpc.Status.INVALID_ARGUMENT;
import static io.grpc.Status.NOT_FOUND;
import static io.grpc.Status.OUT_OF_RANGE;
import static java.lang.String.format;
import static java.util.logging.Level.SEVERE;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.UrlPath.InvalidResourceNameException;
import build.buildfarm.common.Write;
import build.buildfarm.instance.Instance;
import com.google.bytestream.ByteStreamGrpc.ByteStreamImplBase;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusResponse;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.Context.CancellableContext;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.NoSuchFileException;
import java.util.UUID;
import java.util.logging.Logger;

public class ByteStreamService extends ByteStreamImplBase {
  private static final Logger logger = Logger.getLogger(ByteStreamService.class.getName());

  private static int CHUNK_SIZE = 64 * 1024;

  private final Instances instances;

  public ByteStreamService(Instances instances) {
    this.instances = instances;
  }

  void readFrom(
      InputStream in,
      long limit,
      StreamObserver<ReadResponse> responseObserver)
      throws IOException {
    byte buf[] = new byte[CHUNK_SIZE];
    boolean unlimited = limit == 0;
    long remaining = limit;
    boolean complete = false;
    while (!complete) {
      int readBytes = in.read(buf);
      if (readBytes == -1) {
        if (!unlimited) {
          responseObserver.onError(OUT_OF_RANGE.asException());
          remaining = -1;
        }
        complete = true;
      } else if (readBytes > 0) {
        if (readBytes > remaining) {
          logger.warning(format("read %d bytes, expected %d", readBytes, remaining));
          readBytes = (int) remaining;
        }
        responseObserver.onNext(ReadResponse.newBuilder()
            .setData(ByteString.copyFrom(buf, 0, readBytes))
            .build());
        remaining -= readBytes;
        complete = remaining == 0;
      }
    }
    if (remaining == 0) {
      responseObserver.onCompleted();
    }
  }

  void readLimitedBlob(
      Instance instance,
      Digest digest,
      long offset,
      long limit,
      StreamObserver<ReadResponse> responseObserver) {
    try (InputStream in = instance.newBlobInput(digest, offset)) {
      readFrom(in, limit, responseObserver);
    } catch (NoSuchFileException e) {
      responseObserver.onError(NOT_FOUND.asException());
    } catch (IOException e) {
      responseObserver.onError(Status.fromThrowable(e).asException());
    }
  }

  void readBlob(
      Instance instance,
      Digest digest,
      long offset,
      long limit,
      StreamObserver<ReadResponse> responseObserver) {
    if (offset == digest.getSizeBytes()) {
      responseObserver.onCompleted();
    } else if (offset > digest.getSizeBytes()) {
      responseObserver.onError(OUT_OF_RANGE.asException());
    } else {
      long available = digest.getSizeBytes() - offset;
      if (limit == 0) {
        limit = available;
      } else {
        limit = Math.min(available, limit);
      }
      readLimitedBlob(instance, digest, offset, limit, responseObserver);
    }
  }

  void readOperationStream(
      Instance instance,
      String resourceName,
      long offset,
      long limit,
      StreamObserver<ReadResponse> responseObserver) {
    try (InputStream in = instance.newOperationStreamInput(resourceName, offset)) {
      readFrom(in, limit, responseObserver);
    } catch (NoSuchFileException e) {
      responseObserver.onError(NOT_FOUND.asException());
    } catch (IOException e) {
      responseObserver.onError(Status.fromThrowable(e).asException());
    }
  }

  void maybeInstanceRead(
      String resourceName,
      long offset,
      long limit,
      StreamObserver<ReadResponse> responseObserver)
      throws InstanceNotFoundException, InvalidResourceNameException {
    switch (detectResourceOperation(resourceName)) {
      case Blob:
        readLimitedBlob(
            instances.getFromBlob(resourceName),
            parseBlobDigest(resourceName),
            offset,
            limit,
            responseObserver);
        break;
      case OperationStream:
        readOperationStream(
            instances.getFromOperationStream(resourceName),
            resourceName,
            offset,
            limit,
            responseObserver);
        break;
      case UploadBlob:
      default:
        responseObserver.onError(INVALID_ARGUMENT.asException());
        break;
    }
  }

  @Override
  public void read(
      ReadRequest request,
      StreamObserver<ReadResponse> responseObserver) {
    String resourceName = request.getResourceName();
    long offset = request.getReadOffset(), limit = request.getReadLimit();
    logger.fine(
        format(
            "read resourceName=%s offset=%d limit=%d",
            resourceName,
            offset,
            limit));

    try {
      maybeInstanceRead(resourceName, offset, limit, responseObserver);
    } catch (InstanceNotFoundException e) {
      responseObserver.onError(BuildFarmInstances.toStatusException(e));
    } catch (InvalidResourceNameException e) {
      responseObserver.onError(INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
    }
  }

  @Override
  public void queryWriteStatus(
      QueryWriteStatusRequest request,
      StreamObserver<QueryWriteStatusResponse> responseObserver) {
    responseObserver.onError(Status.UNIMPLEMENTED.asException());
  }

  long getBlobCommittedSize(Instance instance, Digest digest) {
    return instance.containsBlob(digest) ? digest.getSizeBytes() : 0;
  }

  long getUploadBlobCommittedSize(Instance instance, Digest digest, UUID uuid) {
    return instance.getBlobWrite(digest, uuid).getCommittedSize();
  }

  long getOperationStreamCommittedSize(Instance instance, String resourceName) {
    return instance.getOperationStreamWrite(resourceName).getCommittedSize();
  }

  long getCommittedSize(String resourceName) throws InstanceNotFoundException, InvalidResourceNameException {
    switch (detectResourceOperation(resourceName)) {
      case Blob:
        return getBlobCommittedSize(
            instances.getFromBlob(resourceName),
            parseBlobDigest(resourceName));
      case UploadBlob:
        return getUploadBlobCommittedSize(
            instances.getFromUploadBlob(resourceName),
            parseUploadBlobDigest(resourceName),
            parseUploadBlobUUID(resourceName));
      case OperationStream:
        return getOperationStreamCommittedSize(
            instances.getFromOperationStream(resourceName),
            resourceName);
      default:
        throw new IllegalArgumentException();
    }
  }

  @Override
  public StreamObserver<WriteRequest> write(
      StreamObserver<WriteResponse> responseObserver) {
    CancellableContext withCancellation = Context.current().withCancellation();
    return new StreamObserver<WriteRequest>() {
      boolean initialized = false;
      String name = null;
      OutputStream out = null;
      Instance instance = null;

      @Override
      public void onNext(WriteRequest request) {
        if (initialized) {
          handleRequest(request);
        } else {
          initialize(request);
        }
      }

      Write getBlobWrite(
          Instance instance,
          Digest digest,
          UUID uuid) throws IOException {
        return instance.getBlobWrite(digest, uuid);
      }

      Write getOperationStreamWrite(
          Instance instance,
          String resourceName) throws IOException {
        return instance.getOperationStreamWrite(name);
      }

      Write getWrite(String resourceName) throws IOException, InstanceNotFoundException, InvalidResourceNameException {
        switch (detectResourceOperation(resourceName)) {
          case UploadBlob:
            return getBlobWrite(
                instances.getFromUploadBlob(resourceName),
                parseUploadBlobDigest(resourceName),
                parseUploadBlobUUID(resourceName));
          case OperationStream:
            return getOperationStreamWrite(
                instances.getFromOperationStream(resourceName),
                resourceName);
          case Blob:
          default:
            throw new IOException(INVALID_ARGUMENT
                .withDescription("unknown resource operation for " + resourceName)
                .asException());
        }
      }

      void initialize(WriteRequest request) {
        String resourceName = request.getResourceName();
        if (resourceName.isEmpty()) {
          responseObserver.onError(INVALID_ARGUMENT
              .withDescription("resource_name is empty")
              .asException());
        } else {
          name = resourceName;
          try {
            Write write = getWrite(resourceName);
            write.addListener(
                () -> {
                  if (!Context.current().isCancelled()) {
                    try {
                      responseObserver.onNext(WriteResponse.newBuilder()
                          .setCommittedSize(write.getCommittedSize())
                          .build());
                    } catch (Throwable t) {
                      logger.log(SEVERE, "error delivering committedSize to " + resourceName, t);
                    }
                  }
                },
                withCancellation.fixedContextExecutor(directExecutor()));
            out = write.getOutput();
            initialized = true;
            handleRequest(request);
          } catch (InstanceNotFoundException e) {
            responseObserver.onError(BuildFarmInstances.toStatusException(e));
          } catch (IOException|InvalidResourceNameException e) {
            responseObserver.onError(Status.fromThrowable(e).asException());
          }
        }
      }

      void handleRequest(WriteRequest request) {
        String resourceName = request.getResourceName();
        if (resourceName.isEmpty()) {
          resourceName = name;
        }
        handleWrite(
            resourceName,
            request.getWriteOffset(),
            request.getData(),
            request.getFinishWrite());
      }

      void handleWrite(
          String resourceName,
          long offset,
          ByteString data,
          boolean finishWrite) {
        try {
          long committedSize = getCommittedSize(name);
          if (offset != 0 && offset != committedSize) {
            responseObserver.onError(INVALID_ARGUMENT
                .withDescription(format("offset %d does not match committed size %d", offset, committedSize))
                .asException());
          } else if (!resourceName.equals(name)) {
            responseObserver.onError(INVALID_ARGUMENT
                .withDescription(format("request resource_name %s does not match previous resource_name %s", resourceName, name))
                .asException());
          } else {
            if (offset == 0 && offset != committedSize) {
              // reset the write
              try {
                out.close();
              } catch (IOException e) {
                // ignore, cancelling
              }
              out = getWrite(resourceName).getOutput();
              checkState(getCommittedSize(resourceName) == 0);
            }
            if (!data.isEmpty()) {
              writeData(data);
            }
            if (finishWrite) {
              out.close();
            }
          }
        } catch (InstanceNotFoundException e) {
          responseObserver.onError(BuildFarmInstances.toStatusException(e));
        } catch (IOException|InvalidResourceNameException e) {
          responseObserver.onError(Status.fromThrowable(e).asException());
        }
      }

      void writeData(ByteString data) {
        try {
          data.writeTo(out);
        } catch (IOException e) {
          responseObserver.onError(Status.fromThrowable(e).asException());
        }
      }

      @Override
      public void onError(Throwable t) {
        if (initialized
            && !DEFAULT_IS_RETRIABLE.apply(Status.fromThrowable(t))) {
          try {
            out.close();
          } catch (IOException e) {
            logger.log(SEVERE, "error closing output stream after error", e);
          }
        } else {
          withCancellation.cancel(t);
        }
      }

      @Override
      public void onCompleted() {
        responseObserver.onCompleted();
      }
    };
  }
}
