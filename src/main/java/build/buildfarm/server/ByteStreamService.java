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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.lang.String.format;
import static java.util.logging.Level.SEVERE;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.UrlPath;
import build.buildfarm.common.UrlPath.ResourceOperation;
import build.buildfarm.common.UrlPath.InvalidResourceNameException;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.Instance.ChunkObserver;
import build.bazel.remote.execution.v2.Digest;
import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusResponse;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import javax.annotation.Nullable;

public class ByteStreamService extends ByteStreamGrpc.ByteStreamImplBase {
  private static final Logger logger = Logger.getLogger(ByteStreamService.class.getName());

  private static final long DEFAULT_CHUNK_SIZE = 1024 * 1024;

  private final Map<String, ByteString> active_write_requests = new HashMap<>();
  private final Instances instances;

  public ByteStreamService(Instances instances) {
    this.instances = instances;
  }

  private void readBlob(
      ReadRequest request,
      StreamObserver<ReadResponse> responseObserver) throws IOException, InterruptedException {
    String resourceName = request.getResourceName();
    try {
      Instance instance = instances.getFromBlob(resourceName);

      Digest digest = UrlPath.parseBlobDigest(resourceName);

      instance.getBlob(
          digest,
          request.getReadOffset(),
          request.getReadLimit(),
          new StreamObserver<ByteString>() {
            @Override
            public void onNext(ByteString nextChunk) {
              ByteString remaining = nextChunk;
              while (remaining.size() >= DEFAULT_CHUNK_SIZE) {
                ByteString chunk = remaining.substring(0, (int) DEFAULT_CHUNK_SIZE);
                remaining = remaining.substring(chunk.size());
                responseObserver.onNext(ReadResponse.newBuilder()
                    .setData(chunk)
                    .build());
              }
              if (!remaining.isEmpty()) {
                responseObserver.onNext(ReadResponse.newBuilder()
                    .setData(remaining)
                    .build());
              }
            }

            @Override
            public void onError(Throwable t) {
              responseObserver.onError(Status.fromThrowable(t).asException());
            }

            @Override
            public void onCompleted() {
              responseObserver.onCompleted();
            }
          });
    } catch (InstanceNotFoundException e) {
      responseObserver.onError(BuildFarmInstances.toStatusException(e));
    } catch (InvalidResourceNameException e) {
      String description = e.getLocalizedMessage();
      responseObserver.onError(Status.INVALID_ARGUMENT
          .withDescription(description)
          .asException());
    }
  }

  private void readOperationStream(
      ReadRequest request,
      StreamObserver<ReadResponse> responseObserver)
      throws IOException, InvalidResourceNameException {
    String resourceName = request.getResourceName();

    Instance instance;
    try {
      instance = instances.getFromBlob(resourceName);
    } catch (InstanceNotFoundException e) {
      responseObserver.onError(BuildFarmInstances.toStatusException(e));
      return;
    }

    String operationStream = UrlPath.parseOperationStream(resourceName);

    long readLimit = request.getReadLimit();
    InputStream input = instance.newStreamInput(operationStream, request.getReadOffset());
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
      responseObserver.onError(Status.OUT_OF_RANGE.asException());
      return;
    }

    try {
      ResourceOperation resourceOperation = UrlPath.detectResourceOperation(resourceName);
      switch (resourceOperation) {
      case Blob:
        readBlob(request, responseObserver);
        break;
      case OperationStream:
        readOperationStream(request, responseObserver);
        break;
      default:
        logger.severe(format("ByteStreamServer:read %s: unknown resource type", resourceName));
        throw new InvalidResourceNameException(resourceName, "Invalid resource type");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (IOException e) {
      responseObserver.onError(new StatusException(Status.fromThrowable(e)));
    } catch (InvalidResourceNameException e) {
      String description = e.getLocalizedMessage();
      responseObserver.onError(Status.INVALID_ARGUMENT
          .withDescription(description)
          .asException());
    }
  }

  private void queryBlobWriteStatus(
      String resourceName,
      StreamObserver<QueryWriteStatusResponse> responseObserver) {
    try {
      Instance instance = instances.getFromBlob(resourceName);
      queryInstanceBlobWriteStatus(instance, resourceName, responseObserver);
    } catch (InstanceNotFoundException e) {
      responseObserver.onError(BuildFarmInstances.toStatusException(e));
    } catch (InvalidResourceNameException e) {
      String description = e.getLocalizedMessage();
      responseObserver.onError(Status.INVALID_ARGUMENT
          .withDescription(description)
          .asException());
    }
  }

  private void queryInstanceBlobWriteStatus(
      Instance instance,
      String resourceName,
      StreamObserver<QueryWriteStatusResponse> responseObserver)
      throws InvalidResourceNameException {
    Digest digest = UrlPath.parseBlobDigest(resourceName);

    Iterable<Digest> missingDigests;
    try {
      missingDigests = instance.findMissingBlobs(ImmutableList.of(digest), newDirectExecutorService()).get();
    } catch (ExecutionException e) {
      responseObserver.onError(Status.fromThrowable(e).asException());
      return;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
    }

    if (Iterables.isEmpty(missingDigests)) {
      responseObserver.onNext(
          QueryWriteStatusResponse.newBuilder()
              .setCommittedSize(digest.getSizeBytes())
              .setComplete(true)
              .build());
      responseObserver.onCompleted();
    } else {
      responseObserver.onError(Status.NOT_FOUND.asException());
    }
  }

  @Override
  public void queryWriteStatus(
      QueryWriteStatusRequest request,
      StreamObserver<QueryWriteStatusResponse> responseObserver) {
    String resourceName = request.getResourceName();

    try {
      ResourceOperation resourceOperation = UrlPath.detectResourceOperation(resourceName);
      switch (resourceOperation) {
      case UploadBlob:
        responseObserver.onError(Status.UNIMPLEMENTED.asException());
        break;
      case OperationStream:
        responseObserver.onError(Status.UNIMPLEMENTED.asException());
        break;
      case Blob:
        queryBlobWriteStatus(resourceName, responseObserver);
        break;
      default:
        logger.severe("ByteStreamServer:query "
            + resourceName
            + ": unknown resource type");
        throw new InvalidResourceNameException(resourceName, "Invalid resource type");
      }
    } catch (InvalidResourceNameException e) {
      String description = e.getLocalizedMessage();
      responseObserver.onError(Status.INVALID_ARGUMENT
          .withDescription(description)
          .asException());
    }
  }

  ChunkObserver getWriteBlobObserver(String resourceName) throws InstanceNotFoundException, InvalidResourceNameException {
    Instance instance = instances.getFromUploadBlob(resourceName);

    Digest digest = UrlPath.parseUploadBlobDigest(resourceName);
    ChunkObserver chunkObserver = instance.getWriteBlobObserver(digest);
    Futures.addCallback(chunkObserver.getCommittedFuture(), new FutureCallback<Long>() {
      @Override
      public void onSuccess(Long committedSize) {
      }

      @Override
      public void onFailure(Throwable t) {
        logger.log(SEVERE, format("Failed writing blob %s", resourceName), t);
      }
    });
    return chunkObserver;
  }

  ChunkObserver getWriteOperationStreamObserver(String resourceName) throws InstanceNotFoundException, InvalidResourceNameException {
    Instance instance = instances.getFromOperationStream(resourceName);

    String operationStream = UrlPath.parseOperationStream(resourceName);
    return instance.getWriteOperationStreamObserver(operationStream);
  }

  ChunkObserver getChunkObserver(String resourceName) throws InstanceNotFoundException, InvalidResourceNameException {
    ResourceOperation resourceOperation = UrlPath.detectResourceOperation(resourceName);
    switch (resourceOperation) {
    case UploadBlob:
      return getWriteBlobObserver(resourceName);
    case OperationStream:
      return getWriteOperationStreamObserver(resourceName);
    default:
      logger.severe("ByteStreamServer:write "
          + resourceName
          + ": unknown resource type");
      throw new InvalidResourceNameException(resourceName, "Invalid resource type");
    }
  }

  @Override
  public StreamObserver<WriteRequest> write(
      final StreamObserver<WriteResponse> responseObserver) {
    return new StreamObserver<WriteRequest>() {
      String resourceName;
      ChunkObserver chunkObserver = null;

      @Override
      public void onCompleted() {
        // I don't expect this to be true, but hopefully it will prevent us from missing a close
        if (chunkObserver != null) {
          chunkObserver.onCompleted();
          chunkObserver = null;
        }
      }

      @Override
      public void onError(Throwable t) {
        if (chunkObserver != null) {
          chunkObserver.onError(t);
          chunkObserver = null;
        }
      }

      void validateRequest(WriteRequest request) {
        String requestResourceName = request.getResourceName();
        if (!requestResourceName.isEmpty() && !resourceName.equals(requestResourceName)) {
          logger.warning(
              format("ByteStreamServer:write:%s: resource name (%s) does not match first request", resourceName, requestResourceName));
          throw new IllegalArgumentException(format("Previous resource name changed while handling request. %s -> %s", resourceName, requestResourceName));
        }
        if (request.getWriteOffset() != chunkObserver.getCommittedSize()) {
          logger.warning(
              format("ByteStreamServer:write:%s: offset(%d) != committed_size(%d)", resourceName, request.getWriteOffset(), chunkObserver.getCommittedSize()));
          throw new IllegalArgumentException("Write offset invalid: " + request.getWriteOffset());
        }

        // finish write, digest compare, etc
      }

      @Override
      public void onNext(WriteRequest request) {
        checkState(
            request.getFinishWrite() || request.getData().size() != 0,
            "write onNext supplied with empty WriteRequest for " + request.getResourceName() + " at " + request.getWriteOffset());
        if (request.getData().size() != 0) {
          try {
            if (chunkObserver == null) {
              resourceName = request.getResourceName();
              if (resourceName.isEmpty()) {
                logger.severe("ByteStreamServer:write: resource name not specified on first write");
                throw new InvalidResourceNameException(resourceName, "Missing resource name in request");
              }
              chunkObserver = getChunkObserver(resourceName);
              Futures.addCallback(chunkObserver.getCommittedFuture(), new FutureCallback<Long>() {
                @Override
                public void onFailure(Throwable t) {
                  logger.log(SEVERE, format("Error During upload of %s", resourceName), t);
                  responseObserver.onError(t);
                }

                @Override
                public void onSuccess(Long committedSize) {
                  responseObserver.onNext(WriteResponse.newBuilder()
                      .setCommittedSize(committedSize)
                      .build());
                  responseObserver.onCompleted();
                }
              });
            }

            if (chunkObserver.getCommittedSize() != 0 && request.getWriteOffset() == 0) {
              chunkObserver.reset();
            }
            validateRequest(request);

            chunkObserver.onNext(request.getData());
          } catch (InstanceNotFoundException|InvalidResourceNameException e) {
            Throwable t = Status.INVALID_ARGUMENT.withDescription(e.getLocalizedMessage()).asException();
            if (chunkObserver != null) {
              chunkObserver.onError(t);
              chunkObserver = null;
            }
            responseObserver.onError(t);
            return;
          }
        }

        if (request.getFinishWrite()) {
          if (chunkObserver != null) {
            chunkObserver.onCompleted();
            chunkObserver = null;
          } else if (request.getData().size() == 0) {
            responseObserver.onNext(WriteResponse.newBuilder()
                .setCommittedSize(0)
                .build());
            responseObserver.onCompleted();
          } else {
            responseObserver.onError(Status.INTERNAL.withDescription("ByteStreamServer:write: resource " + resourceName + " finished without a chunk observer").asException());
          }
        }
      }
    };
  }
}
