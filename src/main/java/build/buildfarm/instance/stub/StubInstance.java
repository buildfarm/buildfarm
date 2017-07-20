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

package build.buildfarm.instance.stub;

import build.buildfarm.common.Digests;
import build.buildfarm.instance.Instance;
import build.buildfarm.v1test.OperationQueueGrpc;
import build.buildfarm.v1test.OperationQueueGrpc.OperationQueueBlockingStub;
import build.buildfarm.v1test.TakeOperationRequest;
import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamBlockingStub;
import com.google.bytestream.ByteStreamGrpc.ByteStreamStub;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.ActionCacheGrpc;
import com.google.devtools.remoteexecution.v1test.ActionCacheGrpc.ActionCacheBlockingStub;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.ContentAddressableStorageGrpc;
import com.google.devtools.remoteexecution.v1test.ContentAddressableStorageGrpc.ContentAddressableStorageBlockingStub;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata;
import com.google.devtools.remoteexecution.v1test.FindMissingBlobsRequest;
import com.google.devtools.remoteexecution.v1test.FindMissingBlobsResponse;
import com.google.devtools.remoteexecution.v1test.GetTreeRequest;
import com.google.devtools.remoteexecution.v1test.GetTreeResponse;
import com.google.devtools.remoteexecution.v1test.Platform;
import com.google.devtools.remoteexecution.v1test.UpdateActionResultRequest;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

public class StubInstance implements Instance {
  private final String name;
  private final Channel channel;

  public StubInstance(String name, Channel channel) {
    this.name = name;
    this.channel = channel;
  }

  private final Supplier<ActionCacheBlockingStub> actionCacheBlockingStub =
      Suppliers.memoize(
          new Supplier<ActionCacheBlockingStub>() {
            @Override
            public ActionCacheBlockingStub get() {
              return ActionCacheGrpc.newBlockingStub(channel);
            }
          });

  private final Supplier<ContentAddressableStorageBlockingStub> contentAddressableStorageBlockingStub =
      Suppliers.memoize(
          new Supplier<ContentAddressableStorageBlockingStub>() {
            @Override
            public ContentAddressableStorageBlockingStub get() {
              return ContentAddressableStorageGrpc.newBlockingStub(channel);
            }
          });

  private final Supplier<ByteStreamBlockingStub> bsBlockingStub =
      Suppliers.memoize(
          new Supplier<ByteStreamBlockingStub>() {
            @Override
            public ByteStreamBlockingStub get() {
              return ByteStreamGrpc.newBlockingStub(channel);
            }
          });

  private final Supplier<ByteStreamStub> bsStub =
      Suppliers.memoize(
          new Supplier<ByteStreamStub>() {
            @Override
            public ByteStreamStub get() {
              return ByteStreamGrpc.newStub(channel);
            }
          });

  private final Supplier<OperationQueueBlockingStub> operationQueueBlockingStub =
      Suppliers.memoize(
          new Supplier<OperationQueueBlockingStub>() {
            @Override
            public OperationQueueBlockingStub get() {
              return OperationQueueGrpc.newBlockingStub(channel);
            }
          });

  @Override
  public String getName() {
    return name;
  }

  @Override
  public ActionResult getActionResult(Digest actionDigest) {
    return null;
  }

  @Override
  public void putActionResult(Digest actionDigest, ActionResult actionResult) {
    actionCacheBlockingStub.get().updateActionResult(UpdateActionResultRequest.newBuilder()
        .setInstanceName(getName())
        .setActionDigest(actionDigest)
        .setActionResult(actionResult)
        .build());
  }

  @Override
  public Iterable<Digest> findMissingBlobs(Iterable<Digest> digests) {
    FindMissingBlobsResponse response = contentAddressableStorageBlockingStub
        .get()
        .findMissingBlobs(FindMissingBlobsRequest.newBuilder()
            .setInstanceName(getName())
            .addAllBlobDigests(digests)
            .build());
    return response.getMissingBlobDigestsList();
  }

  private void uploadChunks(int numItems, Iterable<Chunker.Chunk> chunks)
      throws InterruptedException {
    final CountDownLatch finishLatch = new CountDownLatch(numItems);
    final AtomicReference<RuntimeException> exception = new AtomicReference<>(null);
    StreamObserver<WriteRequest> requestObserver = null;
    for( Chunker.Chunk chunk : chunks ) {
      final Digest digest = chunk.getDigest();
      long offset = chunk.getOffset();
      WriteRequest.Builder request = WriteRequest.newBuilder();
      if (offset == 0) { // Beginning of new upload.
        numItems--;
        request.setResourceName(String.format(
            "%s/uploads/%s/blobs/%s",
            getName(),
            UUID.randomUUID(),
            Digests.toString(digest)));
        // The batches execute simultaneously.
        requestObserver =
            bsStub
                .get()
                .write(
                    new StreamObserver<WriteResponse>() {
                      private long bytesLeft = digest.getSizeBytes();

                      @Override
                      public void onNext(WriteResponse reply) {
                        bytesLeft -= reply.getCommittedSize();
                      }

                      @Override
                      public void onError(Throwable t) {
                        exception.compareAndSet(
                            null, new StatusRuntimeException(Status.fromThrowable(t)));
                        finishLatch.countDown();
                      }

                      @Override
                      public void onCompleted() {
                        if (bytesLeft != 0) {
                          exception.compareAndSet(
                              null, new RuntimeException("Server did not commit all data."));
                        }
                        finishLatch.countDown();
                      }
                    });
      }
      ByteString data = ByteString.copyFrom(chunk.getData());
      boolean finishWrite = offset + data.size() == digest.getSizeBytes();
      request.setData(data).setWriteOffset(offset).setFinishWrite(finishWrite);
      requestObserver.onNext(request.build());
      if (finishWrite) {
        requestObserver.onCompleted();
      }
    }
    finishLatch.await(60 /* FIXME options.remoteTimeout */, TimeUnit.SECONDS);
    if (exception.get() != null) {
      throw exception.get(); // Re-throw the first encountered exception.
    }
  }

  @Override
  public Iterable<Digest> putAllBlobs(Iterable<ByteString> blobs)
      throws IllegalArgumentException, InterruptedException {
    // comments in the chunker indicate this, all inputs are guaranteed to
    // end up in the same order as they are inserted
    Chunker chunker = new Chunker.Builder().addAllInputs(blobs).build();
    List<Chunker.Chunk> chunks = new ImmutableList.Builder<Chunker.Chunk>()
        .addAll(chunker)
        .build();
    uploadChunks(chunks.size(), chunks);
    return new ImmutableList.Builder<Digest>()
        .addAll(Iterables.transform(
            Iterables.filter(chunks, (chunk) -> chunk.getOffset() == 0),
            (chunk) -> chunk.getDigest()))
        .build();
  }

  @Override
  public OutputStream getStreamOutput(String name) {
    return new OutputStream() {
      boolean closed = false;
      String resourceName = name;
      long written_bytes = 0;
      final AtomicReference<RuntimeException> exception = new AtomicReference<>(null);
      StreamObserver<WriteRequest> requestObserver = bsStub.get()
          .write(
              new StreamObserver<WriteResponse>() {
                @Override
                public void onNext(WriteResponse reply) {
                }

                @Override
                public void onError(Throwable t) {
                  exception.compareAndSet(
                      null, new StatusRuntimeException(Status.fromThrowable(t)));
                }

                @Override
                public void onCompleted() {
                  if (!closed) {
                    exception.compareAndSet(
                        null, new RuntimeException("Server closed connection before output stream."));
                  }
                }
              }
          );

      @Override
      public void close() {
        closed = true;
        requestObserver.onNext(WriteRequest.newBuilder()
            .setResourceName(resourceName)
            .setFinishWrite(true)
            .build());
      }

      @Override
      public void write(int b) throws IOException {
        byte[] buf = new byte[1];
        buf[0] = (byte) b;
        write(buf);
      }

      @Override
      public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        if (closed) {
          throw new IOException();
        }
        requestObserver.onNext(WriteRequest.newBuilder()
            .setResourceName(resourceName)
            .setData(ByteString.copyFrom(b, off, len))
            .setWriteOffset(written_bytes)
            .setFinishWrite(false)
            .build());
        if (exception.get() != null) {
          throw exception.get();
        }
        written_bytes += len;
      }
    };
  }

  @Override
  public InputStream newStreamInput(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteString getBlob(Digest blobDigest) {
    String resourceName = String.format(
        "%s/blobs/%s",
        getName(),
        Digests.toString(blobDigest));
    Iterator<ReadResponse> replies = bsBlockingStub
      .get()
      .read(ReadRequest.newBuilder().setResourceName(resourceName).build());
    ByteString blob = ByteString.EMPTY;
    while( replies.hasNext() ) {
      blob = blob.concat(replies.next().getData());
    }
    return blob;
  }

  @Override
  public ByteString getBlob(Digest blobDigest, long offset, long limit) {
    return null;
  }

  @Override
  public Digest putBlob(ByteString blob)
      throws IllegalArgumentException, InterruptedException {
    Chunker chunker = new Chunker.Builder().addInput(blob).build();
    List<Chunker.Chunk> chunks = new ImmutableList.Builder<Chunker.Chunk>()
        .addAll(chunker)
        .build();
    uploadChunks(chunks.size(), chunks);
    return chunks.get(0).getDigest();
  }

  @Override
  public String getTree(
      Digest rootDigest,
      int pageSize,
      String pageToken,
      ImmutableList.Builder<Directory> directories) {
    GetTreeResponse response = contentAddressableStorageBlockingStub
        .get()
        .getTree(GetTreeRequest.newBuilder()
            .setInstanceName(getName())
            .setRootDigest(rootDigest)
            .setPageSize(pageSize)
            .setPageToken(pageToken)
            .build());
    directories.addAll(response.getDirectoriesList());
    return response.getNextPageToken();
  }

  @Override
  public void execute(
      Action action,
      boolean skipCacheLookup,
      int totalInputFileCount,
      long totalInputFileBytes,
      boolean waitForCompletion,
      Consumer<Operation> onOperation) {
    throw new UnsupportedOperationException();
  }

  private void requeue(Operation operation) {
    try {
      ExecuteOperationMetadata metadata =
          operation.getMetadata().unpack(ExecuteOperationMetadata.class);

      ExecuteOperationMetadata executingMetadata = metadata.toBuilder()
          .setStage(ExecuteOperationMetadata.Stage.QUEUED)
          .build();

      operation = operation.toBuilder()
          .setMetadata(Any.pack(executingMetadata))
          .build();
      putOperation(operation);
    } catch(InvalidProtocolBufferException ex) {
      // operation is dropped on the floor
    }
  }

  @Override
  public void match(Platform platform, boolean requeueOnFailure, Function<Operation, Boolean> onMatch) {
    Operation operation = operationQueueBlockingStub.get().take(TakeOperationRequest.newBuilder()
        .setInstanceName(getName())
        .setPlatform(platform)
        .build());
    boolean successful = onMatch.apply(operation);
    if (!successful && requeueOnFailure) {
      requeue(operation);
    }
  }

  @Override
  public void putOperation(Operation operation) {
    operationQueueBlockingStub.get().put(operation);
  }

  @Override
  public boolean watchOperation(
      String operationName,
      boolean watchInitialState,
      Consumer<Operation> watcher) {
    return false;
  }

  @Override
  public String listOperations(
      int pageSize, String pageToken, String filter,
      ImmutableList.Builder<Operation> operations) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Operation getOperation(String operationName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void cancelOperation(String operationName) {
    throw new UnsupportedOperationException();
  }
}
