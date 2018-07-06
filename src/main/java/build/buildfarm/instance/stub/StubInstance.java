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

import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.instance.Instance;
import build.buildfarm.v1test.OperationQueueGrpc;
import build.buildfarm.v1test.OperationQueueGrpc.OperationQueueBlockingStub;
import build.buildfarm.v1test.PollOperationRequest;
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
import com.google.common.collect.Iterators;
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.ActionCacheGrpc;
import com.google.devtools.remoteexecution.v1test.ActionCacheGrpc.ActionCacheBlockingStub;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.ContentAddressableStorageGrpc;
import com.google.devtools.remoteexecution.v1test.ContentAddressableStorageGrpc.ContentAddressableStorageBlockingStub;
import com.google.devtools.remoteexecution.v1test.ExecuteRequest;
import com.google.devtools.remoteexecution.v1test.ExecutionGrpc;
import com.google.devtools.remoteexecution.v1test.ExecutionGrpc.ExecutionBlockingStub;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata;
import com.google.devtools.remoteexecution.v1test.FindMissingBlobsRequest;
import com.google.devtools.remoteexecution.v1test.FindMissingBlobsResponse;
import com.google.devtools.remoteexecution.v1test.GetActionResultRequest;
import com.google.devtools.remoteexecution.v1test.GetTreeRequest;
import com.google.devtools.remoteexecution.v1test.GetTreeResponse;
import com.google.devtools.remoteexecution.v1test.Platform;
import com.google.devtools.remoteexecution.v1test.UpdateActionResultRequest;
import com.google.longrunning.CancelOperationRequest;
import com.google.longrunning.DeleteOperationRequest;
import com.google.longrunning.GetOperationRequest;
import com.google.longrunning.ListOperationsRequest;
import com.google.longrunning.ListOperationsResponse;
import com.google.longrunning.OperationsGrpc;
import com.google.longrunning.OperationsGrpc.OperationsBlockingStub;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class StubInstance implements Instance {
  private final String name;
  private final DigestUtil digestUtil;
  private final ManagedChannel channel;
  private final Retrier retrier;
  private final ByteStreamUploader uploader;
  private final long deadlineAfter;
  private final TimeUnit deadlineAfterUnits;

  public StubInstance(
      String name,
      DigestUtil digestUtil,
      ManagedChannel channel,
      long deadlineAfter, TimeUnit deadlineAfterUnits,
      Retrier retrier,
      ByteStreamUploader uploader) {
    this.name = name;
    this.digestUtil = digestUtil;
    this.channel = channel;
    this.deadlineAfter = deadlineAfter;
    this.deadlineAfterUnits = deadlineAfterUnits;
    this.retrier = retrier;
    this.uploader = uploader;
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

  private final Supplier<OperationsBlockingStub> operationsBlockingStub =
      Suppliers.memoize(
          new Supplier<OperationsBlockingStub>() {
            @Override
            public OperationsBlockingStub get() {
              return OperationsGrpc.newBlockingStub(channel);
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

  private final Supplier<ExecutionBlockingStub> executionBlockingStub =
      Suppliers.memoize(
          new Supplier<ExecutionBlockingStub>() {
            @Override
            public ExecutionBlockingStub get() {
              return ExecutionGrpc.newBlockingStub(channel);
            }
          });

  @Override
  public String getName() {
    return name;
  }

  @Override
  public DigestUtil getDigestUtil() {
    return digestUtil;
  }

  public ManagedChannel getChannel() {
    return channel;
  }

  @Override
  public void start() { }

  @Override
  public void stop() { }

  @Override
  public ActionResult getActionResult(ActionKey actionKey) {
    try {
      return actionCacheBlockingStub.get()
          .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
          .getActionResult(GetActionResultRequest.newBuilder()
              .setInstanceName(getName())
              .setActionDigest(actionKey.getDigest())
              .build());
    } catch (StatusRuntimeException e) {
      if (e.getStatus().equals(Status.NOT_FOUND)) {
        return null;
      }
      throw e;
    }
  }

  @Override
  public void putActionResult(ActionKey actionKey, ActionResult actionResult) {
    actionCacheBlockingStub.get()
        .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
        .updateActionResult(UpdateActionResultRequest.newBuilder()
        .setInstanceName(getName())
        .setActionDigest(actionKey.getDigest())
        .setActionResult(actionResult)
        .build());
  }

  @Override
  public Iterable<Digest> findMissingBlobs(Iterable<Digest> digests) {
    FindMissingBlobsRequest request = FindMissingBlobsRequest.newBuilder()
            .setInstanceName(getName())
            .addAllBlobDigests(digests)
            .build();
    if (request.getSerializedSize() > 4 * 1024 * 1024) {
      throw new IllegalStateException("FINDMISSINGBLOBS IS TOO LARGE");
    }
    FindMissingBlobsResponse response = contentAddressableStorageBlockingStub
        .get()
        .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
        .findMissingBlobs(request);
    return response.getMissingBlobDigestsList();
  }

  @Override
  public Iterable<Digest> putAllBlobs(Iterable<ByteString> blobs)
      throws IOException, IllegalArgumentException, InterruptedException {
    // sort of a blatant misuse - one chunker per input, query digests before exhausting iterators
    ImmutableList.Builder<Chunker> builder = new ImmutableList.Builder<Chunker>();
    for (ByteString blob : blobs) {
      builder.add(new Chunker(blob, digestUtil.compute(blob)));
    }
    ImmutableList<Chunker> chunkers = builder.build();
    List<Digest> digests = new ImmutableList.Builder<Digest>()
        .addAll(Iterables.transform(chunkers, chunker -> chunker.digest()))
        .build();
    uploader.uploadBlobs(chunkers);
    return digests;
  }

  @Override
  public OutputStream getStreamOutput(String name) {
    return new OutputStream() {
      boolean closed = false;
      String resourceName = name;
      long written_bytes = 0;
      final AtomicReference<RuntimeException> exception = new AtomicReference<>(null);
      StreamObserver<WriteRequest> requestObserver = bsStub.get()
          .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
          .write(
              new StreamObserver<WriteResponse>() {
                @Override
                public void onNext(WriteResponse reply) {
                }

                @Override
                public void onError(Throwable t) {
                  exception.compareAndSet(
                      null, Status.fromThrowable(t).asRuntimeException());
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
  public InputStream newStreamInput(String name) throws InterruptedException, IOException {
    Iterator<ReadResponse> replies = retrier.execute(() -> bsBlockingStub
        .get()
        .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
        .read(ReadRequest.newBuilder().setResourceName(name).build()));
    return new ByteStringIteratorInputStream(Iterators.transform(replies, (reply) -> reply.getData()), retrier);
  }

  @Override
  public String getBlobName(Digest blobDigest) {
    return String.format(
        "%s/blobs/%s",
        getName(),
        DigestUtil.toString(blobDigest));
  }

  @Override
  public ByteString getBlob(Digest blobDigest) throws InterruptedException, IOException {
    try (InputStream in = newStreamInput(getBlobName(blobDigest))) {
      return ByteString.readFrom(in);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().equals(Status.NOT_FOUND)) {
        return null;
      }
      throw e;
    }
  }

  @Override
  public ByteString getBlob(Digest blobDigest, long offset, long limit) throws InterruptedException, IOException {
    try (InputStream in = newStreamInput(getBlobName(blobDigest))) {
      in.skip(offset);
      if (limit == 0) {
        return ByteString.readFrom(in);
      }
      int len = (int) limit;
      byte[] buf = new byte[len];
      len = in.read(buf);
      return ByteString.copyFrom(buf, 0, len);
    } catch (IOException e) {
      return null;
    }
  }

  @Override
  public Digest putBlob(ByteString blob)
      throws IOException, IllegalArgumentException, InterruptedException {
    Digest digest = digestUtil.compute(blob);
    Chunker chunker = new Chunker(blob, digest);
    uploader.uploadBlobs(Collections.singleton(chunker));
    return digest;
  }

  @Override
  public String getTree(
      Digest rootDigest,
      int pageSize,
      String pageToken,
      ImmutableList.Builder<Directory> directories,
      boolean acceptMissing) {
    GetTreeResponse response = contentAddressableStorageBlockingStub
        .get()
        .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
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
      Consumer<Operation> onOperation) {
    onOperation.accept(executionBlockingStub
        .get()
        .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
        .execute(ExecuteRequest.newBuilder()
            .setAction(action)
            .setSkipCacheLookup(skipCacheLookup)
            .setTotalInputFileCount(totalInputFileCount)
            .setTotalInputFileBytes(totalInputFileBytes)
            .build()));
  }

  @Override
  public void match(Platform platform, MatchListener listener) throws InterruptedException {
    TakeOperationRequest request = TakeOperationRequest.newBuilder()
        .setInstanceName(getName())
        .setPlatform(platform)
        .build();
    // not required to call onOperationName
    listener.onWaitStart();
    Operation operation = operationQueueBlockingStub.get()
        .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
        .take(request);
    listener.onWaitEnd();
    listener.onOperation(operation);
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
  }

  @Override
  public boolean putOperation(Operation operation) {
    return operationQueueBlockingStub
        .get()
        .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
        .put(operation)
        .getCode() == Code.OK.getNumber();
  }

  @Override
  public boolean pollOperation(
      String operationName,
      ExecuteOperationMetadata.Stage stage) {
    return operationQueueBlockingStub
        .get()
        .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
        .poll(PollOperationRequest.newBuilder()
            .setOperationName(operationName)
            .setStage(stage)
            .build())
        .getCode() == Code.OK.getNumber();
  }

  @Override
  public boolean watchOperation(
      String operationName,
      boolean watchInitialState,
      Predicate<Operation> watcher) {
    return false;
  }

  @Override
  public String listOperations(
      int pageSize, String pageToken, String filter,
      ImmutableList.Builder<Operation> operations) {

    ListOperationsResponse response =
        operationsBlockingStub.get()
            .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
            .listOperations(ListOperationsRequest.newBuilder()
            .setName(getName() + "/operations")
            .setPageSize(pageSize)
            .setPageToken(pageToken)
            .setFilter(filter)
            .build());
    operations.addAll(response.getOperationsList());
    return response.getNextPageToken();
  }

  @Override
  public Operation getOperation(String operationName) {
    return operationsBlockingStub.get()
        .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
        .getOperation(GetOperationRequest.newBuilder()
            .setName(operationName)
            .build());
  }

  @Override
  public void deleteOperation(String operationName) {
    operationsBlockingStub.get()
        .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
        .deleteOperation(DeleteOperationRequest.newBuilder()
            .setName(operationName)
            .build());
  }

  @Override
  public void cancelOperation(String operationName) {
    operationsBlockingStub.get()
        .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
        .cancelOperation(CancelOperationRequest.newBuilder()
        .setName(operationName)
        .build());
  }
}
