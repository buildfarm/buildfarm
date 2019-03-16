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

package build.buildfarm.worker.shard;

import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.allAsList;
import static java.util.logging.Level.SEVERE;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata.Stage;
import build.bazel.remote.execution.v2.ExecutionPolicy;
import build.bazel.remote.execution.v2.ResultsCachePolicy;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.common.ShardBackplane;
import build.buildfarm.instance.AbstractServerInstance;
import build.buildfarm.instance.TokenizableIterator;
import build.buildfarm.instance.TreeIterator.DirectoryEntry;
import build.buildfarm.v1test.CompletedOperationMetadata;
import build.buildfarm.v1test.ExecutingOperationMetadata;
import build.buildfarm.v1test.QueuedOperationMetadata;
import build.buildfarm.v1test.ShardWorkerInstanceConfig;
import build.buildfarm.worker.OutputStreamFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Predicate;
import javax.naming.ConfigurationException;
import java.util.logging.Logger;

public class ShardWorkerInstance extends AbstractServerInstance {
  private static final Logger logger = Logger.getLogger(ShardWorkerInstance.class.getName());

  private final ShardWorkerInstanceConfig config;
  private final ShardBackplane backplane;
  private final InputStreamFactory inputStreamFactory;
  private final OutputStreamFactory outputStreamFactory;

  public ShardWorkerInstance(
      String name,
      DigestUtil digestUtil,
      ShardBackplane backplane,
      ContentAddressableStorage contentAddressableStorage,
      InputStreamFactory inputStreamFactory,
      OutputStreamFactory outputStreamFactory,
      ShardWorkerInstanceConfig config) throws ConfigurationException {
    super(name, digestUtil, contentAddressableStorage, null, null, null, null);
    this.config = config;
    this.backplane = backplane;
    this.inputStreamFactory = inputStreamFactory;
    this.outputStreamFactory = outputStreamFactory;
  }

  @Override
  public ActionResult getActionResult(ActionKey actionKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putActionResult(ActionKey actionKey, ActionResult actionResult) {
    try {
      backplane.putActionResult(actionKey, actionResult);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  @Override
  protected TokenizableIterator<Operation> createOperationsIterator(String pageToken) { throw new UnsupportedOperationException(); }

  @Override
  protected int getListOperationsDefaultPageSize() { return 1024; }

  @Override
  protected int getListOperationsMaxPageSize() { return 1024; }

  @Override
  public String getBlobName(Digest blobDigest) {
    throw new UnsupportedOperationException();
  }

  private void getBlob(InputStream input, long remaining, long limit, StreamObserver<ByteString> blobObserver) {
    try {
      if (limit == 0 || limit > remaining) {
        limit = remaining;
      }
      // slice up into 1M chunks
      long chunkSize = Math.min(1024 * 1024, limit);
      byte[] chunk = new byte[(int) chunkSize];
      while (limit > 0) {
        int n = input.read(chunk);
        blobObserver.onNext(ByteString.copyFrom(chunk, 0, n));
        limit -= n;
      }
      blobObserver.onCompleted();
    } catch (IOException e) {
      blobObserver.onError(e);
    }
  }

  @Override
  public void getBlob(Digest blobDigest, long offset, long limit, StreamObserver<ByteString> blobObserver) {
    try (InputStream input = inputStreamFactory.newInput(blobDigest, offset)) {
      getBlob(input, blobDigest.getSizeBytes() - offset, limit, blobObserver);
    } catch (IOException e) {
      blobObserver.onError(Status.NOT_FOUND.withCause(e).asException());
      try {
        backplane.removeBlobLocation(blobDigest, getName());
      } catch (IOException backplaneException) {
        logger.log(SEVERE, "error removing blob location for " + DigestUtil.toString(blobDigest), backplaneException);
      }
    } catch (InterruptedException e) {
      blobObserver.onError(Status.CANCELLED.withCause(e).asException());
    }
  }

  @Override
  public ByteString getBlob(Digest blobDigest, long offset, long limit) throws IOException {
    Blob blob = contentAddressableStorage.get(blobDigest);
    if (blob == null) {
      backplane.removeBlobLocation(blobDigest, getName());
      return null;
    }
    ByteString content = blob.getData();
    if (offset != 0 || limit != 0) {
      content = content.substring((int) offset, (int) (limit == 0 ? (content.size() - offset) : (limit + offset)));
    }
    return content;
  }

  @Override
  public ChunkObserver getWriteBlobObserver(Digest blobDigest) {
    return new OutputStreamChunkObserver() {
      @Override
      protected OutputStream newOutput() throws IOException {
        return outputStreamFactory.newOutput(blobDigest);
      }
    };
  }

  protected TokenizableIterator<DirectoryEntry> createTreeIterator(Digest rootDigest, String pageToken) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getTree(
      Digest rootDigest,
      int pageSize,
      String pageToken,
      ImmutableList.Builder<Directory> directories,
      boolean acceptMissing) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CommittingOutputStream getStreamOutput(String name, long expectedSize) {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream newStreamInput(String name, long offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void execute(
      Digest actionDigest, 
      boolean skipCacheLookup, 
      ExecutionPolicy executionPolicy, 
      ResultsCachePolicy resultsCachePolicy, 
      RequestMetadata requestMetadata,
      Predicate<Operation> watcher) {
    throw new UnsupportedOperationException();
  }

  @VisibleForTesting
  public String dispatchOperation(MatchListener listener) throws IOException, InterruptedException {
    for (;;) {
      listener.onWaitStart();
      try {
        String operationName = backplane.dispatchOperation();
        if (operationName != null) {
          return operationName;
        }
      } catch (IOException e) {
        Status status = Status.fromThrowable(e);
        if (status.getCode() != Code.UNAVAILABLE && status.getCode() != Code.DEADLINE_EXCEEDED) {
          throw e;
        }
      }
      listener.onWaitEnd();
    }
  }

  private void matchResettable(Platform platform, MatchListener listener) throws IOException, InterruptedException {
    String operationName = dispatchOperation(listener);

    // FIXME platform match
    if (listener.onOperationName(operationName)) {
      // onOperation must be called after this point, or we must throw
      Operation operation = getOperation(operationName);
      if (operation == null) {
        operation = Operation.newBuilder()
            .setName(operationName)
            .setDone(true)
            .build();
      }
      listener.onOperation(operation);
    }
  }

  private void matchInterruptible(Platform platform, MatchListener listener) throws IOException, InterruptedException {
    matchResettable(platform, listener);
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
  }

  @Override
  public void match(Platform platform, MatchListener listener) throws InterruptedException {
    try {
      matchInterruptible(platform, listener);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  @Override
  public boolean putOperation(Operation operation) {
    try {
      return backplane.putOperation(operation, expectExecuteOperationMetadata(operation).getStage());
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  @Override
  public boolean pollOperation(String operationName, Stage stage) {
    throw new UnsupportedOperationException();
  }

  // returns nextPageToken suitable for list restart
  @Override
  public String listOperations(
      int pageSize,
      String pageToken,
      String filter,
      ImmutableList.Builder<Operation> operations) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected boolean matchOperation(Operation operation) { throw new UnsupportedOperationException(); }

  @Override
  protected void enqueueOperation(Operation operation) { throw new UnsupportedOperationException(); }

  @Override
  protected Object operationLock(String operationName) { throw new UnsupportedOperationException(); }

  @Override
  protected Operation createOperation(ActionKey actionKey) { throw new UnsupportedOperationException(); }

  @Override
  protected int getTreeDefaultPageSize() { return 1024; }

  @Override
  protected int getTreeMaxPageSize() { return 1024; }

  @Override
  public Operation getOperation(String name) {
    for (;;) {
      try {
        return backplane.getOperation(name);
      } catch (IOException e) {
        Status status = Status.fromThrowable(e);
        if (status.getCode() != Code.UNAVAILABLE && status.getCode() != Code.DEADLINE_EXCEEDED) {
          throw status.asRuntimeException();
        }
      }
    }
  }

  @Override
  public void cancelOperation(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteOperation(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean watchOperation(
      String operationName,
      Predicate<Operation> watcher) {
    throw new UnsupportedOperationException();
  }

  protected static ExecuteOperationMetadata expectExecuteOperationMetadata(
      Operation operation) {
    if (operation.getMetadata().is(QueuedOperationMetadata.class)) {
      try {
        return operation.getMetadata().unpack(QueuedOperationMetadata.class).getExecuteOperationMetadata();
      } catch(InvalidProtocolBufferException e) {
        logger.log(SEVERE, "error unpacking queued operation metadata from " + operation.getName(), e);
        return null;
      }
    } else if (operation.getMetadata().is(ExecutingOperationMetadata.class)) {
      try {
        return operation.getMetadata().unpack(ExecutingOperationMetadata.class).getExecuteOperationMetadata();
      } catch(InvalidProtocolBufferException e) {
        logger.log(SEVERE, "error unpacking executing operation metadata from " + operation.getName(), e);
        return null;
      }
    } else if (operation.getMetadata().is(CompletedOperationMetadata.class)) {
      try {
        return operation.getMetadata().unpack(CompletedOperationMetadata.class).getExecuteOperationMetadata();
      } catch(InvalidProtocolBufferException e) {
        logger.log(SEVERE, "error unpacking completed operation metadata from " + operation.getName(), e);
        return null;
      }
    } else {
      return AbstractServerInstance.expectExecuteOperationMetadata(operation);
    }
  }

  public Operation stripOperation(Operation operation) {
    return operation.toBuilder()
        .setMetadata(Any.pack(expectExecuteOperationMetadata(operation)))
        .build();
  }

  public Operation stripQueuedOperation(Operation operation) {
    if (operation.getMetadata().is(QueuedOperationMetadata.class)) {
      operation = operation.toBuilder()
        .setMetadata(Any.pack(expectExecuteOperationMetadata(operation)))
        .build();
    }
    return operation;
  }

  @Override
  protected Logger getLogger() {
    return logger;
  }
}
