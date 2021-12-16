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

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecutionPolicy;
import build.bazel.remote.execution.v2.ExecutionStage;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.ResultsCachePolicy;
import build.buildfarm.backplane.Backplane;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.common.CasIndexResults;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.TokenizableIterator;
import build.buildfarm.common.TreeIterator.DirectoryEntry;
import build.buildfarm.common.Watcher;
import build.buildfarm.common.Write;
import build.buildfarm.common.grpc.UniformDelegateServerCallStreamObserver;
import build.buildfarm.instance.MatchListener;
import build.buildfarm.instance.server.AbstractServerInstance;
import build.buildfarm.operations.FindOperationsResults;
import build.buildfarm.v1test.BackplaneStatus;
import build.buildfarm.v1test.CompletedOperationMetadata;
import build.buildfarm.v1test.ExecutingOperationMetadata;
import build.buildfarm.v1test.GetClientStartTimeRequest;
import build.buildfarm.v1test.GetClientStartTimeResult;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.QueuedOperationMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.stub.ServerCallStreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ShardWorkerInstance extends AbstractServerInstance {
  private static final Logger logger = Logger.getLogger(ShardWorkerInstance.class.getName());

  private final Backplane backplane;

  public ShardWorkerInstance(
      String name,
      DigestUtil digestUtil,
      Backplane backplane,
      ContentAddressableStorage contentAddressableStorage) {
    super(name, digestUtil, contentAddressableStorage, null, null, null, null);
    this.backplane = backplane;
  }

  @Override
  public ListenableFuture<ActionResult> getActionResult(
      ActionKey actionKey, RequestMetadata requestMetadata) {
    return immediateFailedFuture(new UnsupportedOperationException());
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
  protected TokenizableIterator<Operation> createOperationsIterator(String pageToken) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected int getListOperationsDefaultPageSize() {
    return 1024;
  }

  @Override
  protected int getListOperationsMaxPageSize() {
    return 1024;
  }

  @Override
  public String getBlobName(Digest blobDigest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void getBlob(
      Digest digest,
      long offset,
      long count,
      ServerCallStreamObserver<ByteString> blobObserver,
      RequestMetadata requestMetadata) {
    Preconditions.checkState(count != 0);
    contentAddressableStorage.get(
        digest,
        offset,
        count,
        new UniformDelegateServerCallStreamObserver<ByteString>(blobObserver) {
          @Override
          public void onNext(ByteString data) {
            blobObserver.onNext(data);
          }

          void removeBlobLocation() {
            try {
              backplane.removeBlobLocation(digest, getName());
            } catch (IOException backplaneException) {
              logger.log(
                  Level.SEVERE,
                  String.format("error removing blob location for %s", DigestUtil.toString(digest)),
                  backplaneException);
            }
          }

          @Override
          public void onError(Throwable t) {
            if (t instanceof IOException) {
              blobObserver.onError(Status.NOT_FOUND.withCause(t).asException());
              removeBlobLocation();
            } else {
              blobObserver.onError(t);
            }
          }

          @Override
          public void onCompleted() {
            blobObserver.onCompleted();
          }
        },
        requestMetadata);
  }

  protected TokenizableIterator<DirectoryEntry> createTreeIterator(
      String reason, Digest rootDigest, String pageToken) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Write getOperationStreamWrite(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream newOperationStreamInput(
      String name, long offset, RequestMetadata requestMetadata) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<Void> execute(
      Digest actionDigest,
      boolean skipCacheLookup,
      ExecutionPolicy executionPolicy,
      ResultsCachePolicy resultsCachePolicy,
      RequestMetadata requestMetadata,
      Watcher watcher) {
    throw new UnsupportedOperationException();
  }

  @VisibleForTesting
  public QueueEntry dispatchOperation(MatchListener listener)
      throws IOException, InterruptedException {
    while (!backplane.isStopped()) {
      listener.onWaitStart();
      try {
        List<Platform.Property> provisions = new ArrayList<>();
        QueueEntry queueEntry = backplane.dispatchOperation(provisions);
        if (queueEntry != null) {
          return queueEntry;
        }
      } catch (IOException e) {
        Status status = Status.fromThrowable(e);
        if (status.getCode() != Code.UNAVAILABLE && status.getCode() != Code.DEADLINE_EXCEEDED) {
          throw e;
        }
      }
      listener.onWaitEnd();
    }
    throw new IOException(Status.UNAVAILABLE.withDescription("backplane is stopped").asException());
  }

  @Override
  public void match(Platform platform, MatchListener listener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BackplaneStatus backplaneStatus() {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public boolean putOperation(Operation operation) {
    try {
      return backplane.putOperation(
          operation, expectExecuteOperationMetadata(operation).getStage());
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  @Override
  public boolean pollOperation(String operationName, ExecutionStage.Value stage) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected boolean matchOperation(Operation operation) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void enqueueOperation(Operation operation) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected Object operationLock() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected Operation createOperation(ActionKey actionKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected int getTreeDefaultPageSize() {
    return 1024;
  }

  @Override
  protected int getTreeMaxPageSize() {
    return 1024;
  }

  @Override
  public Operation getOperation(String name) {
    while (!backplane.isStopped()) {
      try {
        return backplane.getOperation(name);
      } catch (IOException e) {
        Status status = Status.fromThrowable(e);
        if (status.getCode() != Code.UNAVAILABLE && status.getCode() != Code.DEADLINE_EXCEEDED) {
          throw status.asRuntimeException();
        }
      }
    }
    throw Status.UNAVAILABLE.withDescription("backplane is stopped").asRuntimeException();
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
  public ListenableFuture<Void> watchOperation(String operationName, Watcher watcher) {
    throw new UnsupportedOperationException();
  }

  protected static ExecuteOperationMetadata expectExecuteOperationMetadata(Operation operation) {
    if (operation.getMetadata().is(QueuedOperationMetadata.class)) {
      try {
        return operation
            .getMetadata()
            .unpack(QueuedOperationMetadata.class)
            .getExecuteOperationMetadata();
      } catch (InvalidProtocolBufferException e) {
        logger.log(
            Level.SEVERE,
            String.format("error unpacking queued operation metadata from %s", operation.getName()),
            e);
        return null;
      }
    } else if (operation.getMetadata().is(ExecutingOperationMetadata.class)) {
      try {
        return operation
            .getMetadata()
            .unpack(ExecutingOperationMetadata.class)
            .getExecuteOperationMetadata();
      } catch (InvalidProtocolBufferException e) {
        logger.log(
            Level.SEVERE,
            String.format(
                "error unpacking executing operation metadata from %s", operation.getName()),
            e);
        return null;
      }
    } else if (operation.getMetadata().is(CompletedOperationMetadata.class)) {
      try {
        return operation
            .getMetadata()
            .unpack(CompletedOperationMetadata.class)
            .getExecuteOperationMetadata();
      } catch (InvalidProtocolBufferException e) {
        logger.log(
            Level.SEVERE,
            String.format(
                "error unpacking completed operation metadata from %s", operation.getName()),
            e);
        return null;
      }
    } else {
      return AbstractServerInstance.expectExecuteOperationMetadata(operation);
    }
  }

  public Operation stripOperation(Operation operation) {
    return operation
        .toBuilder()
        .setMetadata(Any.pack(expectExecuteOperationMetadata(operation)))
        .build();
  }

  public Operation stripQueuedOperation(Operation operation) {
    if (operation.getMetadata().is(QueuedOperationMetadata.class)) {
      operation =
          operation
              .toBuilder()
              .setMetadata(Any.pack(expectExecuteOperationMetadata(operation)))
              .build();
    }
    return operation;
  }

  @Override
  protected Logger getLogger() {
    return logger;
  }

  @Override
  public GetClientStartTimeResult getClientStartTime(GetClientStartTimeRequest request) {
    try {
      return backplane.getClientStartTime(request);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  @Override
  public CasIndexResults reindexCas(String hostName) {
    try {
      return backplane.reindexCas(hostName);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  @Override
  public FindOperationsResults findOperations(String filterPredicate) {
    try {
      return backplane.findOperations(this, filterPredicate);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  @Override
  public void deregisterWorker(String workerName) {
    try {
      backplane.deregisterWorker(workerName);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }
}
