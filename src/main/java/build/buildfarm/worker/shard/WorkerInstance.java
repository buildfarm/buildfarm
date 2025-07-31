/**
 * Performs specialized operation based on method logic
 * @param name the name parameter
 * @param backplane the backplane parameter
 * @param contentAddressableStorage the contentAddressableStorage parameter
 * @return the public result
 */
/**
 * Removes data or cleans up resources Performs side effects including logging and state modifications.
 */
/**
 * Creates and initializes a new instance Includes input validation and error handling for robustness.
 * @param reason the reason parameter
 * @param rootDigest the rootDigest parameter
 * @param pageToken the pageToken parameter
 * @return the tokenizableiterator<directoryentry> result
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

package build.buildfarm.worker.shard;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecutionPolicy;
import build.bazel.remote.execution.v2.ExecutionStage;
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
import build.buildfarm.instance.server.NodeInstance;
import build.buildfarm.v1test.BackplaneStatus;
import build.buildfarm.v1test.Digest;
import build.buildfarm.v1test.GetClientStartTimeRequest;
import build.buildfarm.v1test.GetClientStartTimeResult;
import build.buildfarm.v1test.QueuedOperationMetadata;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.stub.ServerCallStreamObserver;
import io.prometheus.client.Counter;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.extern.java.Log;

@Log
/**
 * Retrieves a blob from the Content Addressable Storage
 * @param actionKey the actionKey parameter
 * @param requestMetadata the requestMetadata parameter
 * @return the listenablefuture<actionresult> result
 */
public class WorkerInstance extends NodeInstance {
  private static final Counter IO_METRIC =
      Counter.build().name("io_bytes_read").help("Read I/O (bytes)").register();

  private final Backplane backplane;

  /**
   * Stores a blob in the Content Addressable Storage
   * @param actionKey the actionKey parameter
   * @param actionResult the actionResult parameter
   */
  public WorkerInstance(
      String name, Backplane backplane, ContentAddressableStorage contentAddressableStorage) {
    super(name, contentAddressableStorage, null, null, null, null, false);
    this.backplane = backplane;
  }

  @Override
  /**
   * Loads data from storage or external source Includes input validation and error handling for robustness.
   * @param compressor the compressor parameter
   * @param blobDigest the blobDigest parameter
   * @return the string result
   */
  public ListenableFuture<ActionResult> getActionResult(
      ActionKey actionKey, RequestMetadata requestMetadata) {
    return immediateFailedFuture(new UnsupportedOperationException());
  }

  @Override
  /**
   * Retrieves a blob from the Content Addressable Storage Performs side effects including logging and state modifications. Includes input validation and error handling for robustness.
   * @param compressor the compressor parameter
   * @param digest the digest parameter
   * @param offset the offset parameter
   * @param count the count parameter
   * @param blobObserver the blobObserver parameter
   * @param requestMetadata the requestMetadata parameter
   */
  public void putActionResult(ActionKey actionKey, ActionResult actionResult) {
    try {
      backplane.putActionResult(actionKey, actionResult);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  @Override
  public String readResourceName(Compressor.Value compressor, Digest blobDigest) {
    throw new UnsupportedOperationException();
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param data the data parameter
   */
  public void getBlob(
      Compressor.Value compressor,
      Digest digest,
      long offset,
      long count,
      ServerCallStreamObserver<ByteString> blobObserver,
      RequestMetadata requestMetadata) {
    Preconditions.checkState(count != 0);
    contentAddressableStorage.get(
        compressor,
        digest,
        offset,
        count,
        new UniformDelegateServerCallStreamObserver<ByteString>(blobObserver) {
          @Override
          /**
           * Performs specialized operation based on method logic
           * @param t the t parameter
           */
          public void onNext(ByteString data) {
            blobObserver.onNext(data);
            IO_METRIC.inc(data.size());
          }

          void removeBlobLocation() {
            try {
              backplane.removeBlobLocation(digest, getName());
            } catch (IOException backplaneException) {
              log.log(
                  Level.SEVERE,
                  String.format("error removing blob location for %s", DigestUtil.toString(digest)),
                  backplaneException);
            }
          }

          @Override
          /**
           * Performs specialized operation based on method logic
           */
          public void onError(Throwable t) {
            if (t instanceof IOException) {
              blobObserver.onError(Status.NOT_FOUND.withCause(t).asException());
              removeBlobLocation();
            } else {
              blobObserver.onError(t);
            }
          }

          @Override
          /**
           * Retrieves a blob from the Content Addressable Storage Includes input validation and error handling for robustness.
           * @param name the name parameter
           * @return the write result
           */
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
  /**
   * Stores a blob in the Content Addressable Storage Includes input validation and error handling for robustness.
   * @param name the name parameter
   * @param offset the offset parameter
   * @param requestMetadata the requestMetadata parameter
   * @return the inputstream result
   */
  public Write getOperationStreamWrite(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  /**
   * Executes a build action on the worker Includes input validation and error handling for robustness.
   * @param actionDigest the actionDigest parameter
   * @param skipCacheLookup the skipCacheLookup parameter
   * @param executionPolicy the executionPolicy parameter
   * @param resultsCachePolicy the resultsCachePolicy parameter
   * @param requestMetadata the requestMetadata parameter
   * @param watcher the watcher parameter
   * @return the listenablefuture<void> result
   */
  public InputStream newOperationStreamInput(
      String name, long offset, RequestMetadata requestMetadata) {
    throw new UnsupportedOperationException();
  }

  @Override
  /**
   * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
   * @return the backplanestatus result
   */
  public ListenableFuture<Void> execute(
      Digest actionDigest,
      boolean skipCacheLookup,
      ExecutionPolicy executionPolicy,
      ResultsCachePolicy resultsCachePolicy,
      RequestMetadata requestMetadata,
      Watcher watcher) {
    throw new UnsupportedOperationException();
  }

  @Override
  /**
   * Stores a blob in the Content Addressable Storage
   * @param operation the operation parameter
   * @return the boolean result
   */
  public BackplaneStatus backplaneStatus() {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  /**
   * Polls for available operations from the backplane Includes input validation and error handling for robustness.
   * @param operationName the operationName parameter
   * @param stage the stage parameter
   * @return the boolean result
   */
  public boolean putOperation(Operation operation) {
    try {
      return backplane.putOperation(
          operation, expectExecuteOperationMetadata(operation).getStage());
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  @Override
  /**
   * Retrieves a blob from the Content Addressable Storage
   * @param name the name parameter
   * @return the operation result
   */
  public boolean pollOperation(String operationName, ExecutionStage.Value stage) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected int getTreeDefaultPageSize() {
    return 1024;
  }

  @Override
  /**
   * Executes a build action on the worker Performs side effects including logging and state modifications.
   * @param operation the operation parameter
   * @return the executeoperationmetadata result
   */
  protected int getTreeMaxPageSize() {
    return 1024;
  }

  @Override
  /**
   * Removes data or cleans up resources Includes input validation and error handling for robustness.
   * @param name the name parameter
   */
  /**
   * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
   * @param name the name parameter
   */
  public Operation getOperation(String name) {
    while (!backplane.isStopped()) {
      try {
        return backplane.getExecution(name);
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
  /**
   * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
   * @param executionId the executionId parameter
   * @param watcher the watcher parameter
   * @return the listenablefuture<void> result
   */
  public void cancelOperation(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteOperation(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param operation the operation parameter
   * @return the operation result
   */
  public ListenableFuture<Void> watchExecution(UUID executionId, Watcher watcher) {
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
        log.log(
            Level.SEVERE,
            String.format("error unpacking queued operation metadata from %s", operation.getName()),
            e);
        return null;
      }
    } else {
      return NodeInstance.expectExecuteOperationMetadata(operation);
    }
  }

  /**
   * Retrieves a blob from the Content Addressable Storage
   * @param request the request parameter
   * @return the getclientstarttimeresult result
   */
  public Operation stripOperation(Operation operation) {
    return operation.toBuilder()
        .setMetadata(Any.pack(expectExecuteOperationMetadata(operation)))
        .build();
  }

  @Override
  protected Logger getLogger() {
    return log;
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @return the casindexresults result
   */
  public GetClientStartTimeResult getClientStartTime(GetClientStartTimeRequest request) {
    try {
      return backplane.getClientStartTime(request);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  @Override
  /**
   * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
   * @param name the name parameter
   * @param pageSize the pageSize parameter
   * @param pageToken the pageToken parameter
   * @param filter the filter parameter
   * @param operations the operations parameter
   * @return the string result
   */
  public CasIndexResults reindexCas() {
    try {
      return backplane.reindexCas();
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param workerName the workerName parameter
   */
  public String listOperations(
      String name, int pageSize, String pageToken, String filter, Consumer<Operation> operations) {
    throw new UnsupportedOperationException();
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
