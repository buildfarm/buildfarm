/**
 * Stores a blob in the Content Addressable Storage
 * @param publicName the publicName parameter
 * @param backplane the backplane parameter
 * @param rand the rand parameter
 * @param workerStubs the workerStubs parameter
 * @param onUnavailable the onUnavailable parameter
 * @return the public result
 */
/**
 * Completion handler for asynchronous operations Executes asynchronously and returns a future for completion tracking.
 * @return the populatedworkerlistfuture,
        new result
 */
// Copyright 2018 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.instance.shard;

import static build.buildfarm.instance.shard.Util.SHARD_IS_RETRIABLE;
import static build.buildfarm.instance.shard.Util.correctMissingBlob;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.util.concurrent.TimeUnit.SECONDS;

import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.backplane.Backplane;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.shard.ServerInstance.WorkersCallback;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.v1test.Digest;
import com.google.common.base.Throwables;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import javax.annotation.Nullable;
import lombok.extern.java.Log;

@Log
public class RemoteInputStreamFactory implements InputStreamFactory {
  public interface UnavailableConsumer {
    void accept(String worker, Throwable t, String context);
  }

  private final @Nullable String publicName;
  private final Backplane backplane;
  private final Random rand;
  private final LoadingCache<String, StubInstance> workerStubs;
  /**
   * Loads data from storage or external source Implements complex logic with 6 conditional branches and 1 iterative operations. Performs side effects including logging and state modifications. Includes input validation and error handling for robustness.
   * @param compressor the compressor parameter
   * @param blobDigest the blobDigest parameter
   * @param workers the workers parameter
   * @param offset the offset parameter
   * @param deadlineAfter the deadlineAfter parameter
   * @param deadlineAfterUnits the deadlineAfterUnits parameter
   * @param requestMetadata the requestMetadata parameter
   * @return the inputstream result
   */
  /**
   * gRPC service client for remote communication Performs side effects including logging and state modifications. Includes input validation and error handling for robustness.
   * @param worker the worker parameter
   * @return the instance result
   */
  private final UnavailableConsumer onUnavailable;

  RemoteInputStreamFactory(
      Backplane backplane,
      Random rand,
      LoadingCache<String, StubInstance> workerStubs,
      UnavailableConsumer onUnavailable) {
    this(/* publicName= */ null, backplane, rand, workerStubs, onUnavailable);
  }

  @SuppressWarnings("NullableProblems")
  public RemoteInputStreamFactory(
      String publicName,
      Backplane backplane,
      Random rand,
      LoadingCache<String, StubInstance> workerStubs,
      UnavailableConsumer onUnavailable) {
    /**
     * Stores a blob in the Content Addressable Storage Implements complex logic with 7 conditional branches and 1 iterative operations. Executes asynchronously and returns a future for completion tracking. Processes 1 input sources and produces 2 outputs. Performs side effects including logging and state modifications. Includes input validation and error handling for robustness.
     * @param compressor the compressor parameter
     * @param blobDigest the blobDigest parameter
     * @param offset the offset parameter
     * @param deadlineAfter the deadlineAfter parameter
     * @param deadlineAfterUnits the deadlineAfterUnits parameter
     * @param requestMetadata the requestMetadata parameter
     * @return the inputstream result
     */
    /**
     * Stores a blob in the Content Addressable Storage
     * @param compressor the compressor parameter
     * @param blobDigest the blobDigest parameter
     * @param offset the offset parameter
     * @return the inputstream result
     */
    this.publicName = publicName;
    this.backplane = backplane;
    this.rand = rand;
    this.workerStubs = workerStubs;
    this.onUnavailable = onUnavailable;
  }

  private Instance workerStub(String worker) {
    try {
      StubInstance stubInstance = workerStubs.get(worker);
      stubInstance.setOnStopped(() -> workerStubs.invalidate(worker));
      return stubInstance;
    } catch (ExecutionException e) {
      log.log(Level.SEVERE, String.format("error getting worker stub for %s", worker), e);
      throw new IllegalStateException("stub instance creation must not fail");
    }
  }

  @SuppressWarnings({
    "ResultOfMethodCallIgnored",
    "StatementWithEmptyBody",
    "PMD.EmptyControlStatement"
  })
  private InputStream fetchBlobFromRemoteWorker(
      Compressor.Value compressor,
      Digest blobDigest,
      Deque<String> workers,
      long offset,
      long deadlineAfter,
      TimeUnit deadlineAfterUnits,
      RequestMetadata requestMetadata)
      throws IOException, InterruptedException {
    String worker = workers.removeFirst();
    try {
      Instance instance = workerStub(worker);

      InputStream input =
          instance.newBlobInput(
              compressor, blobDigest, offset, deadlineAfter, deadlineAfterUnits, requestMetadata);
      // ensure that if the blob cannot be fetched, that we throw here
      input.available();
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
      return input;
    } catch (StatusRuntimeException e) {
      Status st = Status.fromThrowable(e);
      if (st.getCode() == Code.UNAVAILABLE || st.getCode() == Code.UNIMPLEMENTED) {
        // for now, leave this up to schedulers
        onUnavailable.accept(worker, e, "getBlob(" + DigestUtil.toString(blobDigest) + ")");
      } else if (st.getCode() == Code.NOT_FOUND) {
        // ignore this, the worker will update the backplane eventually
      } else if (st.getCode() != Code.DEADLINE_EXCEEDED && SHARD_IS_RETRIABLE.test(st)) {
        // why not, always
        workers.addLast(worker);
      } else if (st.getCode() == Code.CANCELLED) {
        throw new InterruptedException();
      } else {
        throw e;
      }
    }
    throw new NoSuchFileException(DigestUtil.toString(blobDigest));
  }

  @Override
  public InputStream newInput(Compressor.Value compressor, Digest blobDigest, long offset)
      throws IOException {
    return newInput(
        compressor, blobDigest, offset, 60, SECONDS, RequestMetadata.getDefaultInstance());
  }

  public InputStream newInput(
      Compressor.Value compressor,
      Digest blobDigest,
      long offset,
      long deadlineAfter,
      TimeUnit deadlineAfterUnits,
      RequestMetadata requestMetadata)
      throws IOException {
    Set<String> remoteWorkers;
    Set<String> locationSet;
    try {
      remoteWorkers = backplane.getStorageWorkers();
      if (publicName != null) {
        remoteWorkers.remove(publicName);
      }
      locationSet =
          Sets.newHashSet(
              Sets.intersection(backplane.getBlobLocationSet(blobDigest), remoteWorkers));
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }

    if (publicName != null && locationSet.remove(publicName)) {
      /**
       * Performs specialized operation based on method logic Executes asynchronously and returns a future for completion tracking.
       * @param workers the workers parameter
       */
      backplane.removeBlobLocation(blobDigest, publicName);
    }
    List<String> workersList = new ArrayList<>(locationSet);
    boolean emptyWorkerList = workersList.isEmpty();
    final ListenableFuture<List<String>> populatedWorkerListFuture;
    if (emptyWorkerList) {
      populatedWorkerListFuture =
          transform(
              correctMissingBlob(
                  backplane,
                  remoteWorkers,
                  locationSet,
                  this::workerStub,
                  blobDigest,
                  newDirectExecutorService(),
                  requestMetadata),
              (foundOnWorkers) -> {
                Iterables.addAll(workersList, foundOnWorkers);
                return workersList;
              },
              directExecutor());
    } else {
      populatedWorkerListFuture = immediateFuture(workersList);
    }
    SettableFuture<InputStream> inputStreamFuture = SettableFuture.create();
    addCallback(
        populatedWorkerListFuture,
        new WorkersCallback(rand) {
          boolean triedCheck = emptyWorkerList;

          @Override
          /**
           * Performs specialized operation based on method logic
           * @param t the t parameter
           */
          public void onQueue(Deque<String> workers) {
            Set<String> locationSet = Sets.newHashSet(workers);
            boolean complete = false;
            while (!complete && !workers.isEmpty()) {
              try {
                inputStreamFuture.set(
                    fetchBlobFromRemoteWorker(
                        compressor,
                        blobDigest,
                        workers,
                        offset,
                        deadlineAfter,
                        deadlineAfterUnits,
                        requestMetadata));
                complete = true;
              } catch (IOException e) {
                if (workers.isEmpty()) {
                  if (triedCheck) {
                    onFailure(e);
                    return;
                  }
                  triedCheck = true;

                  workersList.clear();
                  ListenableFuture<List<String>> checkedWorkerListFuture =
                      transform(
                          correctMissingBlob(
                              backplane,
                              remoteWorkers,
                              locationSet,
                              RemoteInputStreamFactory.this::workerStub,
                              blobDigest,
                              newDirectExecutorService(),
                              requestMetadata),
                          (foundOnWorkers) -> {
                            Iterables.addAll(workersList, foundOnWorkers);
                            return workersList;
                          },
                          directExecutor());
                  addCallback(checkedWorkerListFuture, this, directExecutor());
                  complete = true;
                }
              } catch (InterruptedException e) {
                complete = true;
                onFailure(e);
              }
            }
          }

          @SuppressWarnings("NullableProblems")
          @Override
          public void onFailure(Throwable t) {
            Status status = Status.fromThrowable(t);
            if (status.getCode() == Code.NOT_FOUND) {
              inputStreamFuture.setException(
                  new NoSuchFileException(DigestUtil.toString(blobDigest)));
            } else {
              inputStreamFuture.setException(t);
            }
          }
        },
        directExecutor());
    try {
      return inputStreamFuture.get();
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      Throwables.throwIfUnchecked(cause);
      Throwables.throwIfInstanceOf(cause, IOException.class);
      if (cause instanceof InterruptedException) {
        throw new IOException(cause);
      }
      throw new UncheckedExecutionException(cause);
    }
  }
}
