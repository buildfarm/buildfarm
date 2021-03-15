// Copyright 2018 The Bazel Authors. All rights reserved.
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

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.backplane.Backplane;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.shard.ShardInstance.WorkersCallback;
import com.google.common.base.Throwables;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
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
import java.util.logging.Logger;
import javax.annotation.Nullable;

public class RemoteInputStreamFactory implements InputStreamFactory {
  private static final Logger logger = Logger.getLogger(RemoteInputStreamFactory.class.getName());

  public interface UnavailableConsumer {
    void accept(String worker, Throwable t, String context);
  }

  private final @Nullable String publicName;
  private final Backplane backplane;
  private final Random rand;
  private final LoadingCache<String, Instance> workerStubs;
  private final UnavailableConsumer onUnavailable;

  RemoteInputStreamFactory(
      Backplane backplane,
      Random rand,
      LoadingCache<String, Instance> workerStubs,
      UnavailableConsumer onUnavailable) {
    this(/* publicName=*/ null, backplane, rand, workerStubs, onUnavailable);
  }

  public RemoteInputStreamFactory(
      String publicName,
      Backplane backplane,
      Random rand,
      LoadingCache<String, Instance> workerStubs,
      UnavailableConsumer onUnavailable) {
    this.publicName = publicName;
    this.backplane = backplane;
    this.rand = rand;
    this.workerStubs = workerStubs;
    this.onUnavailable = onUnavailable;
  }

  private Instance workerStub(String worker) {
    try {
      return workerStubs.get(worker);
    } catch (ExecutionException e) {
      logger.log(Level.SEVERE, String.format("error getting worker stub for %s", worker), e);
      throw new IllegalStateException("stub instance creation must not fail");
    }
  }

  private InputStream fetchBlobFromRemoteWorker(
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
              blobDigest, offset, deadlineAfter, deadlineAfterUnits, requestMetadata);
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
  public InputStream newInput(Digest blobDigest, long offset)
      throws IOException, InterruptedException {
    return newInput(blobDigest, offset, 60, SECONDS, RequestMetadata.getDefaultInstance());
  }

  public InputStream newInput(
      Digest blobDigest,
      long offset,
      long deadlineAfter,
      TimeUnit deadlineAfterUnits,
      RequestMetadata requestMetadata)
      throws IOException, InterruptedException {
    Set<String> remoteWorkers;
    Set<String> locationSet;
    try {
      Set<String> workers = backplane.getWorkers();
      if (publicName == null) {
        remoteWorkers = workers;
      } else {
        synchronized (workers) {
          remoteWorkers = Sets.difference(workers, ImmutableSet.of(publicName)).immutableCopy();
        }
      }
      locationSet =
          Sets.newHashSet(Sets.intersection(backplane.getBlobLocationSet(blobDigest), workers));
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }

    if (publicName != null && locationSet.remove(publicName)) {
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
          public void onQueue(Deque<String> workers) {
            Set<String> locationSet = Sets.newHashSet(workers);
            boolean complete = false;
            while (!complete && !workers.isEmpty()) {
              try {
                inputStreamFuture.set(
                    fetchBlobFromRemoteWorker(
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
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      Throwables.throwIfUnchecked(cause);
      Throwables.throwIfInstanceOf(cause, IOException.class);
      Throwables.throwIfInstanceOf(cause, InterruptedException.class);
      throw new UncheckedExecutionException(cause);
    }
  }
}
