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

package build.buildfarm.worker.shard;

import static build.buildfarm.instance.shard.Util.correctMissingBlob;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.logging.Level.SEVERE;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.common.ShardBackplane;
import build.buildfarm.common.grpc.Retrier;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.shard.ShardInstance.WorkersCallback;
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
import java.nio.file.NoSuchFileException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

class RemoteInputStreamFactory implements InputStreamFactory {
  private static final Logger logger = Logger.getLogger(Worker.class.getName());

  private final String publicName;
  private final ShardBackplane backplane;
  private final Random rand;
  private final LoadingCache<String, Instance> workerStubs;

  RemoteInputStreamFactory(
      String publicName,
      ShardBackplane backplane,
      Random rand,
      LoadingCache<String, Instance> workerStubs) {
    this.publicName = publicName;
    this.backplane = backplane;
    this.rand = rand;
    this.workerStubs = workerStubs;
  }

  private Instance workerStub(String worker) {
    try {
      return workerStubs.get(worker);
    } catch (ExecutionException e) {
      logger.log(SEVERE, "error getting worker stub for " + worker, e);
      throw new IllegalStateException("stub instance creation must not fail");
    }
  }

  private InputStream fetchBlobFromRemoteWorker(Digest blobDigest, Deque<String> workers, long offset) throws IOException, InterruptedException {
    String worker = workers.removeFirst();
    try {
      Instance instance = workerStub(worker);

      InputStream input = instance.newBlobInput(blobDigest, offset);
      // ensure that if the blob cannot be fetched, that we throw here
      input.available();
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
      return input;
    } catch (StatusRuntimeException e) {
      Status st = Status.fromThrowable(e);
      if (st.getCode().equals(Code.UNAVAILABLE)) {
        // for now, leave this up to schedulers
        // removeMalfunctioningWorker(worker, e, "getBlob(" + DigestUtil.toString(blobDigest) + ")");
      } else if (st.getCode() == Code.NOT_FOUND) {
        // ignore this, the worker will update the backplane eventually
      } else if (Retrier.DEFAULT_IS_RETRIABLE.test(st)) {
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
  public InputStream newInput(Digest blobDigest, long offset) throws IOException, InterruptedException {
    Set<String> remoteWorkers;
    Set<String> locationSet;
    try {
      Set<String> workers = backplane.getWorkers();
      remoteWorkers = Sets.difference(workers, ImmutableSet.<String>of(publicName));
      locationSet = Sets.newHashSet(Sets.intersection(backplane.getBlobLocationSet(blobDigest), workers));
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }

    if (locationSet.remove(publicName)) {
      backplane.removeBlobLocation(blobDigest, publicName);
    }
    List<String> workersList = new ArrayList<>(locationSet);
    boolean emptyWorkerList = workersList.isEmpty();
    final ListenableFuture<List<String>> populatedWorkerListFuture;
    if (emptyWorkerList) {
      populatedWorkerListFuture = transform(
          correctMissingBlob(backplane, remoteWorkers, locationSet, this::workerStub, blobDigest, newDirectExecutorService()),
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
                inputStreamFuture.set(fetchBlobFromRemoteWorker(blobDigest, workers, offset));
                complete = true;
              } catch (IOException e) {
                if (workers.isEmpty()) {
                  if (triedCheck) {
                    onFailure(e);
                    return;
                  }
                  triedCheck = true;

                  workersList.clear();
                  try {
                    ListenableFuture<List<String>> checkedWorkerListFuture = transform(
                        correctMissingBlob(backplane, remoteWorkers, locationSet, RemoteInputStreamFactory.this::workerStub, blobDigest, newDirectExecutorService()),
                        (foundOnWorkers) -> {
                          Iterables.addAll(workersList, foundOnWorkers);
                          return workersList;
                        },
                        directExecutor());
                    addCallback(checkedWorkerListFuture, this, directExecutor());
                    complete = true;
                  } catch (IOException checkException) {
                    complete = true;
                    onFailure(checkException);
                  }
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
              inputStreamFuture.setException(new NoSuchFileException(DigestUtil.toString(blobDigest)));
            } else {
              inputStreamFuture.setException(t);
            }
          }
        },
        directExecutor());
    try {
      return inputStreamFuture.get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      if (e.getCause() instanceof InterruptedException) {
        throw (InterruptedException) e.getCause();
      }
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new UncheckedExecutionException(e.getCause());
    }
  }
}
