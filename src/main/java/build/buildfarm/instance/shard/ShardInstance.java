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

package build.buildfarm.instance.shard;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.ShardBackplane;
import build.buildfarm.common.cache.Cache;
import build.buildfarm.common.cache.CacheBuilder;
import build.buildfarm.common.cache.CacheLoader.InvalidCacheLoadException;
import build.buildfarm.instance.AbstractServerInstance;
import build.buildfarm.instance.GetDirectoryFunction;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.TokenizableIterator;
import build.buildfarm.instance.TreeIterator;
import build.buildfarm.instance.TreeIterator.DirectoryEntry;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.Retrier;
import build.buildfarm.instance.stub.Retrier.Backoff;
import build.buildfarm.instance.stub.RetryException;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.v1test.CompletedOperationMetadata;
import build.buildfarm.v1test.OperationIteratorToken;
import build.buildfarm.v1test.ShardInstanceConfig;
import build.buildfarm.v1test.QueuedOperationMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Command;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.OutputFile;
import com.google.devtools.remoteexecution.v1test.Platform;
import com.google.devtools.remoteexecution.v1test.UpdateBlobRequest;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata.Stage;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Channel;
import io.grpc.Context;
import io.grpc.Context.CancellableContext;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.naming.ConfigurationException;

public class ShardInstance extends AbstractServerInstance {
  private final Runnable onStop;
  private final ShardBackplane backplane;
  private final LoadingCache<String, Instance> workerStubs;
  private final Thread dispatchedMonitor;
  private final Cache<Digest, Directory> directoryCache = CacheBuilder.newBuilder()
      .maximumSize(64 * 1024)
      .build();
  private final Cache<Digest, Command> commandCache = CacheBuilder.newBuilder()
      .maximumSize(64 * 1024)
      .build();
  private final com.google.common.cache.RemovalListener<String, Instance> instanceRemovalListener = (removal) -> {
    Instance instance = removal.getValue();
    if (instance instanceof StubInstance) {
      ManagedChannel channel = ((StubInstance) instance).getChannel();
      channel.shutdownNow();
      try {
        channel.awaitTermination(0, TimeUnit.SECONDS);
      } catch (InterruptedException intEx) {
        /* impossible, 0 timeout */
        Thread.currentThread().interrupt();
      }
    }
  };
  private final Random rand = new Random();
  private ExecutorService operationDeletionService = Executors.newSingleThreadExecutor();

  public ShardInstance(String name, DigestUtil digestUtil, ShardInstanceConfig config, Runnable onStop)
      throws InterruptedException, ConfigurationException {
    this(
        name,
        digestUtil,
        getBackplane(config),
        config.getRunDispatchedMonitor(),
        onStop,
        new CacheLoader<String, Instance>() {
          @Override
          public Instance load(String worker) {
          ManagedChannel channel = createChannel(worker);
          return new StubInstance(
              "", digestUtil, channel,
              60 /* FIXME CONFIG */, TimeUnit.SECONDS,
              createStubRetrier(),
              null);
          }
        });
  }

  private static ShardBackplane getBackplane(ShardInstanceConfig config)
      throws ConfigurationException {
    ShardInstanceConfig.BackplaneCase backplaneCase = config.getBackplaneCase();
    switch (backplaneCase) {
      default:
      case BACKPLANE_NOT_SET:
        throw new IllegalArgumentException("Shard Backplane not set in config");
      case REDIS_SHARD_BACKPLANE_CONFIG:
        return new RedisShardBackplane(config.getRedisShardBackplaneConfig(), ShardInstance::stripOperation, ShardInstance::stripOperation);
    }
  }

  public ShardInstance(
      String name,
      DigestUtil digestUtil,
      ShardBackplane backplane,
      boolean runDispatchedMonitor,
      Runnable onStop,
      CacheLoader<String, Instance> instanceLoader)
      throws InterruptedException {
    super(name, digestUtil, null, null, null, null, null);
    this.backplane = backplane;
    this.onStop = onStop;
    backplane.setOnUnsubscribe(this::stop);
    workerStubs = com.google.common.cache.CacheBuilder.newBuilder()
      .removalListener(instanceRemovalListener)
      .build(instanceLoader);

    if (runDispatchedMonitor) {
      dispatchedMonitor = new Thread(new DispatchedMonitor(
          backplane,
          (operationName) -> {
            try {
              return requeueOperation(operationName);
            } catch (IOException e) {
              e.printStackTrace();
              return false;
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              return true;
            }
          },
          (operationName) -> {
            try {
              errorOperation(operationName, Code.FAILED_PRECONDITION);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }));
    } else {
      dispatchedMonitor = null;
    }
  }

  @Override
  public void start() {
    backplane.start();
    if (dispatchedMonitor != null) {
      dispatchedMonitor.start();
    }
  }

  @Override
  public void stop() {
    if (dispatchedMonitor != null) {
      dispatchedMonitor.stop();
    }
    operationDeletionService.shutdown();
    backplane.stop();
    onStop.run();
    try {
      if (!operationDeletionService.awaitTermination(10, TimeUnit.SECONDS)) {
        System.err.println("Could not shut down operation deletion service, some operations may be zombies");
      }
      operationDeletionService.shutdownNow();
    } catch (InterruptedException intEx) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public ActionResult getActionResult(ActionKey actionKey) {
    // System.out.println("getActionResult: " + DigestUtil.toString(actionKey.getDigest()));
    ActionResult actionResult;
    try {
      actionResult = backplane.getActionResult(actionKey);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
    if (actionResult == null) {
      return actionResult;
    }

    // FIXME output dirs
    // FIXME inline content
    Iterable<OutputFile> outputFiles = actionResult.getOutputFilesList();
    Iterable<Digest> outputDigests = Iterables.transform(outputFiles, (outputFile) -> outputFile.getDigest());
    if (Iterables.isEmpty(findMissingBlobs(outputDigests))) {
      return actionResult;
    }

    // some of our outputs are no longer in the CAS, remove the actionResult
    try {
      backplane.removeActionResult(actionKey);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
    return null;
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
  public Iterable<Digest> findMissingBlobs(Iterable<Digest> blobDigests) {
    return findMissingBlobs(blobDigests, false);
  }

  public ImmutableList<String> checkMissingBlob(Digest digest, boolean correct) throws IOException {
    ImmutableList.Builder<String> foundWorkers = new ImmutableList.Builder<>();
    Deque<String> workers;
    Set<String> workerSet;
    try {
      workerSet = backplane.getWorkerSet();
      List<String> workersList = new ArrayList<>(workerSet);
      Collections.shuffle(workersList, rand);
      workers = new ArrayDeque(workersList);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }

    Set<String> originalLocationSet = backplane.getBlobLocationSet(digest);
    System.out.println("ShardInstance::checkMissingBlob(" + DigestUtil.toString(digest) + "): Current set is: " + originalLocationSet);

    for (String worker : workers) {
      try {
        Iterable<Digest> resultDigests = null;
        while (resultDigests == null) {
          try {
            resultDigests = createStubRetrier().execute(() -> workerStub(worker).findMissingBlobs(ImmutableList.<Digest>of(digest)));
            if (Iterables.size(resultDigests) == 0) {
              System.out.println("ShardInstance::checkMissingBlob(" + DigestUtil.toString(digest) + "): Adding back to " + worker);
              if (correct) {
                backplane.addBlobLocation(digest, worker);
              }
              foundWorkers.add(worker);
            }
          } catch (RetryException e) {
            if (e.getCause() instanceof StatusRuntimeException) {
              throw (StatusRuntimeException) e.getCause();
            }
            throw e;
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw Status.CANCELLED.asRuntimeException();
      } catch (IOException e) {
        throw Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException();
      } catch (StatusRuntimeException e) {
        Status status = Status.fromThrowable(e);
        if (status.getCode() == Code.UNAVAILABLE) {
          removeMalfunctioningWorker(worker, e, String.format("checkMissingBlobs(%s)", DigestUtil.toString(digest)));
        } else if (Context.current().isCancelled()) {
          // do nothing further if we're cancelled or timed out
          throw e;
        } else {
          e.printStackTrace();
        }
      }
    }
    ImmutableList<String> found = foundWorkers.build();

    for (String worker : originalLocationSet) {
      if (workerSet.contains(worker) && !found.contains(worker)) {
        System.out.println("ShardInstance::checkMissingBlob(" + DigestUtil.toString(digest) + "): Removing from " + worker);
        if (correct) {
          backplane.removeBlobLocation(digest, worker);
        }
      }
    }
    return found;
  }

  @Override
  protected Iterable<Digest> findMissingBlobs(Iterable<Digest> blobDigests, boolean forValidation) {
    Iterable<Digest> nonEmptyDigests = Iterables.filter(blobDigests, (digest) -> digest.getSizeBytes() > 0);
    if (Iterables.isEmpty(nonEmptyDigests)) {
      return nonEmptyDigests;
    }

    Deque<String> workers;
    try {
      List<String> workersList = new ArrayList<>(backplane.getWorkerSet());
      Collections.shuffle(workersList, rand);
      workers = new ArrayDeque(workersList);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }

    for (String worker : workers) {
      try {
        Iterable<Digest> workerDigests = nonEmptyDigests;
        Iterable<Digest> workerMissingDigests = null;
        while (workerMissingDigests == null) {
          workerMissingDigests = createStubRetrier().execute(() -> workerStub(worker).findMissingBlobs(workerDigests));
        }
        nonEmptyDigests = workerMissingDigests;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw Status.CANCELLED.asRuntimeException();
      } catch (IOException e) {
        e.printStackTrace();
        throw Status.INTERNAL.asRuntimeException();
      } catch (StatusRuntimeException e) {
        Status status = Status.fromThrowable(e);
        if (status.getCode() == Code.UNAVAILABLE) {
          removeMalfunctioningWorker(worker, e, "findMissingBlobs(...)");
        } else if (Context.current().isCancelled()) {
          // do nothing further if we're cancelled
          throw e;
        } else {
          e.printStackTrace();
        }
      }

      if (Iterables.isEmpty(nonEmptyDigests)) {
        break;
      }
    }
    return nonEmptyDigests;
  }

  @Override
  public Iterable<Digest> putAllBlobs(Iterable<ByteString> blobs)
      throws IllegalArgumentException, InterruptedException, StatusException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getBlobName(Digest blobDigest) {
    throw new UnsupportedOperationException();
  }

  private void fetchBlobFromWorker(
      Digest blobDigest,
      Deque<String> workers,
      long offset,
      long limit,
      StreamObserver<ByteString> blobObserver) {
    String worker = workers.removeFirst();
    workerStub(worker).getBlob(blobDigest, offset, limit, new StreamObserver<ByteString>() {
      @Override
      public void onNext(ByteString nextChunk) {
        blobObserver.onNext(nextChunk);
      }

      @Override
      public void onError(Throwable t) {
        Status status = Status.fromThrowable(t);
        if (Context.current().isCancelled()) {
          blobObserver.onError(t);
          return;
        }
        if (status.getCode() == Code.UNAVAILABLE) {
          removeMalfunctioningWorker(worker, t, "getBlob(" + DigestUtil.toString(blobDigest) + ")");
        } else if (status.getCode() == Code.NOT_FOUND) {
          System.out.println(worker + " did not contain " + DigestUtil.toString(blobDigest));
          // ignore this, the worker will update the backplane eventually
        } else if (status.getCode() == Code.CANCELLED /* yes, gross */ || SHARD_IS_RETRIABLE.test(status)) {
          // why not, always
          workers.addLast(worker);
        } else {
          blobObserver.onError(t);
          return;
        }

        if (workers.isEmpty()) {
          blobObserver.onError(Status.NOT_FOUND.asException());
        } else {
          fetchBlobFromWorker(blobDigest, workers, offset, limit, blobObserver);
        }
      }

      @Override
      public void onCompleted() {
        blobObserver.onCompleted();
      }
    });
  }

  private void getBlobImpl(Digest blobDigest, long offset, long limit, StreamObserver<ByteString> blobObserver) {
    List<String> workersList;
    try {
      workersList = new ArrayList<>(Sets.intersection(backplane.getBlobLocationSet(blobDigest), backplane.getWorkerSet()));
    } catch (IOException e) {
      blobObserver.onError(e);
      return;
    }
    Collections.shuffle(workersList, rand);
    Deque<String> workers = new ArrayDeque(workersList);

    fetchBlobFromWorker(blobDigest, workers, offset, limit, new StreamObserver<ByteString>() {
      boolean triedCheck = false;

      @Override
      public void onNext(ByteString nextChunk) {
        blobObserver.onNext(nextChunk);
      }

      @Override
      public void onError(Throwable t) {
        Status status = Status.fromThrowable(t);
        if (status.getCode() == Code.NOT_FOUND && !triedCheck) {
          triedCheck = true;
          workersList.clear();
          try {
            workersList.addAll(checkMissingBlob(blobDigest, true));
          } catch (IOException e) {
            blobObserver.onError(e);
            return;
          }
          if (!workersList.isEmpty()) {
            Collections.shuffle(workersList, rand);
            final Deque<String> workers = new ArrayDeque(workersList);
            fetchBlobFromWorker(blobDigest, workers, offset, limit, this);
            return;
          }
        }
        blobObserver.onError(t);
      }

      @Override
      public void onCompleted() {
        blobObserver.onCompleted();
      }
    });
  }

  @Override
  public ByteString getBlob(Digest blobDigest, long offset, long limit) throws IOException, InterruptedException {
    SettableFuture<ByteString> blobFuture = SettableFuture.create();
    getBlobImpl(blobDigest, offset, limit, new StreamObserver<ByteString>() {
      ByteString content = ByteString.EMPTY;

      @Override
      public void onNext(ByteString nextChunk) {
        content = content.concat(nextChunk);
      }

      @Override
      public void onError(Throwable t) {
        blobFuture.setException(t);
      }

      @Override
      public void onCompleted() {
        blobFuture.set(content);
      }
    });
    try {
      return blobFuture.get();
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
      throw new IOException(e.getCause());
    }
  }

  @Override
  public void getBlob(Digest blobDigest, long offset, long limit, StreamObserver<ByteString> blobObserver) {
    getBlobImpl(blobDigest, offset, limit, blobObserver);
  }

  private static ManagedChannel createChannel(String target) {
    NettyChannelBuilder builder =
        NettyChannelBuilder.forTarget(target)
            .negotiationType(NegotiationType.PLAINTEXT);
    return builder.build();
  }

  private static final com.google.common.base.Predicate<Status> SHARD_IS_RETRIABLE =
      st -> st.getCode() != Code.CANCELLED && Retrier.DEFAULT_IS_RETRIABLE.test(st);

  private static Retrier createStubRetrier() {
    return new Retrier(
        Backoff.exponential(
            Duration.ofMillis(/*options.experimentalRemoteRetryStartDelayMillis=*/ 100),
            Duration.ofMillis(/*options.experimentalRemoteRetryMaxDelayMillis=*/ 5000),
            /*options.experimentalRemoteRetryMultiplier=*/ 2,
            /*options.experimentalRemoteRetryJitter=*/ 0.1,
            /*options.experimentalRemoteRetryMaxAttempts=*/ 5),
        SHARD_IS_RETRIABLE);
  }

  private static ByteStreamUploader createStubUploader(Channel channel, ListeningScheduledExecutorService service) {
    return new ByteStreamUploader("", channel, null, 300, createStubRetrier(), service);
  }

  private synchronized Instance workerStub(String worker) {
    try {
      return workerStubs.get(worker);
    } catch (ExecutionException e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public Digest putBlob(ByteString blob)
      throws IllegalArgumentException, InterruptedException {
    for(;;) {
      String worker = null;
      try {
        try {
          worker = backplane.getRandomWorker();
        } catch (IOException e) {
          throw Status.fromThrowable(e).asRuntimeException();
        }

        if (worker == null) {
          // FIXME should be made into a retry operation, resulting in an IOException
          // FIXME should we wait for a worker to become available?
          throw Status.RESOURCE_EXHAUSTED.asRuntimeException();
        }
        // System.out.println("putBlob(" + DigestUtil.toString(digestUtil.compute(blob)) + ") => " + worker);
        Digest digest;
        try {
          digest = workerStub(worker).putBlob(blob);
        } catch (StatusException e) {
          throw Status.fromThrowable(e).asRuntimeException();
        } catch (RetryException e) {
          if (e.getCause() instanceof StatusRuntimeException) {
            throw (StatusRuntimeException) e.getCause();
          }
          throw e;
        }
        boolean known = false;
        try {
          commandCache.get(digest, new Callable<Command>() {
            @Override
            public Command call() throws IOException {
              return Command.parseFrom(blob);
            }
          }).get();
          known = true;
        } catch (ExecutionException e) {
          /* not a command */
        }
        if (!known) {
          try {
            directoryCache.get(digest, new Callable<Directory>() {
              @Override
              public Directory call() throws IOException {
                return Directory.parseFrom(blob);
              }
            }).get();
            known = true;
          } catch (ExecutionException e) {
            /* not a directory */
          }
        }
        return digest;
      } catch (StatusRuntimeException e) {
        Status status = Status.fromThrowable(e);
        if (status.getCode() == Code.UNAVAILABLE) {
          removeMalfunctioningWorker(worker, e, "putBlob(" + DigestUtil.toString(digestUtil.compute(blob)) + ")");
        } else if (Context.current().isCancelled()) {
          throw e;
        } else {
          e.printStackTrace();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  void updateCaches(Digest digest, ByteString blob) throws InterruptedException {
    boolean known = false;
    try {
      commandCache.get(digest, new Callable<Command>() {
        @Override
        public Command call() throws IOException {
          return Command.parseFrom(blob);
        }
      }).get();
      known = true;
    } catch (ExecutionException e) {
      /* not a command */
    }
    if (!known) {
      try {
        directoryCache.get(digest, new Callable<Directory>() {
          @Override
          public Directory call() throws IOException {
            return Directory.parseFrom(blob);
          }
        }).get();
        known = true;
      } catch (ExecutionException e) {
        /* not a directory */
      }
    }
  }

  CommittingOutputStream createBlobOutputStream(Digest blobDigest) throws IOException {
    // the name might very well be how bytestream is supposed to resume uploads...
    Instance instance = workerStub(backplane.getRandomWorker());
    String resourceName = String.format(
        "uploads/%s/blobs/%s",
        UUID.randomUUID(), DigestUtil.toString(blobDigest));
    return instance.getStreamOutput(resourceName, blobDigest.getSizeBytes());
  }

  private class FailedChunkObserver implements ChunkObserver {
    ListenableFuture<Long> failedFuture;

    FailedChunkObserver(Throwable t) {
      failedFuture = Futures.immediateFailedFuture(t);
    }

    @Override
    public ListenableFuture<Long> getCommittedFuture() {
      return failedFuture;
    }

    @Override
    public long getCommittedSize() {
      return 0l;
    }

    @Override
    public void reset() {
    }

    @Override
    public void onNext(ByteString chunk) {
    }

    @Override
    public void onCompleted() {
    }

    @Override
    public void onError(Throwable t) {
      System.err.println("Received onError in failed chunkObserver");
      t.printStackTrace();
    }
  }

  public ChunkObserver getWriteBlobObserver(Digest blobDigest) {
    final CommittingOutputStream initialOutput;
    try {
      initialOutput = createBlobOutputStream(blobDigest);
    } catch (IOException e) {
      return new FailedChunkObserver(e);
    }

    ChunkObserver chunkObserver = new ChunkObserver() {
      long committedSize = 0l;
      ByteString smallBlob = ByteString.EMPTY;
      CommittingOutputStream out = initialOutput;

      @Override
      public long getCommittedSize() {
        return committedSize;
      }

      @Override
      public ListenableFuture<Long> getCommittedFuture() {
        return out.getCommittedFuture();
      }

      @Override
      public void reset() {
        try {
          out.close();
          out = createBlobOutputStream(blobDigest);
        } catch (IOException e) {
          throw Status.INTERNAL.withCause(e).asRuntimeException();
        }
        committedSize = 0;
        smallBlob = ByteString.EMPTY;
      }

      @Override
      public void onNext(ByteString chunk) {
        try {
          chunk.writeTo(out);
          committedSize += chunk.size();
          if (smallBlob != null && committedSize < 4 * 1024 * 1024) {
            smallBlob = smallBlob.concat(chunk);
          } else {
            smallBlob = null;
          }
        } catch (IOException e) {
          throw Status.INTERNAL.withCause(e).asRuntimeException();
        }
      }

      @Override
      public void onCompleted() {
        try {
          // need to cancel the request if size or digest doesn't match
          out.close();
        } catch (IOException e) {
          throw Status.INTERNAL.withCause(e).asRuntimeException();
        }

        try {
          if (smallBlob != null) {
            updateCaches(blobDigest, smallBlob);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw Status.CANCELLED.withCause(e).asRuntimeException();
        }
      }

      @Override
      public void onError(Throwable t) {
        try {
          out.close(); // yup.
        } catch (IOException e) {
          // ignore?
        }
      }
    };
    Context.current().addListener((context) -> {
      chunkObserver.onError(Status.CANCELLED.asRuntimeException());
    }, MoreExecutors.directExecutor());
    return chunkObserver;
  }

  protected int getTreeDefaultPageSize() { return 1024; }
  protected int getTreeMaxPageSize() { return 1024; }
  protected TokenizableIterator<DirectoryEntry> createTreeIterator(
      Digest rootDigest, String pageToken) throws IOException, InterruptedException {
    final Function<Digest, ListenableFuture<Directory>> getDirectoryFunction;
    Iterable<Directory> directories = backplane.getTree(rootDigest);
    if (directories != null) {
      // FIXME WE MUST VALIDATE THAT THIS DIRECTORIES TREE CONTAINS EVERY SINGLE DIGEST REQUESTED
      getDirectoryFunction = new Function<Digest, ListenableFuture<Directory>>() {
        Map<Digest, Directory> index = createDirectoriesIndex(directories);

        @Override
        public ListenableFuture<Directory> apply(Digest digest) {
          Directory directory = index.get(digest);
          if (directory == null) {
            throw new IllegalStateException(
                "not technically illegal, but a get for " + DigestUtil.toString(digest)
                + " was called on the cached tree entry for " + DigestUtil.toString(rootDigest)
                + ", this is probably an error");
          }
          return Futures.immediateFuture(directory);
        }
      };
    } else {
      getDirectoryFunction = (directoryBlobDigest) -> expectDirectory(directoryBlobDigest);
    }
    return new TreeIterator(getDirectoryFunction, rootDigest, pageToken);
  }

  @Override
  protected Action expectAction(Operation operation) throws InterruptedException {
    if (operation.getMetadata().is(QueuedOperationMetadata.class)) {
      try {
        QueuedOperationMetadata metadata = operation.getMetadata()
            .unpack(QueuedOperationMetadata.class);
        return metadata.getAction();
      } catch (InvalidProtocolBufferException e) {
        return null;
      }
    }
    return super.expectAction(operation);
  }

  interface Fetcher<T> {
    T fetch() throws InterruptedException;
  }

  @Override
  protected ListenableFuture<Directory> expectDirectory(Digest directoryBlobDigest) {
    final Fetcher<Directory> fetcher = () -> getUnchecked(super.expectDirectory(directoryBlobDigest));
    // is there a better interface to use for the cache with these nice futures?
    return directoryCache.get(directoryBlobDigest, new Callable<Directory>() {
      @Override
      public Directory call() throws InterruptedException {
        return fetcher.fetch();
      }
    });
  }

  @Override
  protected ListenableFuture<Command> expectCommand(Digest commandBlobDigest) {
    final Fetcher<Command> fetcher = () -> getUnchecked(super.expectCommand(commandBlobDigest));
    return Futures.catching(
        commandCache.get(commandBlobDigest, new Callable<Command>() {
          @Override
          public Command call() throws InterruptedException {
            return fetcher.fetch();
          }
        }),
        InvalidCacheLoadException.class,
        (e) -> { return null; });
  }

  private void removeMalfunctioningWorker(String worker, Throwable t, String context) {
    try {
      if (backplane.isWorker(worker)) {
        backplane.removeWorker(worker);

        System.out.println("Removed worker '" + worker + "' during(" + context + ") because of:");
        t.printStackTrace();
      }
    } catch (IOException eIO) {
      throw Status.fromThrowable(eIO).asRuntimeException();
    }

    workerStubs.invalidate(worker);
  }

  @Override
  public CommittingOutputStream getStreamOutput(String name, long expectedSize) {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream newStreamInput(String name, long offset) {
    throw new UnsupportedOperationException();
  }

  private ListenableFuture<QueuedOperationMetadata> buildQueuedOperationMetadata(Action action, Executor executor) {
    QueuedOperationMetadata.Builder queuedBuilder = QueuedOperationMetadata.newBuilder()
        .setAction(action)
        .setExecuteOperationMetadata(ExecuteOperationMetadata.newBuilder()
            .setActionDigest(digestUtil.compute(action))
            .build());
    return Futures.transform(
        Futures.allAsList(
            Futures.transform(insistCommand(action.getCommandDigest()), queuedBuilder::setCommand, executor),
            Futures.transform(getTreeDirectories(action.getInputRootDigest()), queuedBuilder::addAllDirectories, executor)),
        (result) -> queuedBuilder.build());
  }

  protected Operation createOperation(ActionKey actionKey) { throw new UnsupportedOperationException(); }

  private Operation createOperation(Message metadata) {
    String name = createOperationName(UUID.randomUUID().toString());

    return Operation.newBuilder()
        .setName(name)
        .setDone(false)
        .setMetadata(Any.pack(metadata))
        .build();
  }

  private boolean requeueOperation(String operationName) throws IOException, InterruptedException {
    Operation operation = getOperation(operationName);
    if (operation == null) {
      System.out.println("Operation " + operationName + " no longer exists");
      return false;
    }
    if (operation.getDone()) {
      System.out.println("Operation " + operation.getName() + " has already completed");
      return false;
    }
    Action action = expectAction(operation);
    if (action == null) {
      System.out.println("Action for " + operation.getName() + " no longer exists");
      return false;
    }
    final QueuedOperationMetadata metadata;
    boolean isQueuedOperationMetadata = operation.getMetadata().is(QueuedOperationMetadata.class);
    if (operation.getMetadata().is(QueuedOperationMetadata.class)) {
      try {
        metadata = operation.getMetadata().unpack(QueuedOperationMetadata.class);
      } catch (InvalidProtocolBufferException e) {
        e.printStackTrace();
        return false;
      }
    } else {
      try {
        metadata = buildQueuedOperationMetadata(action, MoreExecutors.directExecutor()).get();
      } catch (ExecutionException e) {
        e.printStackTrace();
        return false;
      }
    }
    try {
      validateQueuedOperationMetadata(metadata);
    } catch (Exception e) {
      // FIXME this should not be a catch all...
      e.printStackTrace();
      return false;
    }

    if (!isQueuedOperationMetadata) {
      Operation queuedOperation = operation.toBuilder()
          .setMetadata(Any.pack(metadata))
          .build();
      backplane.putOperation(queuedOperation, ExecuteOperationMetadata.Stage.QUEUED);
    } else {
      backplane.requeueDispatchedOperation(operation);
    }
    return true;
  }

  protected static ExecuteOperationMetadata expectExecuteOperationMetadata(
      Operation operation) {
    if (operation.getMetadata().is(QueuedOperationMetadata.class)) {
      try {
        return operation.getMetadata().unpack(QueuedOperationMetadata.class).getExecuteOperationMetadata();
      } catch(InvalidProtocolBufferException e) {
        e.printStackTrace();
        return null;
      }
    } else if (operation.getMetadata().is(CompletedOperationMetadata.class)) {
      try {
        return operation.getMetadata().unpack(CompletedOperationMetadata.class).getExecuteOperationMetadata();
      } catch(InvalidProtocolBufferException e) {
        e.printStackTrace();
        return null;
      }
    } else {
      return AbstractServerInstance.expectExecuteOperationMetadata(operation);
    }
  }

  @Override
  public ListenableFuture<Operation> execute(Action action, boolean skipCacheLookup) {
    try {
      if (backplane.canQueue()) {
        return queue(action, skipCacheLookup);
      }
    } catch (IOException e) {
      return Futures.immediateFailedFuture(e);
    }
    return Futures.immediateFailedFuture(Status.RESOURCE_EXHAUSTED.withDescription("cannot currently queue for execution").asRuntimeException());
  }

  @VisibleForTesting
  public ListenableFuture<Operation> queue(Action action, boolean skipCacheLookup) {
    ListenableFuture<QueuedOperationMetadata> queuedFuture = buildQueuedOperationMetadata(action, MoreExecutors.directExecutor());
    ListenableFuture<QueuedOperationMetadata> validatedFuture = Futures.transformAsync(
        queuedFuture,
        (metadata) -> {
          validateQueuedOperationMetadata(metadata);
          return queuedFuture;
        });
    ListenableFuture<Operation> operationFuture = Futures.transform(validatedFuture, this::createOperation);

    SettableFuture<Operation> executeFuture = SettableFuture.create();
    Futures.addCallback(operationFuture, new FutureCallback<Operation>() {
      @Override
      public void onSuccess(Operation operation) {
        operation = stripOperation(operation);
        try {
          // we can't respond until they could watch the thing
          backplane.putOperation(operation, ExecuteOperationMetadata.Stage.UNKNOWN);
          executeFuture.set(operation);
        } catch (IOException e) {
          e.printStackTrace();
          onFailure(e);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        executeFuture.setException(t);
      }
    });

    CancellableContext withCancellation = Context.ROOT.withCancellation();
    withCancellation.run(() -> {
      // nice to have for requeuing
      Futures.addCallback(validatedFuture, new FutureCallback<QueuedOperationMetadata>() {
        @Override
        public void onSuccess(QueuedOperationMetadata metadata) {
          try {
            backplane.putTree(action.getInputRootDigest(), metadata.getDirectoriesList());
          } catch (IOException e) {
            e.printStackTrace();
          }
        }

        @Override
        public void onFailure(Throwable t) {
        }
      });

      ByteString actionBlob = action.toByteString();
      Digest actionDigest = digestUtil.compute(actionBlob);
      ListenableFuture<Long> actionCommittedFuture = Futures.transformAsync(validatedFuture, (metadata) -> {
        CommittingOutputStream out = createBlobOutputStream(actionDigest);
        actionBlob.writeTo(out);
        out.close(); // unnecessary given implementation
        return out.getCommittedFuture();
      });

      // onQueue call?
      Futures.addCallback(
          // need to have for requeuing
          Futures.transformAsync(actionCommittedFuture, (committedSize) -> operationFuture),
          new FutureCallback<Operation>() {
            @Override
            public void onSuccess(Operation operation) {
              // if we ever do contexts here, we will need to do the right thing and make it withCancellation
              try {
                backplane.putOperation(operation, ExecuteOperationMetadata.Stage.QUEUED);
              } catch (IOException e) {
                deleteOperation(operation.getName());
              }
            }

            void deleteOperation(String operationName) {
              operationDeletionService.execute(new Runnable() {
                // we must make all efforts to delete this thing
                int attempt = 1;

                @Override
                public void run() {
                  try {
                    backplane.deleteOperation(operationName);
                  } catch (IOException e) {
                    if (attempt % 100 == 0) {
                      System.err.println(String.format("On attempt %d to delete %s: %s", attempt, operationName, e.getLocalizedMessage()));
                    }
                    // hopefully as deferred execution...
                    operationDeletionService.execute(this);
                    attempt++;
                  }
                }
              });
            }

            @Override
            public void onFailure(Throwable t) {
              try {
                deleteOperation(executeFuture.get().getName());
              } catch (ExecutionException e) {
                // we never succesfully registered the operation
              } catch (InterruptedException e) {
                // nothing
              }
            }
        });
    });
    return executeFuture;
  }

  @Override
  public void match(Platform platform, MatchListener listener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean putOperation(Operation operation) throws InterruptedException {
    if (isErrored(operation)) {
      try {
        Action action = expectAction(operation);
        if (action != null) {
          backplane.removeTree(action.getInputRootDigest());
        }
        return backplane.putOperation(operation, ExecuteOperationMetadata.Stage.COMPLETED);
      } catch (IOException e) {
        throw Status.fromThrowable(e).asRuntimeException();
      }
    }
    throw new UnsupportedOperationException();
  }

  protected boolean matchOperation(Operation operation) { throw new UnsupportedOperationException(); }
  protected void enqueueOperation(Operation operation) { throw new UnsupportedOperationException(); }
  protected Object operationLock(String operationName) { throw new UnsupportedOperationException(); }

  @Override
  public boolean pollOperation(String operationName, Stage stage) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected int getListOperationsDefaultPageSize() { return 1024; }

  @Override
  protected int getListOperationsMaxPageSize() { return 1024; }

  @Override
  protected TokenizableIterator<Operation> createOperationsIterator(String pageToken) {
    Iterator<Operation> iter;
    try {
      iter = Iterables.transform(
          backplane.getOperations(),
          (operationName) -> {
            try {
              return backplane.getOperation(operationName);
            } catch (IOException e) {
              throw Status.fromThrowable(e).asRuntimeException();
            }
          }).iterator();
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
    OperationIteratorToken token;
    if (!pageToken.isEmpty()) {
      try {
        token = OperationIteratorToken.parseFrom(
            BaseEncoding.base64().decode(pageToken));
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalArgumentException();
      }
      boolean paged = false;
      while (iter.hasNext() && !paged) {
        paged = iter.next().getName().equals(token.getOperationName());
      }
    } else {
      token = null;
    }
    return new TokenizableIterator<Operation>() {
      private OperationIteratorToken nextToken = token;

      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public Operation next() {
        Operation operation = iter.next();
        nextToken = OperationIteratorToken.newBuilder()
            .setOperationName(operation.getName())
            .build();
        return operation;
      }

      @Override
      public String toNextPageToken() {
        if (hasNext()) {
          return BaseEncoding.base64().encode(nextToken.toByteArray());
        }
        return "";
      }
    };
  }

  @Override
  public Operation getOperation(String name) {
    try {
      return backplane.getOperation(name);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  @Override
  public void deleteOperation(String name) {
    try {
      backplane.deleteOperation(name);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  @Override
  public boolean watchOperation(
      String operationName,
      boolean watchInitialState,
      Predicate<Operation> watcher) {
    if (watchInitialState) {
      Operation operation = getOperation(operationName);
      if (!watcher.test(operation)) {
        return false;
      }
      if (operation == null || operation.getDone()) {
        return true;
      }
    }

    try {
      return backplane.watchOperation(operationName, watcher);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  private static Operation stripOperation(Operation operation) {
    return operation.toBuilder()
        .setMetadata(Any.pack(expectExecuteOperationMetadata(operation)))
        .build();
  }
}
