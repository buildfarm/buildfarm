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
import build.buildfarm.instance.AbstractServerInstance;
import build.buildfarm.instance.GetDirectoryFunction;
import build.buildfarm.instance.TokenizableIterator;
import build.buildfarm.instance.TreeIterator;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.Retrier;
import build.buildfarm.instance.stub.Retrier.Backoff;
import build.buildfarm.instance.stub.RetryException;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.v1test.CompletedOperationMetadata;
import build.buildfarm.v1test.OperationIteratorToken;
import build.buildfarm.v1test.ShardInstanceConfig;
import build.buildfarm.v1test.QueuedOperationMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
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
import com.google.protobuf.util.JsonFormat;
import io.grpc.Channel;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import javax.naming.ConfigurationException;

public class ShardInstance extends AbstractServerInstance {
  private final ShardInstanceConfig config;
  private final ShardBackplane backplane;
  private final Map<String, StubInstance> workerStubs;
  private final Thread dispatchedMonitor;
  private final Thread completedCollector;
  private final Thread actionCacheSweeper;
  private final ListeningScheduledExecutorService retryScheduler =
      MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(1));
  private final Map<Digest, Directory> directoryCache = new ConcurrentLRUCache<>(64 * 1024);
  private final ConcurrentMap<Digest, Command> commandCache = new ConcurrentLRUCache<>(64 * 1024);
  private final Random rand = new Random();

  public ShardInstance(String name, DigestUtil digestUtil, ShardInstanceConfig config) throws InterruptedException, ConfigurationException {
    super(name, digestUtil, null, null, null, null);
    this.config = config;
    ShardInstanceConfig.BackplaneCase backplaneCase = config.getBackplaneCase();
    switch (backplaneCase) {
      default:
      case BACKPLANE_NOT_SET:
        throw new IllegalArgumentException("Shard Backplane not set in config");
      case REDIS_SHARD_BACKPLANE_CONFIG:
        backplane = new RedisShardBackplane(config.getRedisShardBackplaneConfig(), this::stripOperation, this::stripOperation);
        break;
    }
    workerStubs = new ConcurrentHashMap<>();

    if (config.getRunDispatchedMonitor()) {
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

    if (config.getRunCompletedCollector()) {
      completedCollector = new Thread(new CompletedCollector(
          backplane,
          config.getMaxCompletedOperationsCount()));
    } else {
      completedCollector = null;
    }

    if (config.getRunActionCacheSweeper()) {
      actionCacheSweeper = new Thread(new ActionCacheSweeper(
          backplane,
          this::findMissingBlobs,
          config.getActionCacheSweepPeriod()));
    } else {
      actionCacheSweeper = null;
    }
  }

  @Override
  public void start() {
    backplane.start();
    if (dispatchedMonitor != null) {
      dispatchedMonitor.start();
    }
    if (completedCollector != null) {
      completedCollector.start();
    }
    if (actionCacheSweeper != null) {
      actionCacheSweeper.start();
    }
  }

  @Override
  public void stop() {
    if (actionCacheSweeper != null) {
      actionCacheSweeper.stop();
    }
    if (completedCollector != null) {
      completedCollector.stop();
    }
    if (dispatchedMonitor != null) {
      dispatchedMonitor.stop();
    }
    backplane.stop();
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
        } else if (status.getCode() == Code.CANCELLED && Context.current().isCancelled()) {
          // do nothing further if we're cancelled
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
        } else if (status.getCode() == Code.CANCELLED && Context.current().isCancelled()) {
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

  private ByteString fetchBlobFromWorker(Digest blobDigest, Deque<String> workers, long offset, long limit) throws IOException, InterruptedException {
    String worker = workers.removeFirst();
    try {
      for (;;) {
        try {
          return workerStub(worker).getBlob(blobDigest, offset, limit);
        } catch (RetryException e) {
          Throwable cause = e.getCause();
          if (cause instanceof StatusRuntimeException) {
            throw (StatusRuntimeException) e.getCause();
          }
          throw e;
        }
      }
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      if (status.getCode() == Code.CANCELLED && Context.current().isCancelled()) {
        throw e;
      }
      if (status.getCode() == Code.UNAVAILABLE) {
        removeMalfunctioningWorker(worker, e, "getBlob(" + DigestUtil.toString(blobDigest) + ")");
      } else if (status.getCode() == Code.NOT_FOUND) {
        System.out.println(worker + " did not contain " + DigestUtil.toString(blobDigest));
        // ignore this, the worker will update the backplane eventually
      } else if (status.getCode() == Code.CANCELLED /* yes, gross */ || SHARD_IS_RETRIABLE.test(status)) {
        // why not, always
        workers.addLast(worker);
      } else {
        throw e;
      }
      return null;
    }
  }

  private ByteString getBlobImpl(Digest blobDigest, long offset, long limit, boolean forValidation) throws InterruptedException, IOException {
    List<String> workersList = new ArrayList<>(Sets.intersection(backplane.getBlobLocationSet(blobDigest), backplane.getWorkerSet()));
    Collections.shuffle(workersList, rand);
    Deque<String> workers = new ArrayDeque(workersList);
    boolean triedCheck = false;

    ByteString content;
    do {
      if (workers.isEmpty()) {
        if (forValidation) {
          System.out.println("No location for getBlobImpl(" + DigestUtil.toString(blobDigest) + ", " + offset + ", " + limit + ")");
        }
        if (triedCheck) {
          return null;
        }

        workersList.clear();
        workersList.addAll(checkMissingBlob(blobDigest, true));
        Collections.shuffle(workersList, rand);
        workers = new ArrayDeque(workersList);
        content = null;

        triedCheck = true;
      } else {
        content = fetchBlobFromWorker(blobDigest, workers, offset, limit);
      }
    } while (content == null);

    return content;
  }

  @Override
  protected ByteString getBlob(Digest blobDigest, boolean forValidation) throws InterruptedException, IOException {
    return getBlobImpl(blobDigest, 0, 0, forValidation);
  }

  @Override
  public ByteString getBlob(Digest blobDigest, long offset, long limit) throws InterruptedException, IOException {
    return getBlobImpl(blobDigest, offset, limit, false);
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

  private ByteStreamUploader createStubUploader(Channel channel) {
    return new ByteStreamUploader("", channel, null, 300, createStubRetrier(), retryScheduler);
  }

  private synchronized StubInstance workerStub(String worker) {
    StubInstance instance = workerStubs.get(worker);
    if (instance == null) {
      ManagedChannel channel = createChannel(worker);
      instance = new StubInstance(
          "", digestUtil, channel,
          60 /* FIXME CONFIG */, TimeUnit.SECONDS,
          createStubRetrier(),
          createStubUploader(channel));
      workerStubs.put(worker, instance);
    }
    return instance;
  }

  @Override
  public Digest putBlob(ByteString blob)
      throws IllegalArgumentException, InterruptedException, StatusException {
    for(;;) {
      String worker = null;
      try {
        try {
          worker = backplane.getRandomWorker();
        } catch (IOException e) {
          throw Status.fromThrowable(e).asException();
        }

        if (worker == null) {
          // FIXME should be made into a retry operation, resulting in an IOException
          // FIXME should we wait for a worker to become available?
          throw Status.RESOURCE_EXHAUSTED.asException();
        }
        // System.out.println("putBlob(" + DigestUtil.toString(digestUtil.compute(blob)) + ") => " + worker);
        Digest digest;
        try {
          digest = workerStub(worker).putBlob(blob);
        } catch (RetryException e) {
          if (e.getCause() instanceof StatusRuntimeException) {
            throw (StatusRuntimeException) e.getCause();
          }
          throw e;
        }
        boolean known = false;
        try {
          if (commandCache.get(digest) == null) {
            Command command = Command.parseFrom(blob);
            commandCache.put(digest, command);
          }
          known = true;
        } catch (InvalidProtocolBufferException e) {
          /* not a command */
        }
        if (!known) {
          try {
            if (directoryCache.get(digest) == null) {
              Directory directory = Directory.parseFrom(blob);
              directoryCache.put(digest, directory);
            }
            known = true;
          } catch (InvalidProtocolBufferException e) {
            /* not a directory */
          }
        }
        return digest;
      } catch (StatusRuntimeException e) {
        Status status = Status.fromThrowable(e);
        if (status.getCode() == Code.UNAVAILABLE) {
          removeMalfunctioningWorker(worker, e, "putBlob(" + DigestUtil.toString(digestUtil.compute(blob)) + ")");
        } else if (status.getCode() == Code.CANCELLED && Context.current().isCancelled()) {
          throw e;
        } else {
          e.printStackTrace();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  protected int getTreeDefaultPageSize() { return 1024; }
  protected int getTreeMaxPageSize() { return 1024; }
  protected TokenizableIterator<Directory> createTreeIterator(
      Digest rootDigest, String pageToken) throws InterruptedException, IOException {
    final GetDirectoryFunction getDirectoryFunction;
    Iterable<Directory> directories = backplane.getTree(rootDigest);
    if (directories != null) {
      // FIXME WE MUST VALIDATE THAT THIS DIRECTORIES TREE CONTAINS EVERY SINGLE DIGEST REQUESTED
      getDirectoryFunction = new GetDirectoryFunction() {
        Map<Digest, Directory> index = createDirectoriesIndex(directories);

        @Override
        public Directory apply(Digest digest) {
          Directory directory = index.get(digest);
          if (directory == null) {
            throw new IllegalStateException(
                "not technically illegal, but a get for " + DigestUtil.toString(digest)
                + " was called on the cached tree entry for " + DigestUtil.toString(rootDigest)
                + ", this is probably an error");
          }
          return directory;
        }
      };
    } else {
      getDirectoryFunction = this::recoveryExpectDirectory;
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

  private Directory recoveryExpectDirectory(Digest directoryBlobDigest) throws InterruptedException {
    Directory directory = expectDirectory(directoryBlobDigest);
    if (directory == null) {
      try {
        // should probably prevent the round trip to redis here with the results
        checkMissingBlob(directoryBlobDigest, true);
      } catch (IOException e) {
        e.printStackTrace();
        return null;
      }
    }
    return expectDirectory(directoryBlobDigest);
  }

  @Override
  protected Directory expectDirectory(Digest directoryBlobDigest) throws InterruptedException {
    Directory directory = directoryCache.get(directoryBlobDigest);
    if (directory == null) {
      directory = super.expectDirectory(directoryBlobDigest);
      if (directory != null) {
        directoryCache.put(directoryBlobDigest, directory);
      }
    }
    return directory;
  }

  @Override
  protected Command expectCommand(Digest commandBlobDigest) throws InterruptedException {
    Command command = commandCache.get(commandBlobDigest);
    if (command == null) {
      command = super.expectCommand(commandBlobDigest);
      if (command == null) {
        try {
          // should probably prevent the round trip to redis here with the results
          checkMissingBlob(commandBlobDigest, true);
          command = super.expectCommand(commandBlobDigest);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if (command != null) {
        commandCache.put(commandBlobDigest, command);
      }
    }
    return command;
  }

  private void removeMalfunctioningWorker(String worker, Exception e, String context) {
    StubInstance instance = workerStubs.remove(worker);
    try {
      if (backplane.isWorker(worker)) {
        backplane.removeWorker(worker);

        System.out.println("Removing worker '" + worker + "' during(" + context + ") because of:");
        e.printStackTrace();
      }
    } catch (IOException eIO) {
      throw Status.fromThrowable(eIO).asRuntimeException();
    }

    if (instance != null) {
      ManagedChannel channel = instance.getChannel();
      channel.shutdownNow();
      try {
        channel.awaitTermination(0, TimeUnit.SECONDS);
      } catch (InterruptedException intEx) {
        /* impossible, 0 timeout */
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public OutputStream getStreamOutput(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream newStreamInput(String name) {
    throw new UnsupportedOperationException();
  }

  private Operation buildQueuedOperation(Operation operation, Action action, Command command, Iterable<Directory> directories) {
    QueuedOperationMetadata metadata = createQueuedOperationMetadata(action, command, directories);

    metadata = metadata.toBuilder()
        .setExecuteOperationMetadata(metadata.getExecuteOperationMetadata().toBuilder()
            .setStage(ExecuteOperationMetadata.Stage.QUEUED)
            .build())
        .build();

    return operation.toBuilder()
        .setMetadata(Any.pack(metadata))
        .build();
  }

  private QueuedOperationMetadata createQueuedOperationMetadata(Action action, Command command, Iterable<Directory> directories) {
    return QueuedOperationMetadata.newBuilder()
        .setAction(action)
        .setCommand(command)
        .addAllDirectories(directories)
        .setExecuteOperationMetadata(ExecuteOperationMetadata.newBuilder()
            .setActionDigest(digestUtil.compute(action))
            .build())
        .build();
  }

  protected Operation createOperation(ActionKey actionKey) { throw new UnsupportedOperationException(); }

  private Operation createOperation() {
    String name = createOperationName(UUID.randomUUID().toString());

    return Operation.newBuilder()
        .setName(name)
        .setDone(false)
        .build();
  }

  public boolean validateOperation(String operationName, boolean validateDone) throws IOException, InterruptedException {
    Operation operation = getOperation(operationName);
    if (operation == null) {
      System.out.println("Operation " + operationName + " no longer exists");
      return false;
    }
    if (validateDone && operation.getDone()) {
      System.out.println("Operation " + operation.getName() + " has already completed");
      return false;
    }
    Action action = expectAction(operation);
    if (action == null) {
      System.out.println("Action for " + operation.getName() + " no longer exists");
      return false;
    }
    boolean isQueuedOperationMetadata = operation.getMetadata().is(QueuedOperationMetadata.class);
    Iterable<Directory> directories;
    Command command;
    if (isQueuedOperationMetadata) {
      try {
        QueuedOperationMetadata metadata = operation.getMetadata()
            .unpack(QueuedOperationMetadata.class);
        directories = metadata.getDirectoriesList();
        command = metadata.getCommand();
      } catch (InvalidProtocolBufferException e) {
        e.printStackTrace();
        return false;
      }
    } else {
      directories = getTreeDirectories(action.getInputRootDigest());
      command = expectCommand(action.getCommandDigest());
    }
    try {
      validateAction(action, command, directories);
    } catch (Exception e) {
      // FIXME this should not be a catch all...
      e.printStackTrace();
      return false;
    }
    return true;
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
    boolean isQueuedOperationMetadata = operation.getMetadata().is(QueuedOperationMetadata.class);
    Iterable<Directory> directories;
    Command command;
    if (isQueuedOperationMetadata) {
      try {
        QueuedOperationMetadata metadata = operation.getMetadata()
            .unpack(QueuedOperationMetadata.class);
        directories = metadata.getDirectoriesList();
        command = metadata.getCommand();
      } catch (InvalidProtocolBufferException e) {
        e.printStackTrace();
        return false;
      }
    } else {
      directories = getTreeDirectories(action.getInputRootDigest());
      command = expectCommand(action.getCommandDigest());
    }
    try {
      validateAction(action, command, directories);
    } catch (Exception e) {
      // FIXME this should not be a catch all...
      e.printStackTrace();
      return false;
    }

    if (!isQueuedOperationMetadata) {
      Operation queuedOperation = buildQueuedOperation(
          operation,
          action,
          command,
          directories);
      backplane.putOperation(queuedOperation, ExecuteOperationMetadata.Stage.QUEUED);
    } else {
      backplane.requeueDispatchedOperation(operation);
    }
    return true;
  }

  @Override
  protected ExecuteOperationMetadata expectExecuteOperationMetadata(
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
      return super.expectExecuteOperationMetadata(operation);
    }
  }

  @Override
  public void execute(
      Action action,
      boolean skipCacheLookup,
      int totalInputFileCount,
      long totalInputFileBytes,
      Consumer<Operation> onOperation) throws InterruptedException {
    Iterable<Directory> directories;
    Command command;
    try {
      directories = getTreeDirectories(action.getInputRootDigest());
      command = expectCommand(action.getCommandDigest());
    } catch (IOException e) {
      try {
        System.out.println("execute invalid: " + JsonFormat.printer().print(action));
      } catch (InvalidProtocolBufferException invalidProtoEx) {
        invalidProtoEx.printStackTrace();
      }
      throw new IllegalStateException();
    }

    try {
      validateAction(action, command, directories);
      /* we could probably store this with the action key and just say that every
       * outstanding action has its tree retained */
      backplane.putTree(action.getInputRootDigest(), directories);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    } catch (IllegalStateException e) {
      try {
        System.out.println("execute invalid: " + JsonFormat.printer().print(action));
      } catch (InvalidProtocolBufferException invalidProtoEx) {
        invalidProtoEx.printStackTrace();
      }
      throw e;
    }

    Operation operation = createOperation();

    /*
    try {
      System.out.println("execute(" + operation.getName() + "): " + JsonFormat.printer().print(action));
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
    }
    */

    try {
      backplane.putOperation(operation, ExecuteOperationMetadata.Stage.UNKNOWN);
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }

    onOperation.accept(operation);

    // FIXME lookup

    // make the action available to the worker
    try {
      putBlob(action.toByteString());
    } catch (StatusException e) {
      e.printStackTrace();
      return;
    }

    final Operation queuedOperation = buildQueuedOperation(
        operation,
        action,
        command,
        directories);

    try {
      backplane.putOperation(queuedOperation, ExecuteOperationMetadata.Stage.QUEUED);
    } catch (IOException e) {
      // cleanup? retry?
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  @Override
  public void match(Platform platform, Predicate<Operation> onMatch) {
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

  private Operation stripOperation(Operation operation) {
    return operation.toBuilder()
        .setMetadata(Any.pack(expectExecuteOperationMetadata(operation)))
        .build();
  }
}
