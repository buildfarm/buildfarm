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

import static build.buildfarm.instance.shard.Util.correctMissingBlob;
import static build.buildfarm.worker.CASFileCache.getOrIOException;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

import build.buildfarm.common.ContentAddressableStorage;
import build.buildfarm.common.ContentAddressableStorage.Blob;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.ShardBackplane;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.Instance.MatchListener;
import build.buildfarm.instance.memory.MemoryLRUContentAddressableStorage;
import build.buildfarm.instance.shard.RedisShardBackplane;
import build.buildfarm.instance.shard.ShardInstance.WorkersCallback;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.Retrier;
import build.buildfarm.instance.stub.Retrier.Backoff;
import build.buildfarm.instance.stub.RetryException;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.server.InstanceNotFoundException;
import build.buildfarm.server.Instances;
import build.buildfarm.server.ContentAddressableStorageService;
import build.buildfarm.server.ByteStreamService;
import build.buildfarm.worker.CASFileCache;
import build.buildfarm.worker.Dirent;
import build.buildfarm.worker.ExecuteActionStage;
import build.buildfarm.worker.FuseCAS;
import build.buildfarm.worker.InputFetchStage;
import build.buildfarm.worker.InputStreamFactory;
import build.buildfarm.worker.MatchStage;
import build.buildfarm.worker.OutputDirectory;
import build.buildfarm.worker.OutputStreamFactory;
import build.buildfarm.worker.Pipeline;
import build.buildfarm.worker.PipelineStage;
import build.buildfarm.worker.Poller;
import build.buildfarm.worker.PutOperationStage;
import build.buildfarm.worker.ReportResultStage;
import build.buildfarm.worker.UploadManifest;
import build.buildfarm.worker.WorkerContext;
import build.buildfarm.v1test.CASInsertionPolicy;
import build.buildfarm.v1test.QueuedOperationMetadata;
import build.buildfarm.v1test.ShardWorkerConfig;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.devtools.common.options.OptionsParser;
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.DirectoryNode;
import com.google.devtools.remoteexecution.v1test.FileNode;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata.Stage;
import com.google.devtools.remoteexecution.v1test.OutputFile;
import com.google.devtools.remoteexecution.v1test.Tree;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import io.grpc.Channel;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.naming.ConfigurationException;

public class Worker {
  private static final Logger nettyLogger = Logger.getLogger("io.grpc.netty");

  private final ShardWorkerConfig config;
  private final ShardWorkerInstance instance;
  private final Server server;
  private final Path root;
  private final DigestUtil digestUtil;
  private final InputStreamFactory storageInputStreamFactory;
  private final FuseCAS fuseCAS;
  private final CASFileCache fileCache;
  private final Pipeline pipeline;
  private final ShardBackplane backplane;
  private final Map<String, StubInstance> workerStubs;
  private final Map<Path, Iterable<Path>> rootInputFiles = new ConcurrentHashMap<>();
  private final Map<Path, Iterable<Digest>> rootInputDirectories = new ConcurrentHashMap<>();
  private final ListeningScheduledExecutorService retryScheduler =
      MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(1));
  private final Set<String> activeOperations = new ConcurrentSkipListSet<>();
  private static final int shutdownWaitTimeInMillis = 10000;

  public Worker(ShardWorkerConfig config) throws ConfigurationException {
    this(ServerBuilder.forPort(config.getPort()), config);
  }

  private static Path getValidRoot(ShardWorkerConfig config) throws ConfigurationException {
    String rootValue = config.getRoot();
    if (Strings.isNullOrEmpty(rootValue)) {
      throw new ConfigurationException("root value in config missing");
    }
    Path root = Paths.get(rootValue);
    if (!Files.isDirectory(root)) {
      throw new ConfigurationException("root [" + root.toString() + "] is not directory");
    }
    return root;
  }

  private static Path getValidCasCacheDirectory(ShardWorkerConfig config, Path root) throws ConfigurationException {
    String casCacheValue = config.getCasCacheDirectory();
    if (Strings.isNullOrEmpty(casCacheValue)) {
      throw new ConfigurationException("Cas cache directory value in config missing");
    }
    return root.resolve(casCacheValue);
  }

  private static HashFunction getValidHashFunction(ShardWorkerConfig config) throws ConfigurationException {
    try {
      return HashFunction.get(config.getHashFunction());
    } catch (IllegalArgumentException e) {
      throw new ConfigurationException("hash_function value unrecognized");
    }
  }

  private static ManagedChannel createChannel(String target) {
    NettyChannelBuilder builder =
        NettyChannelBuilder.forTarget(target)
            .negotiationType(NegotiationType.PLAINTEXT);
    return builder.build();
  }

  private static Retrier createStubRetrier() {
    return new Retrier(
        Backoff.exponential(
            java.time.Duration.ofMillis(/*options.experimentalRemoteRetryStartDelayMillis=*/ 100),
            java.time.Duration.ofMillis(/*options.experimentalRemoteRetryMaxDelayMillis=*/ 5000),
            /*options.experimentalRemoteRetryMultiplier=*/ 2,
            /*options.experimentalRemoteRetryJitter=*/ 0.1,
            /*options.experimentalRemoteRetryMaxAttempts=*/ 5),
        Retrier.DEFAULT_IS_RETRIABLE);
  }

  private static Retrier createBackplaneRetrier() {
    return new Retrier(
        Backoff.exponential(
              java.time.Duration.ofMillis(/*options.experimentalRemoteRetryStartDelayMillis=*/ 100),
              java.time.Duration.ofMillis(/*options.experimentalRemoteRetryMaxDelayMillis=*/ 5000),
              /*options.experimentalRemoteRetryMultiplier=*/ 2,
              /*options.experimentalRemoteRetryJitter=*/ 0.1,
              /*options.experimentalRemoteRetryMaxAttempts=*/ 5),
          Retrier.REDIS_IS_RETRIABLE);
    }

  private ByteStreamUploader createStubUploader(Channel channel) {
    return new ByteStreamUploader("", channel, null, 300, createStubRetrier(), retryScheduler);
  }

  private StubInstance workerStub(String worker) {
    StubInstance instance = workerStubs.get(worker);
    if (instance == null) {
      ManagedChannel channel = createChannel(worker);
      instance = new StubInstance(
          "", digestUtil, channel,
          10 /* FIXME CONFIG */, TimeUnit.SECONDS,
          createStubRetrier(),
          createStubUploader(channel));
      workerStubs.put(worker, instance);
    }
    return instance;
  }

  private void fetchInputs(
      Path execDir,
      Digest directoryDigest,
      Map<Digest, Directory> directoriesIndex,
      OutputDirectory outputDirectory,
      ImmutableList.Builder<Path> inputFiles,
      ImmutableList.Builder<Digest> inputDirectories)
      throws IOException, InterruptedException {
    Directory directory = directoriesIndex.get(directoryDigest);
    if (directory == null) {
      throw new IOException("Directory " + DigestUtil.toString(directoryDigest) + " is not in directories index");
    }

    for (FileNode fileNode : directory.getFilesList()) {
      Path execPath = execDir.resolve(fileNode.getName());
      Path fileCacheKey = fileCache.put(fileNode.getDigest(), fileNode.getIsExecutable(), /* containingDirectory=*/ null);
      inputFiles.add(fileCacheKey);
      Files.createLink(execPath, fileCacheKey);
    }

    for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
      Digest digest = directoryNode.getDigest();
      String name = directoryNode.getName();
      OutputDirectory childOutputDirectory = outputDirectory != null
          ? outputDirectory.getChild(name) : null;
      Path dirPath = execDir.resolve(name);
      if (childOutputDirectory != null || !config.getLinkInputDirectories()) {
        Files.createDirectories(dirPath);
        fetchInputs(dirPath, digest, directoriesIndex, childOutputDirectory, inputFiles, inputDirectories);
      } else {
        linkDirectory(dirPath, digest, directoriesIndex);
        inputDirectories.add(digest);
      }
    }
  }

  private void linkDirectory(
      Path execPath,
      Digest digest,
      Map<Digest, Directory> directoriesIndex) throws IOException, InterruptedException {
    Path cachePath = fileCache.putDirectory(digest, directoriesIndex);
    Files.createSymbolicLink(execPath, cachePath);
  }

  private Operation stripOperation(Operation operation) {
    return instance.stripOperation(operation);
  }

  private Operation stripQueuedOperation(Operation operation) {
    return instance.stripQueuedOperation(operation);
  }

  private static int inlineOrDigest(
      ByteString content,
      CASInsertionPolicy policy,
      ImmutableList.Builder<ByteString> contents,
      int inlineContentBytes,
      int inlineContentLimit,
      Runnable setInline,
      Consumer<ByteString> setDigest) {
    boolean withinLimit = inlineContentBytes + content.size() <= inlineContentLimit;
    if (withinLimit) {
      setInline.run();
      inlineContentBytes += content.size();
    }
    if (policy.equals(CASInsertionPolicy.ALWAYS_INSERT) ||
        (!withinLimit && policy.equals(CASInsertionPolicy.INSERT_ABOVE_LIMIT))) {
      contents.add(content);
      setDigest.accept(content);
    }
    return inlineContentBytes;
  }

  private void putAllBlobs(Iterable<ByteString> blobs) {
    for (ByteString content : blobs) {
      if (content.size() > 0) {
        Blob blob = new Blob(content, digestUtil);
        fileCache.put(blob);
      }
    }
  }

  public Worker(ServerBuilder<?> serverBuilder, ShardWorkerConfig config) throws ConfigurationException {
    this.config = config;
    workerStubs = new HashMap<>();
    root = getValidRoot(config);

    digestUtil = new DigestUtil(getValidHashFunction(config));

    ShardWorkerConfig.BackplaneCase backplaneCase = config.getBackplaneCase();
    switch (backplaneCase) {
      default:
      case BACKPLANE_NOT_SET:
        throw new IllegalArgumentException("Shard Backplane not set in config");
      case REDIS_SHARD_BACKPLANE_CONFIG:
        backplane = new RedisShardBackplane(config.getRedisShardBackplaneConfig(), this::stripOperation, this::stripQueuedOperation, (o) -> false, (o) -> false);
        break;
    }
    InputStreamFactory remoteInputStreamFactory = new InputStreamFactory() {
      private Random rand = new Random();

      private InputStream fetchBlobFromRemoteWorker(Digest blobDigest, Deque<String> workers, long offset) throws IOException, InterruptedException {
        String worker = workers.removeFirst();
        try {
          Instance instance = workerStub(worker);

          InputStream input = instance.newStreamInput(instance.getBlobName(blobDigest), offset);
          // ensure that if the blob cannot be fetched, that we throw here
          input.available();
          if (Thread.interrupted()) {
            throw new InterruptedException();
          }
          return input;
        } catch (RetryException e) {
          Status st = Status.fromThrowable(e);
          if (st.getCode().equals(Code.UNAVAILABLE)) {
            // for now, leave this up to schedulers
            // removeMalfunctioningWorker(worker, e, "getBlob(" + DigestUtil.toString(blobDigest) + ")");
          } else if (st.getCode() == Code.NOT_FOUND) {
            // ignore this, the worker will update the backplane eventually
          } else if (Retrier.DEFAULT_IS_RETRIABLE.test(st)) {
            // why not, always
            workers.addLast(worker);
          } else {
            throw e;
          }
        }
        throw new NoSuchFileException(DigestUtil.toString(blobDigest));
      }

      @Override
      public InputStream newInput(Digest blobDigest, long offset) throws IOException, InterruptedException {
        Set<String> workerSet;
        Set<String> locationSet;
        try {
          workerSet = Sets.difference(backplane.getWorkerSet(), ImmutableSet.<String>of(config.getPublicName()));
          locationSet = Sets.newHashSet(Sets.intersection(backplane.getBlobLocationSet(blobDigest), workerSet));
        } catch (IOException e) {
          throw Status.fromThrowable(e).asRuntimeException();
        }

        if (locationSet.remove(config.getPublicName())) {
          backplane.removeBlobLocation(blobDigest, config.getPublicName());
        }
        List<String> workersList = new ArrayList<>(locationSet);
        boolean emptyWorkerList = workersList.isEmpty();
        final ListenableFuture<List<String>> populatedWorkerListFuture;
        if (emptyWorkerList) {
          populatedWorkerListFuture = transform(
              correctMissingBlob(backplane, workerSet, locationSet, (worker) -> workerStub(worker), blobDigest, newDirectExecutorService()),
              (foundOnWorkers) -> {
                Iterables.addAll(workersList, foundOnWorkers);
                return workersList;
              });
        } else {
          populatedWorkerListFuture = immediateFuture(workersList);
        }
        SettableFuture<InputStream> inputStreamFuture = SettableFuture.create();
        addCallback(populatedWorkerListFuture, new WorkersCallback(rand) {
          boolean triedCheck = emptyWorkerList;

          @Override
          public void onQueue(Deque<String> workers) {
            Set<String> locationSet = Sets.newHashSet(workers);
            boolean failed = false;
            while (!failed && !workers.isEmpty()) {
              try {
                inputStreamFuture.set(fetchBlobFromRemoteWorker(blobDigest, workers, offset));
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
                        correctMissingBlob(backplane, workerSet, locationSet, (worker) -> workerStub(worker), blobDigest, newDirectExecutorService()),
                        (foundOnWorkers) -> {
                          Iterables.addAll(workersList, foundOnWorkers);
                          return workersList;
                        });
                    addCallback(checkedWorkerListFuture, this);
                  } catch (IOException checkException) {
                    failed = true;
                    onFailure(checkException);
                  }
                }
              } catch (InterruptedException e) {
                failed = true;
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
        });
        try {
          return inputStreamFuture.get();
        } catch (ExecutionException e) {
          if (e.getCause() instanceof IOException) {
            throw (IOException) e.getCause();
          }
          if (e.getCause() instanceof RuntimeException) {
            throw (RuntimeException) e.getCause();
          }
          throw new UncheckedExecutionException(e.getCause());
        }
      }
    };

    final ContentAddressableStorage storage;
    final OutputStreamFactory outputStreamFactory;
    if (config.getUseFuseCas()) {
      fileCache = null;

      storage = new MemoryLRUContentAddressableStorage(config.getCasMaxSizeBytes(), this::onStoragePut);
      storageInputStreamFactory = (digest, offset) -> storage.get(digest).getData().substring((int) offset).newInput();
      outputStreamFactory = (digest) -> { throw new UnsupportedOperationException(); };

      InputStreamFactory localPopulatingInputStreamFactory = new InputStreamFactory() {
        @Override
        public InputStream newInput(Digest blobDigest, long offset) throws IOException, InterruptedException {
          ByteString content = ByteString.readFrom(remoteInputStreamFactory.newInput(blobDigest, offset));

          if (offset == 0) {
            // extra computations
            Blob blob = new Blob(content, digestUtil);
            // here's hoping that our digest matches...
            storage.put(blob);
          }

          return content.newInput();
        }
      };
      fuseCAS = new FuseCAS(root, new EmptyInputStreamFactory(new FailoverInputStreamFactory(storageInputStreamFactory, localPopulatingInputStreamFactory)));
    } else {
      fuseCAS = null;

      InputStreamFactory selfInputStreamChain = new InputStreamFactory() {
        @Override
        public InputStream newInput(Digest blobDigest, long offset) throws IOException, InterruptedException {
          // oh the complication... using storage here requires us to be instantiated in this context
          return new EmptyInputStreamFactory(new FailoverInputStreamFactory(storageInputStreamFactory, remoteInputStreamFactory))
              .newInput(blobDigest, offset);
        }
      };
      Path casCacheDirectory = getValidCasCacheDirectory(config, root);
      fileCache = new CASFileCache(
          selfInputStreamChain,
          root.resolve(casCacheDirectory),
          config.getCasCacheMaxSizeBytes(),
          digestUtil,
          this::onStoragePut,
          this::onStorageExpire);
      storage = fileCache;
      storageInputStreamFactory = fileCache;
      outputStreamFactory = fileCache;
    }
    instance = new ShardWorkerInstance(
        config.getPublicName(),
        digestUtil,
        backplane,
        storage,
        storageInputStreamFactory,
        outputStreamFactory,
        config.getShardWorkerInstanceConfig());

    Instances instances = Instances.singular(instance);
    server = serverBuilder
        .addService(new ContentAddressableStorageService(instances))
        .addService(new ByteStreamService(instances))
        .build();

    // FIXME factor into pipeline factory/constructor
    WorkerContext context = new WorkerContext() {
      @Override
      public String getName() {
        return config.getPublicName();
      }

      @Override
      public Poller createPoller(String name, String operationName, Stage stage) {
        return createPoller(name, operationName, stage, () -> {});
      }

      @Override
      public Poller createPoller(String name, String operationName, Stage stage, Runnable onFailure) {
        Poller poller = new Poller(
            config.getOperationPollPeriod(),
            () -> {
              boolean success = false;
              try {
                success = backplane.pollOperation(operationName, stage, System.currentTimeMillis() + 30 * 1000);
              } catch (IOException e) {
                e.printStackTrace();
              }

              logInfo(name + ": poller: Completed Poll for " + operationName + ": " + (success ? "OK" : "Failed"));
              if (!success) {
                onFailure.run();
              }
              return success;
            });
        new Thread(poller).start();
        return poller;
      }

      @Override
      public DigestUtil getDigestUtil() {
        return instance.getDigestUtil();
      }

      @Override
      public void match(MatchListener listener) throws InterruptedException {
        instance.match(config.getPlatform(), new MatchListener() {
          @Override
          public void onWaitStart() {
            listener.onWaitStart();
          }

          @Override
          public void onWaitEnd() {
            listener.onWaitEnd();
          }

          @Override
          public boolean onOperationName(String operationName) {
            if (activeOperations.contains(operationName)) {
              System.err.println("WorkerContext::match: WARNING matched duplicate operation " + operationName);
              return listener.onOperation(null);
            }
            activeOperations.add(operationName);
            boolean success = listener.onOperationName(operationName);
            if (!success) {
              try {
                // fast path to requeue, implementation specific
                backplane.pollOperation(operationName, ExecuteOperationMetadata.Stage.QUEUED, 0);
              } catch (IOException e) {
                System.err.println("Failure while trying to fast requeue " + operationName);
                e.printStackTrace();
              }
            }
            return success;
          }

          @Override
          public boolean onOperation(Operation operation) {
            // could not fetch
            if (operation.getDone()) {
              activeOperations.remove(operation.getName());
              listener.onOperation(null);
              try {
                backplane.pollOperation(operation.getName(), ExecuteOperationMetadata.Stage.QUEUED, 0);
              } catch (IOException e) {
                System.err.println("Failure while trying to fast requeue " + operation.getName());
                e.printStackTrace();
              }
              return false;
            }

            boolean success = listener.onOperation(operation);
            if (!success) {
              try {
                requeue(operation);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
              }
            }
            return success;
          }
        });
        if (Thread.interrupted()) {
          throw new InterruptedException();
        }
      }

      private ExecuteOperationMetadata expectExecuteOperationMetadata(Operation operation) {
        Any metadata = operation.getMetadata();
        if (metadata == null) {
          return null;
        }

        if (metadata.is(QueuedOperationMetadata.class)) {
          try {
            return operation.getMetadata().unpack(QueuedOperationMetadata.class).getExecuteOperationMetadata();
          } catch(InvalidProtocolBufferException e) {
            e.printStackTrace();
            return null;
          }
        }

        if (metadata.is(ExecuteOperationMetadata.class)) {
          try {
            return operation.getMetadata().unpack(ExecuteOperationMetadata.class);
          } catch(InvalidProtocolBufferException e) {
            e.printStackTrace();
            return null;
          }
        }

        return null;
      }

      @Override
      public void requeue(Operation operation) throws InterruptedException {
        deactivate(operation);
        try {
          backplane.pollOperation(operation.getName(), ExecuteOperationMetadata.Stage.QUEUED, 0);
        } catch (IOException e) {
          // ignore, at least dispatcher will pick us up in 30s
          e.printStackTrace();
        }
      }

      @Override
      public void deactivate(Operation operation) {
        activeOperations.remove(operation.getName());
      }

      @Override
      public void logInfo(String msg) {
        System.out.println(msg);
      }

      @Override
      public int getInlineContentLimit() {
        return config.getInlineContentLimit();
      }

      @Override
      public CASInsertionPolicy getFileCasPolicy() {
        return CASInsertionPolicy.ALWAYS_INSERT;
      }

      @Override
      public CASInsertionPolicy getStdoutCasPolicy() {
        return CASInsertionPolicy.ALWAYS_INSERT;
      }

      @Override
      public CASInsertionPolicy getStderrCasPolicy() {
        return CASInsertionPolicy.ALWAYS_INSERT;
      }

      @Override
      public int getExecuteStageWidth() {
        return config.getExecuteStageWidth();
      }

      @Override
      public int getTreePageSize() {
        return 0;
      }

      @Override
      public boolean hasDefaultActionTimeout() {
        return false;
      }

      @Override
      public boolean hasMaximumActionTimeout() {
        return false;
      }

      @Override
      public boolean getStreamStdout() {
        return true;
      }

      @Override
      public boolean getStreamStderr() {
        return true;
      }

      @Override
      public Duration getDefaultActionTimeout() {
        return null;
      }

      @Override
      public Duration getMaximumActionTimeout() {
        return null;
      }

      private int updateActionResultStdOutputs(
          ActionResult.Builder resultBuilder,
          ImmutableList.Builder<ByteString> contents,
          int inlineContentBytes) {
        ByteString stdoutRaw = resultBuilder.getStdoutRaw();
        if (stdoutRaw.size() > 0) {
          // reset to allow policy to determine inlining
          resultBuilder.setStdoutRaw(ByteString.EMPTY);
          inlineContentBytes = inlineOrDigest(
              stdoutRaw,
              getStdoutCasPolicy(),
              contents,
              inlineContentBytes,
              getInlineContentLimit(),
              () -> resultBuilder.setStdoutRaw(stdoutRaw),
              (content) -> resultBuilder.setStdoutDigest(getDigestUtil().compute(content)));
        }

        ByteString stderrRaw = resultBuilder.getStderrRaw();
        if (stderrRaw.size() > 0) {
          // reset to allow policy to determine inlining
          resultBuilder.setStderrRaw(ByteString.EMPTY);
          inlineContentBytes = inlineOrDigest(
              stderrRaw,
              getStderrCasPolicy(),
              contents,
              inlineContentBytes,
              getInlineContentLimit(),
              () -> resultBuilder.setStderrRaw(stdoutRaw),
              (content) -> resultBuilder.setStderrDigest(getDigestUtil().compute(content)));
        }

        return inlineContentBytes;
      }

      @Override
      public void uploadOutputs(
          ActionResult.Builder resultBuilder,
          Path actionRoot,
          Iterable<String> outputFiles,
          Iterable<String> outputDirs)
          throws IOException, InterruptedException {
        int inlineContentBytes = 0;
        ImmutableList.Builder<ByteString> contents = new ImmutableList.Builder<>();
        for (String outputFile : outputFiles) {
          Path outputPath = actionRoot.resolve(outputFile);
          if (!Files.exists(outputPath)) {
            logInfo("ReportResultStage: " + outputPath + " does not exist...");
            continue;
          }

          // FIXME put the output into the fileCache
          // FIXME this needs to be streamed to the server, not read to completion, but
          // this is a constraint of not knowing the hash, however, if we put the entry
          // into the cache, we can likely do so, stream the output up, and be done
          //
          // will run into issues if we end up blocking on the cache insertion, might
          // want to decrement input references *before* this to ensure that we cannot
          // cause an internal deadlock

          ByteString content;
          try (InputStream inputStream = Files.newInputStream(outputPath)) {
            content = ByteString.readFrom(inputStream);
          } catch (IOException e) {
            continue;
          }

          OutputFile.Builder outputFileBuilder = resultBuilder.addOutputFilesBuilder()
              .setPath(outputFile)
              .setIsExecutable(Files.isExecutable(outputPath));
          inlineContentBytes = inlineOrDigest(
              content,
              getFileCasPolicy(),
              contents,
              inlineContentBytes,
              getInlineContentLimit(),
              () -> outputFileBuilder.setContent(content),
              (fileContent) -> outputFileBuilder.setDigest(getDigestUtil().compute(fileContent)));
        }

        for (String outputDir : outputDirs) {
          Path outputDirPath = actionRoot.resolve(outputDir);
          if (!Files.exists(outputDirPath)) {
            logInfo("ReportResultStage: " + outputDir + " does not exist...");
            continue;
          }

          Tree.Builder treeBuilder = Tree.newBuilder();
          Directory.Builder outputRoot = treeBuilder.getRootBuilder();
          Files.walkFileTree(outputDirPath, new SimpleFileVisitor<Path>() {
            Directory.Builder currentDirectory = null;
            Stack<Directory.Builder> path = new Stack<>();

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
              ByteString content;
              try (InputStream inputStream = Files.newInputStream(file)) {
                content = ByteString.readFrom(inputStream);
              } catch (IOException e) {
                e.printStackTrace();
                return FileVisitResult.CONTINUE;
              }

              // should we cast to PosixFilePermissions and do gymnastics there for executable?

              // TODO symlink per revision proposal
              contents.add(content);
              FileNode.Builder fileNodeBuilder = currentDirectory.addFilesBuilder()
                  .setName(file.getFileName().toString())
                  .setDigest(getDigestUtil().compute(content))
                  .setIsExecutable(Files.isExecutable(file));
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
              path.push(currentDirectory);
              if (dir.equals(outputDirPath)) {
                currentDirectory = outputRoot;
              } else {
                currentDirectory = treeBuilder.addChildrenBuilder();
              }
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
              Directory.Builder parentDirectory = path.pop();
              if (parentDirectory != null) {
                parentDirectory.addDirectoriesBuilder()
                    .setName(dir.getFileName().toString())
                    .setDigest(getDigestUtil().compute(currentDirectory.build()));
              }
              currentDirectory = parentDirectory;
              return FileVisitResult.CONTINUE;
            }
          });
          Tree tree = treeBuilder.build();
          ByteString treeBlob = tree.toByteString();
          contents.add(treeBlob);
          Digest treeDigest = getDigestUtil().compute(treeBlob);
          resultBuilder.addOutputDirectoriesBuilder()
              .setPath(outputDir)
              .setTreeDigest(treeDigest);
        }

        /* put together our outputs and update the result */
        updateActionResultStdOutputs(resultBuilder, contents, inlineContentBytes);

        List<ByteString> blobs = contents.build();
        if (!blobs.isEmpty()) {
          putAllBlobs(blobs);
        }
      }

      @Override
      public boolean putOperation(Operation operation, Action action) throws IOException, InterruptedException {
        boolean success = createBackplaneRetrier().execute(() -> instance.putOperation(operation));
        if (success && operation.getDone()) {
          backplane.removeTree(action.getInputRootDigest());
        }
        return success;
      }

      @Override
      public void createActionRoot(Path actionRoot, Map<Digest, Directory> directoriesIndex, Action action) throws IOException, InterruptedException {
        if (config.getUseFuseCas()) {
          String topdir = root.relativize(actionRoot).toString();
          fuseCAS.createInputRoot(topdir, action.getInputRootDigest());
        } else {
          OutputDirectory outputDirectory = OutputDirectory.parse(
              action.getOutputFilesList(),
              action.getOutputDirectoriesList());

          if (Files.exists(actionRoot)) {
            getOrIOException(fileCache.removeDirectoryAsync(actionRoot));
          }
          Files.createDirectories(actionRoot);

          ImmutableList.Builder<Path> inputFiles = new ImmutableList.Builder<>();
          ImmutableList.Builder<Digest> inputDirectories = new ImmutableList.Builder<>();

          System.out.println("WorkerContext::createActionRoot(" + DigestUtil.toString(action.getInputRootDigest()) + ") calling fetchInputs");
          boolean fetched = false;
          try {
            fetchInputs(
                actionRoot,
                action.getInputRootDigest(),
                directoriesIndex,
                outputDirectory,
                inputFiles,
                inputDirectories);
            fetched = true;
          } finally {
            if (!fetched) {
              fileCache.decrementReferences(inputFiles.build(), inputDirectories.build());
            }
          }

          rootInputFiles.put(actionRoot, inputFiles.build());
          rootInputDirectories.put(actionRoot, inputDirectories.build());

          System.out.println("WorkerContext::createActionRoot(" + DigestUtil.toString(action.getInputRootDigest()) + ") stamping output directories");
          try {
            outputDirectory.stamp(actionRoot);
          } catch (IOException e) {
            destroyActionRoot(actionRoot);
            throw e;
          }
        }
      }

      // might want to split for removeDirectory and decrement references to avoid removing for streamed output
      @Override
      public void destroyActionRoot(Path actionRoot) throws IOException, InterruptedException {
        if (config.getUseFuseCas()) {
          String topdir = root.relativize(actionRoot).toString();
          fuseCAS.destroyInputRoot(topdir);
        } else {
          Iterable<Path> inputFiles = rootInputFiles.remove(actionRoot);
          Iterable<Digest> inputDirectories = rootInputDirectories.remove(actionRoot);
          if (inputFiles != null || inputDirectories != null) {
            fileCache.decrementReferences(
                inputFiles == null ? ImmutableList.<Path>of() : inputFiles,
                inputDirectories == null ? ImmutableList.<Digest>of() : inputDirectories);
          }
          if (Files.exists(actionRoot)) {
            getOrIOException(fileCache.removeDirectoryAsync(actionRoot));
          }
        }
      }

      public Path getRoot() {
        return root;
      }

      @Override
      public void putActionResult(ActionKey actionKey, ActionResult actionResult) throws IOException, InterruptedException {
        createBackplaneRetrier().execute(() -> {
          instance.putActionResult(actionKey, actionResult);
          return null;
        });
      }

      @Override
      public OutputStream getStreamOutput(String name) {
        throw new UnsupportedOperationException();
      }
    };

    PipelineStage completeStage = new PutOperationStage(context::deactivate);
    PipelineStage errorStage = completeStage; /* new ErrorStage(); */
    PipelineStage reportResultStage = new ReportResultStage(context, completeStage, errorStage);
    PipelineStage executeActionStage = new ExecuteActionStage(context, reportResultStage, errorStage);
    reportResultStage.setInput(executeActionStage);
    PipelineStage inputFetchStage = new InputFetchStage(context, executeActionStage, new PutOperationStage(context::requeue));
    executeActionStage.setInput(inputFetchStage);
    PipelineStage matchStage = new MatchStage(context, inputFetchStage, errorStage);
    inputFetchStage.setInput(matchStage);

    pipeline = new Pipeline();
    // pipeline.add(errorStage, 0);
    pipeline.add(matchStage, 1);
    pipeline.add(inputFetchStage, 2);
    pipeline.add(executeActionStage, 3);
    pipeline.add(reportResultStage, 4);
  }

  public void stop() throws InterruptedException {
    System.err.println("Closing the pipeline");
    try {
      pipeline.close();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    if (config.getUseFuseCas()) {
      System.err.println("Umounting Fuse");
      fuseCAS.stop();
    } else {
      fileCache.stop();
    }
    if (server != null) {
      System.err.println("Shutting down the server");
      server.shutdown();

      try {
        server.awaitTermination(shutdownWaitTimeInMillis, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      } finally {
        server.shutdownNow();
      }
    }
    backplane.stop();
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
  }

  private void onStoragePut(Digest digest) {
    try {
      backplane.addBlobLocation(digest, config.getPublicName());
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  private void onStorageExpire(Iterable<Digest> digests) {
    try {
      backplane.removeBlobsLocation(digests, config.getPublicName());
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  private void blockUntilShutdown() throws InterruptedException {
    // should really be waiting for both of these things...
    /*
    if (server != null) {
      server.awaitTermination();
    }
    */
    try {
      pipeline.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    if (server != null) {
      server.shutdown();

      try {
        server.awaitTermination(shutdownWaitTimeInMillis, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      } finally {
        server.shutdownNow();
      }
    }
    if (config.getUseFuseCas()) {
      fuseCAS.stop();
    } else {
      fileCache.stop();
    }
    backplane.stop();
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
  }

  private void removeWorker(String name) {
    try {
      backplane.removeWorker(name);
    } catch (IOException e) {
      Status status = Status.fromThrowable(e);
      if (status.getCode() != Code.UNAVAILABLE && status.getCode() != Code.DEADLINE_EXCEEDED) {
        throw status.asRuntimeException();
      }
      System.out.println("backplane was unavailable or overloaded, deferring removeWorker");
    }
  }

  private void addBlobsLocation(List<Digest> digests, String name) {
    for (;;) {
      try {
        backplane.addBlobsLocation(digests, name);
        return;
      } catch (IOException e) {
        Status status = Status.fromThrowable(e);
        if (status.getCode() != Code.UNAVAILABLE && status.getCode() != Code.DEADLINE_EXCEEDED) {
          throw status.asRuntimeException();
        }
      }
    }
  }

  private void addWorker(String name) {
    for (;;) {
      try {
        backplane.addWorker(name);
        return;
      } catch (IOException e) {
        Status status = Status.fromThrowable(e);
        if (status.getCode() != Code.UNAVAILABLE && status.getCode() != Code.DEADLINE_EXCEEDED) {
          throw status.asRuntimeException();
        }
      }
    }
  }

  public void start() throws InterruptedException {
    try {
      backplane.start();

      removeWorker(config.getPublicName());

      if (!config.getUseFuseCas()) {
        List<Dirent> dirents = null;
        try {
          dirents = UploadManifest.readdir(root, /* followSymlinks= */ false);
        } catch (IOException e) {
          e.printStackTrace();
        }

        ImmutableList.Builder<ListenableFuture<Void>> removeDirectoryFutures = new ImmutableList.Builder<>();

        // only valid path under root is cache
        for (Dirent dirent : dirents) {
          String name = dirent.getName();
          Path child = root.resolve(name);
          if (!child.equals(fileCache.getRoot())) {
            removeDirectoryFutures.add(fileCache.removeDirectoryAsync(root.resolve(name)));
          }
        }

        ImmutableList.Builder<Digest> blobDigests = new ImmutableList.Builder<>();
        fileCache.start(blobDigests::add);
        addBlobsLocation(blobDigests.build(), config.getPublicName());

        getOrIOException(allAsList(removeDirectoryFutures.build()));
      }

      server.start();
      addWorker(config.getPublicName());
    } catch (Exception e) {
      stop();
      e.printStackTrace();
      return;
    }
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        try {
          Worker.this.stop();
          System.err.println("*** server shut down");
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    });
    pipeline.start();
  }

  private static ShardWorkerConfig toShardWorkerConfig(Readable input, WorkerOptions options) throws IOException {
    ShardWorkerConfig.Builder builder = ShardWorkerConfig.newBuilder();
    TextFormat.merge(input, builder);
    if (!Strings.isNullOrEmpty(options.root)) {
      builder.setRoot(options.root);
    }
    if (!Strings.isNullOrEmpty(options.casCacheDirectory)) {
      builder.setCasCacheDirectory(options.casCacheDirectory);
    }
    if (!Strings.isNullOrEmpty(options.publicName)) {
      builder.setPublicName(options.publicName);
    }

    return builder.build();
  }

  private static void printUsage(OptionsParser parser) {
    System.out.println("Usage: CONFIG_PATH");
    System.out.println(parser.describeOptions(Collections.<String, String>emptyMap(),
                                              OptionsParser.HelpVerbosity.LONG));
  }

  public static void main(String[] args) throws Exception {
    // Only log severe log messages from Netty. Otherwise it logs warnings that look like this:
    //
    // 170714 08:16:28.552:WT 18 [io.grpc.netty.NettyServerHandler.onStreamError] Stream Error
    // io.netty.handler.codec.http2.Http2Exception$StreamException: Received DATA frame for an
    // unknown stream 11369
    nettyLogger.setLevel(Level.SEVERE);

    OptionsParser parser = OptionsParser.newOptionsParser(WorkerOptions.class);
    parser.parseAndExitUponError(args);
    List<String> residue = parser.getResidue();
    if (residue.isEmpty()) {
      printUsage(parser);
      throw new IllegalArgumentException("Missing CONFIG_PATH");
    }
    Path configPath = Paths.get(residue.get(0));
    Worker worker;
    try (InputStream configInputStream = Files.newInputStream(configPath)) {
      worker = new Worker(toShardWorkerConfig(new InputStreamReader(configInputStream), parser.getOptions(WorkerOptions.class)));
    }
    worker.start();
    worker.blockUntilShutdown();
  }
}
