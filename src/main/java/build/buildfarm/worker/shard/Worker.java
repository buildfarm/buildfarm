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

import build.buildfarm.common.ContentAddressableStorage;
import build.buildfarm.common.ContentAddressableStorage.Blob;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.ShardBackplane;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.memory.MemoryLRUContentAddressableStorage;
import build.buildfarm.instance.shard.RedisShardBackplane;
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
import build.buildfarm.worker.ExecuteActionStage;
import build.buildfarm.worker.Fetcher;
import build.buildfarm.worker.FuseCAS;
import build.buildfarm.worker.InputFetchStage;
import build.buildfarm.worker.InputStreamFactory;
import build.buildfarm.worker.MatchStage;
import build.buildfarm.worker.OutputDirectory;
import build.buildfarm.worker.Pipeline;
import build.buildfarm.worker.PipelineStage;
import build.buildfarm.worker.Poller;
import build.buildfarm.worker.PutOperationStage;
import build.buildfarm.worker.ReportResultStage;
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
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
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
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.naming.ConfigurationException;

public class Worker implements Instances {
  private static final Logger nettyLogger = Logger.getLogger("io.grpc.netty");

  private final ShardWorkerConfig config;
  private final ShardWorkerInstance instance;
  private final Server server;
  private final Path root;
  private final DigestUtil digestUtil;
  private final ContentAddressableStorage storage;
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

  @Override
  public Instance getFromBlob(String blobName) throws InstanceNotFoundException { return instance; }

  @Override
  public Instance getFromUploadBlob(String uploadBlobName) throws InstanceNotFoundException { return instance; }

  @Override
  public Instance getFromOperationsCollectionName(String operationsCollectionName) throws InstanceNotFoundException { return instance; }

  @Override
  public Instance getFromOperationName(String operationName) throws InstanceNotFoundException { return instance; }

  @Override
  public Instance getFromOperationStream(String operationStream) throws InstanceNotFoundException { return instance; }

  @Override
  public Instance get(String name) throws InstanceNotFoundException { return instance; }

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

  /*
  private void removeMalfunctioningWorkerClient(String worker) {
    StubInstance instance = workerStubs.remove(worker);
    if (instance != null) {
      ManagedChannel channel = instance.getChannel();
      channel.shutdownNow();
      try {
        channel.awaitTermination(0, TimeUnit.SECONDS);
      } catch (InterruptedException intEx) {
        // impossible, 0 timeout
        Thread.currentThread().interrupt();
      }
    }
  }
  */

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
      /*
      System.out.println(String.format("Worker::fetchInputs(%s) linking %s (%s)",
            DigestUtil.toString(directoryDigest),
            fileNode.getIsExecutable() ? ("*" + fileNode.getName() + "*") : fileNode.getName(),
            DigestUtil.toString(fileNode.getDigest())));
            */
      Path fileCacheKey = fileCache.put(fileNode.getDigest(), fileNode.getIsExecutable(), /* containingDirectory=*/ null);
      inputFiles.add(fileCacheKey);
      /*
      System.out.println(String.format("Worker::fetchInputs(%s) linking %s complete (%s)",
            DigestUtil.toString(directoryDigest),
            fileNode.getIsExecutable() ? ("*" + fileNode.getName() + "*") : fileNode.getName(),
            DigestUtil.toString(fileNode.getDigest())));
            */
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
        /*
        System.out.println(String.format("Worker::fetchInputs(%s) subdirectory %s (%s)",
              DigestUtil.toString(directoryDigest),
              name,
              DigestUtil.toString(digest)));
              */
        fetchInputs(dirPath, digest, directoriesIndex, childOutputDirectory, inputFiles, inputDirectories);
        /*
        System.out.println(String.format("Worker::fetchInputs(%s) subdirectory complete %s (%s)",
              DigestUtil.toString(directoryDigest),
              name,
              DigestUtil.toString(digest)));
              */
      } else {
        /*
        System.out.println(String.format("Worker::fetchInputs(%s) linkDirectory %s (%s)",
              DigestUtil.toString(directoryDigest),
              name,
              DigestUtil.toString(digest)));
              */
        linkDirectory(dirPath, digest, directoriesIndex);
        inputDirectories.add(digest);
        /*
        System.out.println(String.format("Worker::fetchInputs(%s) linkDirectory complete %s (%s)",
              DigestUtil.toString(directoryDigest),
              name,
              DigestUtil.toString(digest)));
              */
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
        backplane = new RedisShardBackplane(config.getRedisShardBackplaneConfig(), this::stripOperation, this::stripQueuedOperation, () -> {});
        break;
    }
    Fetcher remoteFetcher = new Fetcher() {
      private Random rand = new Random();

      private ByteString fetchBlobFromRemoteWorker(Digest blobDigest, Deque<String> workers) throws IOException, InterruptedException {
        String worker = workers.removeFirst();
        try {
          for (;;) {
            try {
              return workerStub(worker).getBlob(blobDigest);
            } catch (RetryException e) {
              Throwable cause = e.getCause();
              if (cause instanceof StatusRuntimeException) {
                StatusRuntimeException sre = (StatusRuntimeException) e.getCause();
                throw sre;
              }
              throw e;
            }
          }
        } catch (StatusRuntimeException e) {
          Status st = Status.fromThrowable(e);
          if (st.getCode().equals(Status.Code.UNAVAILABLE)) {
            backplane.removeBlobLocation(blobDigest, worker);
            // for now, leave this up to schedulers
            // removeMalfunctioningWorker(worker, e, "getBlob(" + DigestUtil.toString(blobDigest) + ")");
          } else if (st.getCode().equals(Status.Code.NOT_FOUND)) {
            // ignore this, the worker will update the backplane eventually
          } else if (Retrier.DEFAULT_IS_RETRIABLE.test(st)) {
            // why not, always
            workers.addLast(worker);
          } else {
            throw e;
          }
          return null;
        }
      }

      private List<String> correctMissingBlob(Digest digest) throws IOException {
        ImmutableList.Builder<String> foundWorkers = new ImmutableList.Builder<>();
        Deque<String> workers;
        try {
          List<String> workersList = new ArrayList<>(
              Sets.difference(backplane.getWorkerSet(), ImmutableSet.<String>of(config.getPublicName())));
          Collections.shuffle(workersList, rand);
          workers = new ArrayDeque(workersList);
        } catch (IOException e) {
          throw Status.fromThrowable(e).asRuntimeException();
        }

        for (String worker : workers) {
          try {
            Iterable<Digest> resultDigests = null;
            while (resultDigests == null) {
              try {
                resultDigests = createStubRetrier().execute(() -> workerStub(worker).findMissingBlobs(ImmutableList.<Digest>of(digest)));
                if (Iterables.size(resultDigests) == 0) {
                  System.out.println("Worker::RemoteFetcher::correctMissingBlob(" + DigestUtil.toString(digest) + "): Adding back to " + worker);
                  backplane.addBlobLocation(digest, worker);
                  foundWorkers.add(worker);
                }
              } catch (RetryException e) {
                if (e.getCause() instanceof StatusRuntimeException) {
                  StatusRuntimeException sre = (StatusRuntimeException) e.getCause();
                  throw sre;
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
            if (e.getStatus().getCode().equals(Status.UNAVAILABLE.getCode())) {
              // removeMalfunctioningWorker(worker, e, "findMissingBlobs(...)");
            } else {
              e.printStackTrace();
            }
          }
        }
        return foundWorkers.build();
      }

      @Override
      public ByteString fetchBlob(Digest blobDigest) throws InterruptedException, IOException {
        Set<String> workerSet = new HashSet<>(Sets.intersection(
            backplane.getBlobLocationSet(blobDigest),
            backplane.getWorkerSet()));
        if (workerSet.remove(config.getPublicName())) {
          backplane.removeBlobLocation(blobDigest, config.getPublicName());
        }
        List<String> workersList = new ArrayList<>(workerSet);
        Collections.shuffle(workersList, rand);
        Deque<String> workers = new ArrayDeque(workersList);

        boolean printFinal = false;
        boolean triedCheck = false;
        ByteString content;
        do {
          if (workers.isEmpty()) {
            if (triedCheck) {
              // maybe just return null here
              throw new IOException("worker not found for blob " + DigestUtil.toString(blobDigest));
            }

            workersList.clear();
            workersList.addAll(correctMissingBlob(blobDigest));
            Collections.shuffle(workersList, rand);
            workers = new ArrayDeque(workersList);
            content = null;
            triedCheck = true;
          } else {
            int sizeBeforeFetch = workers.size();
            content = fetchBlobFromRemoteWorker(blobDigest, workers);
            if (sizeBeforeFetch == workers.size()) {
              printFinal = true;
              System.out.println("Pushed worker to end of request list: " + workers.peekLast() + " for " + DigestUtil.toString(blobDigest));
            }
          }
        } while (content == null);

        if (printFinal) {
          System.out.println("fetch with retried loop succeeded for " + DigestUtil.toString(blobDigest));
        }

        return content;
      }
    };

    Fetcher localPopulatingFetcher = new Fetcher() {
      @Override
      public ByteString fetchBlob(Digest blobDigest) throws InterruptedException, IOException {
        ByteString content = remoteFetcher.fetchBlob(blobDigest);

        // extra computations
        Blob blob = new Blob(content, digestUtil);
        // here's hoping that our digest matches...
        storage.put(blob);

        return content;
      }
    };

    if (config.getUseFuseCas()) {
      fileCache = null;

      storage = new MemoryLRUContentAddressableStorage(config.getCasMaxSizeBytes(), this::onStoragePut);

      fuseCAS = new FuseCAS(root, new EmptyFetcher(new StorageFetcher(storage, localPopulatingFetcher)));
    } else {
      fuseCAS = null;

      InputStreamFactory inputStreamFactory = new InputStreamFactory() {
        @Override
        public InputStream newInput(Digest blobDigest) throws InterruptedException, IOException {
          // jank hax
          ByteString blob = new EmptyFetcher(new StorageFetcher(storage, remoteFetcher))
              .fetchBlob(blobDigest);

          if (blob == null) {
            return null;
          }
          
          return blob.newInput();
        }
      };
      Path casCacheDirectory = getValidCasCacheDirectory(config, root);
      fileCache = new CASFileCache(
          inputStreamFactory,
          root.resolve(casCacheDirectory),
          config.getCasCacheMaxSizeBytes(),
          digestUtil,
          this::onStoragePut,
          this::onStorageExpire);
      storage = fileCache;
    }
    instance = new ShardWorkerInstance(
        config.getPublicName(),
        digestUtil,
        backplane,
        new EmptyFetcher(new StorageFetcher(storage, localPopulatingFetcher)),
        storage,
        config.getShardWorkerInstanceConfig());

    server = serverBuilder
        .addService(new ContentAddressableStorageService(this))
        .addService(new ByteStreamService(this))
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
      public void match(Predicate<Operation> onMatch) throws InterruptedException {
        instance.match(config.getPlatform(), (operation) -> {
          if (activeOperations.contains(operation.getName())) {
            return onMatch.test(null);
          }
          activeOperations.add(operation.getName());
          return onMatch.test(operation);
        });
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

      @Override
      public ByteString getBlob(Digest digest) throws IOException, InterruptedException {
        return instance.fetchBlob(digest);
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
          instance.putAllBlobs(blobs);
        }
      }

      @Override
      public boolean putOperation(Operation operation, Action action) throws IOException, InterruptedException {
        boolean success = instance.putOperation(operation);
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
            CASFileCache.removeDirectory(actionRoot);
          }
          Files.createDirectories(actionRoot);

          ImmutableList.Builder<Path> inputFiles = new ImmutableList.Builder<>();
          ImmutableList.Builder<Digest> inputDirectories = new ImmutableList.Builder<>();

          System.out.println("WorkerContext::createActionRoot(" + DigestUtil.toString(action.getInputRootDigest()) + ") calling fetchInputs");
          try {
            fetchInputs(
                actionRoot,
                action.getInputRootDigest(),
                directoriesIndex,
                outputDirectory,
                inputFiles,
                inputDirectories);
          } catch (IOException e) {
            fileCache.decrementReferences(inputFiles.build(), inputDirectories.build());
            throw e;
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
      public void destroyActionRoot(Path actionRoot) throws InterruptedException, IOException {
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
            removeDirectory(actionRoot);
          }
        }
      }

      public Path getRoot() {
        return root;
      }

      @Override
      public void removeDirectory(Path path) throws IOException {
        CASFileCache.removeDirectory(path);
      }

      @Override
      public void putActionResult(ActionKey actionKey, ActionResult actionResult) {
        instance.putActionResult(actionKey, actionResult);
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

  public void stop() {
    System.err.println("Closing the pipeline");
    try {
      pipeline.close();
    } catch (InterruptedException e) {
      return;
    }
    if (config.getUseFuseCas()) {
      System.err.println("Umounting Fuse");
      fuseCAS.stop();
    }
    if (server != null) {
      System.err.println("Shutting down the server");
      server.shutdown();
    }
    backplane.stop();
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
    pipeline.join();
    if (server != null) {
      server.shutdown();
    }
    backplane.stop();
  }

  @Override
  public void start() {
    try {
      backplane.removeWorker(config.getPublicName());

      if (!config.getUseFuseCas()) {
        ImmutableList.Builder<Digest> blobDigests = new ImmutableList.Builder<>();
        fileCache.start(blobDigests::add);
        backplane.addBlobsLocation(blobDigests.build(), config.getPublicName());
      }

      server.start();
      backplane.addWorker(config.getPublicName());

      pipeline.start();
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        Worker.this.stop();
        System.err.println("*** server shut down");
      }
    });
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
