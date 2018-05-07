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
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.server.InstanceNotFoundException;
import build.buildfarm.server.Instances;
import build.buildfarm.server.ContentAddressableStorageService;
import build.buildfarm.server.ByteStreamService;
import build.buildfarm.worker.CASFileCache;
import build.buildfarm.worker.ExecuteActionStage;
import build.buildfarm.worker.Fetcher;
import build.buildfarm.worker.InputFetchStage;
import build.buildfarm.worker.InputStreamFactory;
import build.buildfarm.worker.MatchStage;
import build.buildfarm.worker.OutputDirectory;
import build.buildfarm.worker.Pipeline;
import build.buildfarm.worker.PipelineStage;
import build.buildfarm.worker.Poller;
import build.buildfarm.worker.ReportResultStage;
import build.buildfarm.worker.WorkerContext;
import build.buildfarm.v1test.CASInsertionPolicy;
import build.buildfarm.v1test.ShardWorkerConfig;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableList;
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
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata.Stage;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.TextFormat;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import javax.naming.ConfigurationException;

public class Worker implements Instances {
  private final ShardWorkerConfig config;
  private final ShardWorkerInstance instance;
  private final Server server;
  private final Path root;
  private final DigestUtil digestUtil;
  private final CASFileCache fileCache;
  private final Pipeline pipeline;
  private final ShardBackplane backplane;
  private final Map<String, StubInstance> workerStubs;
  private final Map<Path, Iterable<Path>> rootInputFiles = new ConcurrentHashMap<>();
  private final Map<Path, Iterable<Digest>> rootInputDirectories = new ConcurrentHashMap<>();
  private final ListeningScheduledExecutorService retryScheduler =
      MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(1));

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
    return Paths.get(rootValue);
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
          createStubUploader(channel));
      workerStubs.put(worker, instance);
    }
    return instance;
  }

  private void fetchInputs(
      Path execDir,
      Digest inputRoot,
      OutputDirectory outputDirectory,
      ImmutableList.Builder<Path> inputFiles,
      ImmutableList.Builder<Digest> inputDirectories) throws IOException, InterruptedException {
    ImmutableList.Builder<Directory> directories = new ImmutableList.Builder<>();
    String pageToken = "";

    do {
      pageToken = instance.getTree(inputRoot, config.getTreePageSize(), pageToken, directories);
    } while (!pageToken.isEmpty());

    Set<Digest> directoryDigests = new HashSet<>();
    ImmutableMap.Builder<Digest, Directory> directoriesIndex = new ImmutableMap.Builder<>();
    for (Directory directory : directories.build()) {
      Digest directoryDigest = instance.getDigestUtil().compute(directory);
      if (!directoryDigests.add(directoryDigest)) {
        continue;
      }
      directoriesIndex.put(directoryDigest, directory);
    }

    linkInputs(execDir, inputRoot, directoriesIndex.build(), outputDirectory, inputFiles, inputDirectories);
  }

  private void linkInputs(
      Path execDir,
      Digest inputRoot,
      Map<Digest, Directory> directoriesIndex,
      OutputDirectory outputDirectory,
      ImmutableList.Builder<Path> inputFiles,
      ImmutableList.Builder<Digest> inputDirectories)
      throws IOException, InterruptedException {
    Directory directory = directoriesIndex.get(inputRoot);
    if (directory == null) {
      throw new IOException("Directory " + DigestUtil.toString(inputRoot) + " is not in input index");
    }

    for (FileNode fileNode : directory.getFilesList()) {
      Path execPath = execDir.resolve(fileNode.getName());
      Path fileCacheKey = fileCache.put(fileNode.getDigest(), fileNode.getIsExecutable(), /* containingDirectory=*/ null);
      if (fileCacheKey == null) {
        throw new IOException("failed to create cache entry for " + execPath);
      }
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
        linkInputs(dirPath, digest, directoriesIndex, childOutputDirectory, inputFiles, inputDirectories);
      } else {
        inputDirectories.add(digest);
        linkDirectory(dirPath, digest, directoriesIndex);
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
        backplane = new RedisShardBackplane(config.getRedisShardBackplaneConfig());
        break;
    }
    Fetcher fetcher = new Fetcher() {
      @Override
      public ByteString fetchBlob(Digest blobDigest) {
        for (;;) {
          String worker = backplane.getBlobLocation(blobDigest);
          if (worker == null) {
            return null;
          }
          if (worker.equals(config.getPublicName())) {
            if (fileCache.contains(blobDigest)) {
              return fileCache.get(blobDigest).getData();
            }
            return null;
          }
          return workerStub(worker).getBlob(blobDigest);
        }
      }
    };

    InputStreamFactory inputStreamFactory = new InputStreamFactory() {
      @Override
      public InputStream newInput(Digest digest) throws IOException {
        if (digest.getSizeBytes() == 0) {
          return ByteString.EMPTY.newInput();
        }

        ByteString blob = fetcher.fetchBlob(digest);
        if (blob == null) {
          throw new IOException("file not found: " + DigestUtil.toString(digest));
        }
        return blob.newInput();
      }
    };
    Path casCacheDirectory = getValidCasCacheDirectory(config, root);
    fileCache = new CASFileCache(
        inputStreamFactory,
        root.resolve(casCacheDirectory),
        config.getCasCacheMaxSizeBytes(),
        digestUtil);
    instance = new ShardWorkerInstance(
        config.getPublicName(),
        digestUtil,
        backplane,
        fetcher,
        fileCache,
        config.getShardWorkerInstanceConfig());

    server = serverBuilder
        .addService(new ContentAddressableStorageService(this))
        .addService(new ByteStreamService(this))
        .build();

    // FIXME XXX GIGANTIC HAX DO NOT RELEASE WITH THIS WITHOUT TESTING
    // should be able to do an async proxy or other better fastpaths stuff
    ByteStreamUploader localUploader = createStubUploader(createChannel(config.getPublicName()));

    // FIXME factor into pipeline factory/constructor
    WorkerContext context = new WorkerContext() {
      public Poller createPoller(String name, String operationName, Stage stage) {
        return createPoller(name, operationName, stage, () -> {});
      }

      public Poller createPoller(String name, String operationName, Stage stage, Runnable onFailure) {
        return new Poller(null, null) {
          @Override
          public void stop() {
          }
        };
      }

      @Override
      public DigestUtil getDigestUtil() {
        return instance.getDigestUtil();
      }

      @Override
      public void match(Predicate<Operation> onMatch) throws InterruptedException {
        instance.match(config.getPlatform(), config.getRequeueOnFailure(), onMatch);
        if (Thread.currentThread().isInterrupted()) {
          throw new InterruptedException();
        }
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
      public boolean getLinkInputDirectories() {
        return false;
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
      public ByteStreamUploader getUploader() {
        return localUploader;
      }

      @Override
      public ByteString getBlob(Digest digest) {
        return instance.fetchBlob(digest);
      }

      @Override
      public void createActionRoot(Path actionRoot, Action action) throws IOException, InterruptedException {
        OutputDirectory outputDirectory = OutputDirectory.parse(
            action.getOutputFilesList(),
            action.getOutputDirectoriesList());

        if (Files.exists(actionRoot)) {
          CASFileCache.removeDirectory(actionRoot);
        }
        Files.createDirectories(actionRoot);

        ImmutableList.Builder<Path> inputFiles = new ImmutableList.Builder<>();
        ImmutableList.Builder<Digest> inputDirectories = new ImmutableList.Builder<>();

        fetchInputs(
            actionRoot,
            action.getInputRootDigest(),
            outputDirectory,
            inputFiles,
            inputDirectories);

        outputDirectory.stamp(actionRoot);

        rootInputFiles.put(actionRoot, inputFiles.build());
        rootInputDirectories.put(actionRoot, inputDirectories.build());
      }

      @Override
      public void destroyActionRoot(Path actionRoot) throws IOException {
        Iterable<Path> inputFiles = rootInputFiles.remove(actionRoot);
        Iterable<Digest> inputDirectories = rootInputDirectories.remove(actionRoot);
        if (inputFiles != null || inputDirectories == null) {
          fileCache.decrementReferences(
              inputFiles == null ? ImmutableList.<Path>of() : inputFiles,
              inputDirectories == null ? ImmutableList.<Digest>of() : inputDirectories);
        }
        removeDirectory(actionRoot);
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

      @Override
      public boolean putOperation(Operation operation) {
        return instance.putOperation(operation);
      }
    };

    PipelineStage errorStage = new ReportResultStage.NullStage(); /* ErrorStage(); */
    PipelineStage reportResultStage = new ReportResultStage(context, errorStage);
    PipelineStage executeActionStage = new ExecuteActionStage(context, reportResultStage, errorStage);
    reportResultStage.setInput(executeActionStage);
    PipelineStage inputFetchStage = new InputFetchStage(context, executeActionStage, errorStage);
    executeActionStage.setInput(inputFetchStage);
    PipelineStage matchStage = new MatchStage(context, inputFetchStage, errorStage);
    inputFetchStage.setInput(matchStage);

    pipeline = new Pipeline(inputFetchStage);
    // pipeline.add(errorStage, 0);
    pipeline.add(matchStage, 1);
    pipeline.add(inputFetchStage, 2);
    pipeline.add(executeActionStage, 3);
    pipeline.add(reportResultStage, 4);
  }

  public void stop() {
    System.err.println("Closing the pipeline");
    pipeline.close();
    if (server != null) {
      System.err.println("Shutting down the server");
      server.shutdown();
    }
  }

  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  public void start() throws IOException {
    pipeline.start();
    try {
      fileCache.start();
      server.start();
    } catch (IOException ex) {
      ex.printStackTrace();
      return;
    }
    backplane.addWorker(config.getPublicName());
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

    return builder.build();
  }

  private static void printUsage(OptionsParser parser) {
    System.out.println("Usage: CONFIG_PATH");
    System.out.println(parser.describeOptions(Collections.<String, String>emptyMap(),
                                              OptionsParser.HelpVerbosity.LONG));
  }

  public static void main(String[] args) throws Exception {
    OptionsParser parser = OptionsParser.newOptionsParser(WorkerOptions.class);
    parser.parseAndExitUponError(args);
    List<String> residue = parser.getResidue();
    if (residue.isEmpty()) {
      printUsage(parser);
      throw new IllegalArgumentException("Missing CONFIG_PATH");
    }
    Path configPath = Paths.get(residue.get(0));
    try (InputStream configInputStream = Files.newInputStream(configPath)) {
      Worker worker = new Worker(toShardWorkerConfig(new InputStreamReader(configInputStream), parser.getOptions(WorkerOptions.class)));
      configInputStream.close();
      worker.start();
      worker.blockUntilShutdown();
    }
  }
}
