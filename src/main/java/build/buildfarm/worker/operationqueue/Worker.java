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

package build.buildfarm.worker.operationqueue;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.Instance.MatchListener;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.Chunker;
import build.buildfarm.instance.stub.Retrier;
import build.buildfarm.instance.stub.Retrier.Backoff;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.v1test.CASInsertionPolicy;
import build.buildfarm.v1test.InstanceEndpoint;
import build.buildfarm.v1test.WorkerConfig;
import build.buildfarm.worker.CASFileCache;
import build.buildfarm.worker.ExecuteActionStage;
import build.buildfarm.worker.InputFetchStage;
import build.buildfarm.worker.InputStreamFactory;
import build.buildfarm.worker.MatchStage;
import build.buildfarm.worker.OutputDirectory;
import build.buildfarm.worker.Pipeline;
import build.buildfarm.worker.PipelineStage;
import build.buildfarm.worker.Poller;
import build.buildfarm.worker.PutOperationStage;
import build.buildfarm.worker.ReportResultStage;
import build.buildfarm.worker.UploadManifest;
import build.buildfarm.worker.WorkerContext;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.devtools.common.options.OptionsParser;
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.DirectoryNode;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata.Stage;
import com.google.devtools.remoteexecution.v1test.FileNode;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.logging.Logger;
import javax.naming.ConfigurationException;

public class Worker {
  public static final Logger logger = Logger.getLogger(Worker.class.getName());
  private final Instance casInstance;
  private final Instance acInstance;
  private final Instance operationQueueInstance;
  private final ByteStreamUploader uploader;
  private final WorkerConfig config;
  private final Path root;
  private final CASFileCache fileCache;
  private final Map<Path, Iterable<Path>> rootInputFiles = new ConcurrentHashMap<>();
  private final Map<Path, Iterable<Digest>> rootInputDirectories = new ConcurrentHashMap<>();

  private static final ListeningScheduledExecutorService retryScheduler =
      MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(1));
  private final Set<String> activeOperations = new ConcurrentSkipListSet<>();

  private static ManagedChannel createChannel(String target) {
    NettyChannelBuilder builder =
        NettyChannelBuilder.forTarget(target)
            .negotiationType(NegotiationType.PLAINTEXT);
    return builder.build();
  }

  private static Path getValidRoot(WorkerConfig config, FileSystem fileSystem) throws ConfigurationException {
    String rootValue = config.getRoot();
    if (Strings.isNullOrEmpty(rootValue)) {
      throw new ConfigurationException("root value in config missing");
    }
    return fileSystem.getPath(rootValue);
  }

  private static Path getValidCasCacheDirectory(WorkerConfig config, Path root) throws ConfigurationException {
    String casCacheValue = config.getCasCacheDirectory();
    if (Strings.isNullOrEmpty(casCacheValue)) {
      throw new ConfigurationException("Cas cache directory value in config missing");
    }
    return root.resolve(casCacheValue);
  }

  private static HashFunction getValidHashFunction(WorkerConfig config) throws ConfigurationException {
    try {
      return HashFunction.get(config.getHashFunction());
    } catch (IllegalArgumentException e) {
      throw new ConfigurationException("hash_function value unrecognized");
    }
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

  private static ByteStreamUploader createStubUploader(Channel channel) {
    return new ByteStreamUploader("", channel, null, 300, createStubRetrier(), retryScheduler);
  }

  private static Instance createInstance(
      InstanceEndpoint instanceEndpoint,
      DigestUtil digestUtil) {
    return createInstance(
        instanceEndpoint.getInstanceName(),
        digestUtil,
        createChannel(instanceEndpoint.getTarget()),
        null);
  }

  private static Instance createInstance(
      String name,
      DigestUtil digestUtil,
      ManagedChannel channel,
      ByteStreamUploader uploader) {
    return new StubInstance(
        name,
        digestUtil,
        channel,
        60 /* FIXME CONFIG */, TimeUnit.SECONDS,
        createStubRetrier(),
        uploader);
  }

  public Worker(WorkerConfig config) throws ConfigurationException {
    this(config, FileSystems.getDefault());
  }

  public Worker(WorkerConfig config, FileSystem fileSystem) throws ConfigurationException {
    this.config = config;

    /* configuration validation */
    root = getValidRoot(config, fileSystem);
    Path casCacheDirectory = getValidCasCacheDirectory(config, root);
    HashFunction hashFunction = getValidHashFunction(config);

    /* initialization */
    DigestUtil digestUtil = new DigestUtil(hashFunction);
    InstanceEndpoint casEndpoint = config.getContentAddressableStorage();
    ManagedChannel casChannel = createChannel(casEndpoint.getTarget());
    uploader = createStubUploader(casChannel);
    casInstance = createInstance(casEndpoint.getInstanceName(), digestUtil, casChannel, uploader);
    acInstance = createInstance(config.getActionCache(), digestUtil);
    operationQueueInstance = createInstance(config.getOperationQueue(), digestUtil);
    InputStreamFactory inputStreamFactory = new InputStreamFactory() {
      @Override
      public InputStream newInput(Digest digest) throws IOException, InterruptedException {
        return casInstance.newStreamInput(casInstance.getBlobName(digest));
      }
    };
    fileCache = new CASFileCache(
        inputStreamFactory,
        root.resolve(casCacheDirectory),
        config.getCasCacheMaxSizeBytes(),
        casInstance.getDigestUtil());
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
      pageToken = casInstance.getTree(inputRoot, config.getTreePageSize(), pageToken, directories, /*acceptMissing=*/ false);
    } while (!pageToken.isEmpty());

    Set<Digest> directoryDigests = new HashSet<>();
    ImmutableMap.Builder<Digest, Directory> directoriesIndex = new ImmutableMap.Builder<>();
    for (Directory directory : directories.build()) {
      Digest directoryDigest = casInstance.getDigestUtil().compute(directory);
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
        throw new IOException("InputFetchStage: Failed to create cache entry for " + execPath);
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

  private static UploadManifest createManifest(
      ActionResult.Builder result,
      DigestUtil digestUtil,
      Path execRoot,
      Iterable<String> outputFiles,
      Iterable<String> outputDirs,
      int inlineContentLimit,
      CASInsertionPolicy fileCasPolicy,
      CASInsertionPolicy stdoutCasPolicy,
      CASInsertionPolicy stderrCasPolicy)
      throws IOException, InterruptedException {
    UploadManifest manifest = new UploadManifest(
        digestUtil,
        result,
        execRoot,
        /* allowSymlinks= */ true,
        inlineContentLimit);

    manifest.addFiles(
        Iterables.transform(outputFiles, (file) -> execRoot.resolve(file)),
        fileCasPolicy);
    manifest.addFiles(
        Iterables.transform(outputDirs, (dir) -> execRoot.resolve(dir)),
        fileCasPolicy);

    /* put together our outputs and update the result */
    if (result.getStdoutRaw().size() > 0) {
      manifest.addContent(
          result.getStdoutRaw(),
          stdoutCasPolicy,
          result::setStdoutRaw,
          result::setStdoutDigest);
    }
    if (result.getStderrRaw().size() > 0) {
      manifest.addContent(
          result.getStderrRaw(),
          stderrCasPolicy,
          result::setStderrRaw,
          result::setStderrDigest);
    }

    return manifest;
  }

  private static void uploadManifest(UploadManifest manifest, ByteStreamUploader uploader)
      throws IOException, InterruptedException {
    List<Chunker> filesToUpload = new ArrayList<>();

    Map<Digest, Path> digestToFile = manifest.getDigestToFile();
    Map<Digest, Chunker> digestToChunkers = manifest.getDigestToChunkers();
    Collection<Digest> digests = new ArrayList<>();
    digests.addAll(digestToFile.keySet());
    digests.addAll(digestToChunkers.keySet());

    for (Digest digest : digests) {
      Chunker chunker;
      Path file = digestToFile.get(digest);
      if (file != null) {
        chunker = new Chunker(file, digest);
      } else {
        chunker = digestToChunkers.get(digest);
        if (chunker == null) {
          String message = "FindMissingBlobs call returned an unknown digest: " + digest;
          throw new IOException(message);
        }
      }
      filesToUpload.add(chunker);
    }

    if (!filesToUpload.isEmpty()) {
      uploader.uploadBlobs(filesToUpload);
    }
  }

  @VisibleForTesting
  public static void uploadOutputs(
      ActionResult.Builder resultBuilder,
      DigestUtil digestUtil,
      Path actionRoot,
      Iterable<String> outputFiles,
      Iterable<String> outputDirs,
      ByteStreamUploader uploader,
      int inlineContentLimit,
      CASInsertionPolicy fileCasPolicy,
      CASInsertionPolicy stdoutCasPolicy,
      CASInsertionPolicy stderrCasPolicy)
      throws IOException, InterruptedException {
    uploadManifest(
        createManifest(
            resultBuilder,
            digestUtil,
            actionRoot,
            outputFiles,
            outputDirs,
            inlineContentLimit,
            fileCasPolicy,
            stdoutCasPolicy,
            stderrCasPolicy),
        uploader);
  }

  public void start() throws InterruptedException {
    try {
      Files.createDirectories(root);
      fileCache.start();
    } catch(IOException e) {
      e.printStackTrace();
      return;
    }

    WorkerContext context = new WorkerContext() {
      @Override
      public String getName() {
        try {
          return java.net.InetAddress.getLocalHost().getHostName();
        } catch (java.net.UnknownHostException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public Poller createPoller(String name, String operationName, Stage stage) {
        return createPoller(name, operationName, stage, () -> {});
      }

      @Override
      public Poller createPoller(String name, String operationName, Stage stage, Runnable onFailure) {
        Poller poller = new Poller(config.getOperationPollPeriod(), () -> {
              boolean success = operationQueueInstance.pollOperation(operationName, stage);
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
        return casInstance.getDigestUtil();
      }

      @Override
      public void match(MatchListener listener) throws InterruptedException {
        operationQueueInstance.match(config.getPlatform(), new MatchListener() {
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
            return true;
          }

          @Override
          public boolean onOperation(Operation operation) {
            if (activeOperations.contains(operation.getName())) {
              System.err.println("WorkerContext::match: WARNING matched duplicate operation " + operation.getName());
              return listener.onOperation(null);
            }
            activeOperations.add(operation.getName());
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

      @Override
      public void requeue(Operation operation) throws InterruptedException {
        try {
          deactivate(operation);
          ExecuteOperationMetadata metadata =
              operation.getMetadata().unpack(ExecuteOperationMetadata.class);

          ExecuteOperationMetadata executingMetadata = metadata.toBuilder()
              .setStage(ExecuteOperationMetadata.Stage.QUEUED)
              .build();

          operation = operation.toBuilder()
              .setMetadata(Any.pack(executingMetadata))
              .build();
          operationQueueInstance.putOperation(operation);
        } catch (InvalidProtocolBufferException e) {
          e.printStackTrace();
        }
      }

      @Override
      public void deactivate(Operation operation) {
        activeOperations.remove(operation.getName());
      }

      @Override
      public boolean putOperation(Operation operation, Action action) throws InterruptedException {
        return operationQueueInstance.putOperation(operation);
      }

      @Override
      public void logInfo(String msg) {
        logger.info(msg);
      }

      @Override
      public int getInlineContentLimit() {
        return config.getInlineContentLimit();
      }

      @Override
      public CASInsertionPolicy getFileCasPolicy() {
        return config.getFileCasPolicy();
      }

      @Override
      public CASInsertionPolicy getStdoutCasPolicy() {
        return config.getStdoutCasPolicy();
      }

      @Override
      public CASInsertionPolicy getStderrCasPolicy() {
        return config.getStderrCasPolicy();
      }

      @Override
      public int getExecuteStageWidth() {
        return config.getExecuteStageWidth();
      }

      @Override
      public int getTreePageSize() {
        return config.getTreePageSize();
      }

      @Override
      public boolean hasDefaultActionTimeout() {
        return config.hasDefaultActionTimeout();
      }

      @Override
      public boolean hasMaximumActionTimeout() {
        return config.hasMaximumActionTimeout();
      }

      @Override
      public boolean getStreamStdout() {
        return config.getStreamStdout();
      }

      @Override
      public boolean getStreamStderr() {
        return config.getStreamStderr();
      }

      @Override
      public Duration getDefaultActionTimeout() {
        return config.getDefaultActionTimeout();
      }

      @Override
      public Duration getMaximumActionTimeout() {
        return config.getMaximumActionTimeout();
      }

      @Override
      public void uploadOutputs(
          ActionResult.Builder resultBuilder,
          Path actionRoot,
          Iterable<String> outputFiles,
          Iterable<String> outputDirs)
          throws IOException, InterruptedException {
        Worker.uploadOutputs(
            resultBuilder,
            casInstance.getDigestUtil(),
            actionRoot,
            outputFiles,
            outputDirs,
            uploader,
            config.getInlineContentLimit(),
            config.getFileCasPolicy(),
            config.getStdoutCasPolicy(),
            config.getStderrCasPolicy());
      }

      @Override
      public ByteString getBlob(Digest digest) throws InterruptedException, IOException {
        return casInstance.getBlob(digest);
      }

      @Override
      public void createActionRoot(Path root, Map<Digest, Directory> directoriesIndex, Action action) throws IOException, InterruptedException {
        OutputDirectory outputDirectory = OutputDirectory.parse(
            action.getOutputFilesList(),
            action.getOutputDirectoriesList());

        if (Files.exists(root)) {
          CASFileCache.removeDirectory(root);
        }
        Files.createDirectories(root);

        ImmutableList.Builder<Path> inputFiles = new ImmutableList.Builder<>();
        ImmutableList.Builder<Digest> inputDirectories = new ImmutableList.Builder<>();

        fetchInputs(
            root,
            action.getInputRootDigest(),
            outputDirectory,
            inputFiles,
            inputDirectories);

        outputDirectory.stamp(root);

        rootInputFiles.put(root, inputFiles.build());
        rootInputDirectories.put(root, inputDirectories.build());
      }

      @Override
      public void destroyActionRoot(Path root) throws IOException {
        Iterable<Path> inputFiles = rootInputFiles.remove(root);
        Iterable<Digest> inputDirectories = rootInputDirectories.remove(root);

        fileCache.decrementReferences(inputFiles, inputDirectories);
        CASFileCache.removeDirectory(root);
      }

      @Override
      public Path getRoot() {
        return root;
      }

      @Override
      public void removeDirectory(Path path) throws IOException {
        CASFileCache.removeDirectory(path);
      }

      // doesn't belong in CAS or AC, must be in OQ
      @Override
      public OutputStream getStreamOutput(String name) {
        return operationQueueInstance.getStreamOutput(name);
      }

      @Override
      public void putActionResult(ActionKey actionKey, ActionResult actionResult) {
        acInstance.putActionResult(actionKey, actionResult);
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

    Pipeline pipeline = new Pipeline();
    // pipeline.add(errorStage, 0);
    pipeline.add(matchStage, 1);
    pipeline.add(inputFetchStage, 2);
    pipeline.add(executeActionStage, 3);
    pipeline.add(reportResultStage, 4);
    pipeline.start();
    pipeline.join(); // uninterruptable
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
  }

  private static WorkerConfig toWorkerConfig(Readable input, WorkerOptions options) throws IOException {
    WorkerConfig.Builder builder = WorkerConfig.newBuilder();
    TextFormat.merge(input, builder);
    if (!Strings.isNullOrEmpty(options.root)) {
      builder.setRoot(options.root);
    }

    if (!Strings.isNullOrEmpty(options.casCacheDirectory)) {
      builder.setCasCacheDirectory(options.casCacheDirectory);
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
    Worker worker;
    try (InputStream configInputStream = Files.newInputStream(configPath)) {
      worker = new Worker(toWorkerConfig(new InputStreamReader(configInputStream), parser.getOptions(WorkerOptions.class)));
    }
    worker.start();
  }
}
