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

import static build.buildfarm.cas.CASFileCache.getInterruptiblyOrIOException;
import static build.buildfarm.common.IOUtils.formatIOError;
import static build.buildfarm.instance.Utils.getBlob;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.logging.Level.SEVERE;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.ExecutionStage;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.cas.CASFileCache;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.common.LoggingMain;
import build.buildfarm.common.Poller;
import build.buildfarm.common.Write;
import build.buildfarm.common.grpc.Retrier;
import build.buildfarm.common.grpc.Retrier.Backoff;
import build.buildfarm.common.io.Directories;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.Instance.MatchListener;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.Chunker;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.v1test.CASInsertionPolicy;
import build.buildfarm.v1test.ExecutionPolicy;
import build.buildfarm.v1test.InstanceEndpoint;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.QueuedOperation;
import build.buildfarm.v1test.WorkerConfig;
import build.buildfarm.worker.ExecuteActionStage;
import build.buildfarm.worker.ExecutionPolicies;
import build.buildfarm.worker.InputFetchStage;
import build.buildfarm.worker.MatchStage;
import build.buildfarm.worker.OutputDirectory;
import build.buildfarm.worker.Pipeline;
import build.buildfarm.worker.PipelineStage;
import build.buildfarm.worker.PutOperationStage;
import build.buildfarm.worker.ReportResultStage;
import build.buildfarm.worker.UploadManifest;
import build.buildfarm.worker.WorkerContext;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.devtools.common.options.OptionsParser;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import com.google.protobuf.util.Durations;
import io.grpc.Channel;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.naming.ConfigurationException;

public class Worker extends LoggingMain {
  private static final Logger logger = Logger.getLogger(Worker.class.getName());

  private final Instance casInstance;
  private final Instance acInstance;
  private final Instance operationQueueInstance;
  private final ByteStreamUploader uploader;
  private final WorkerConfig config;
  private final Path root;
  private final CASFileCache fileCache;
  private final Map<Path, Iterable<String>> rootInputFiles = new ConcurrentHashMap<>();
  private final Map<Path, Iterable<Digest>> rootInputDirectories = new ConcurrentHashMap<>();
  private Pipeline pipeline;

  private static final ListeningScheduledExecutorService retryScheduler =
      listeningDecorator(newSingleThreadScheduledExecutor());
  private static final Retrier retrier = createStubRetrier();

  private static ManagedChannel createChannel(String target) {
    NettyChannelBuilder builder =
        NettyChannelBuilder.forTarget(target).negotiationType(NegotiationType.PLAINTEXT);
    return builder.build();
  }

  private static Path getValidRoot(WorkerConfig config, FileSystem fileSystem)
      throws ConfigurationException {
    String rootValue = config.getRoot();
    if (Strings.isNullOrEmpty(rootValue)) {
      throw new ConfigurationException("root value in config missing");
    }
    return fileSystem.getPath(rootValue);
  }

  private static Path getValidCasCacheDirectory(WorkerConfig config, Path root)
      throws ConfigurationException {
    String casCacheValue = config.getCasCacheDirectory();
    if (Strings.isNullOrEmpty(casCacheValue)) {
      throw new ConfigurationException("Cas cache directory value in config missing");
    }
    return root.resolve(casCacheValue);
  }

  private static HashFunction getValidHashFunction(WorkerConfig config)
      throws ConfigurationException {
    try {
      return HashFunction.get(config.getDigestFunction());
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
        Retrier.DEFAULT_IS_RETRIABLE,
        retryScheduler);
  }

  private static ByteStreamUploader createStubUploader(
      String instanceName, Channel channel, Retrier retrier) {
    return new ByteStreamUploader(instanceName, channel, null, 300, retrier);
  }

  private static Instance newStubInstance(
      InstanceEndpoint instanceEndpoint, DigestUtil digestUtil) {
    return newStubInstance(
        instanceEndpoint.getInstanceName(),
        createChannel(instanceEndpoint.getTarget()),
        digestUtil,
        instanceEndpoint.getDeadlineAfterSeconds());
  }

  private static Instance newStubInstance(
      String name, ManagedChannel channel, DigestUtil digestUtil, long deadlineAfterSeconds) {
    return new StubInstance(
        name,
        /* identifier=*/ "",
        digestUtil,
        channel,
        Durations.fromSeconds(deadlineAfterSeconds),
        retrier,
        retryScheduler);
  }

  public Worker(WorkerConfig config) throws ConfigurationException {
    this(config, FileSystems.getDefault());
  }

  public Worker(WorkerConfig config, FileSystem fileSystem) throws ConfigurationException {
    super("BuildFarmOperationQueueWorker");
    this.config = config;

    /* configuration validation */
    root = getValidRoot(config, fileSystem);
    Path casCacheDirectory = getValidCasCacheDirectory(config, root);
    HashFunction hashFunction = getValidHashFunction(config);

    /* initialization */
    DigestUtil digestUtil = new DigestUtil(hashFunction);
    InstanceEndpoint casEndpoint = config.getContentAddressableStorage();
    ManagedChannel casChannel = createChannel(casEndpoint.getTarget());
    uploader = createStubUploader(casEndpoint.getInstanceName(), casChannel, retrier);
    casInstance =
        newStubInstance(
            casEndpoint.getInstanceName(),
            casChannel,
            digestUtil,
            casEndpoint.getDeadlineAfterSeconds());
    acInstance = newStubInstance(config.getActionCache(), digestUtil);
    operationQueueInstance = newStubInstance(config.getOperationQueue(), digestUtil);
    InputStreamFactory inputStreamFactory =
        new InputStreamFactory() {
          @Override
          public InputStream newInput(Digest digest, long offset) throws IOException {
            return casInstance.newBlobInput(
                digest, offset, 60, SECONDS, RequestMetadata.getDefaultInstance());
          }
        };
    fileCache =
        new InjectedCASFileCache(
            inputStreamFactory,
            root.resolve(casCacheDirectory),
            config.getCasCacheMaxSizeBytes(),
            config.getCasCacheMaxEntrySizeBytes(),
            /* storeFileDirsIndexInMemory= */ true,
            casInstance.getDigestUtil(),
            newDirectExecutorService(),
            directExecutor());
  }

  private void fetchInputs(
      Path execDir,
      Digest inputRoot,
      Map<Digest, Directory> directoriesIndex,
      OutputDirectory outputDirectory,
      ImmutableList.Builder<String> inputFiles,
      ImmutableList.Builder<Digest> inputDirectories)
      throws IOException, InterruptedException {
    Directory directory;
    if (inputRoot.getSizeBytes() == 0) {
      directory = Directory.getDefaultInstance();
    } else {
      directory = directoriesIndex.get(inputRoot);
      if (directory == null) {
        throw new IOException(
            "Directory " + DigestUtil.toString(inputRoot) + " is not in input index");
      }
    }

    getInterruptiblyOrIOException(
        allAsList(
            fileCache.putFiles(
                directory.getFilesList(), execDir, inputFiles, newDirectExecutorService())));
    for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
      Digest digest = directoryNode.getDigest();
      String name = directoryNode.getName();
      OutputDirectory childOutputDirectory =
          outputDirectory != null ? outputDirectory.getChild(name) : null;
      Path dirPath = execDir.resolve(name);
      if (childOutputDirectory != null || !config.getLinkInputDirectories()) {
        Files.createDirectories(dirPath);
        fetchInputs(
            dirPath, digest, directoriesIndex, childOutputDirectory, inputFiles, inputDirectories);
      } else {
        inputDirectories.add(digest);
        linkDirectory(dirPath, digest, directoriesIndex);
      }
    }
  }

  private void linkDirectory(Path execPath, Digest digest, Map<Digest, Directory> directoriesIndex)
      throws IOException, InterruptedException {
    Path cachePath =
        getInterruptiblyOrIOException(
            fileCache.putDirectory(digest, directoriesIndex, newDirectExecutorService()));
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
    UploadManifest manifest =
        new UploadManifest(
            digestUtil, result, execRoot, /* allowSymlinks= */ true, inlineContentLimit);

    manifest.addFiles(
        Iterables.transform(outputFiles, (file) -> execRoot.resolve(file)), fileCasPolicy);
    manifest.addDirectories(Iterables.transform(outputDirs, (dir) -> execRoot.resolve(dir)));

    /* put together our outputs and update the result */
    if (result.getStdoutRaw().size() > 0) {
      manifest.addContent(
          result.getStdoutRaw(), stdoutCasPolicy, result::setStdoutRaw, result::setStdoutDigest);
    }
    if (result.getStderrRaw().size() > 0) {
      manifest.addContent(
          result.getStderrRaw(), stderrCasPolicy, result::setStderrRaw, result::setStderrDigest);
    }

    return manifest;
  }

  private static void uploadManifest(UploadManifest manifest, ByteStreamUploader uploader)
      throws IOException, InterruptedException {
    Map<HashCode, Chunker> filesToUpload = Maps.newHashMap();

    Map<Digest, Path> digestToFile = manifest.getDigestToFile();
    Map<Digest, Chunker> digestToChunkers = manifest.getDigestToChunkers();
    Collection<Digest> digests = new ArrayList<>();
    digests.addAll(digestToFile.keySet());
    digests.addAll(digestToChunkers.keySet());

    for (Digest digest : digests) {
      Chunker chunker;
      Path file = digestToFile.get(digest);
      if (file != null) {
        chunker = Chunker.builder().setInput(digest.getSizeBytes(), file).build();
      } else {
        chunker = digestToChunkers.get(digest);
        if (chunker == null) {
          String message = "FindMissingBlobs call returned an unknown digest: " + digest;
          throw new IOException(message);
        }
      }
      filesToUpload.put(HashCode.fromString(digest.getHash()), chunker);
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
      fileCache.start(/* skipLoad= */ false);
    } catch (IOException e) {
      logger.log(SEVERE, "error starting file cache", e);
      return;
    }

    OperationQueueClient oq =
        new OperationQueueClient(
            operationQueueInstance, config.getPlatform(), config.getExecutionPoliciesList());

    WorkerContext context =
        new WorkerContext() {
          ListMultimap<String, ExecutionPolicy> policies =
              ExecutionPolicies.toMultimap(config.getExecutionPoliciesList());

          @Override
          public String getName() {
            try {
              return java.net.InetAddress.getLocalHost().getHostName();
            } catch (java.net.UnknownHostException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public boolean shouldErrorOperationOnRemainingResources() {
            return config.getErrorOperationRemainingResources();
          }

          @Override
          public Poller createPoller(
              String name, QueueEntry queueEntry, ExecutionStage.Value stage) {
            Poller poller = new Poller(config.getOperationPollPeriod());
            resumePoller(poller, name, queueEntry, stage, () -> {}, Deadline.after(10, DAYS));
            return poller;
          }

          @Override
          public void resumePoller(
              Poller poller,
              String name,
              QueueEntry queueEntry,
              ExecutionStage.Value stage,
              Runnable onFailure,
              Deadline deadline) {
            String operationName = queueEntry.getExecuteEntry().getOperationName();
            poller.resume(
                () -> {
                  boolean success = oq.poll(operationName, stage);
                  logger.log(
                      Level.INFO,
                      format(
                          "%s: poller: Completed Poll for %s: %s",
                          name, operationName, success ? "OK" : "Failed"));
                  if (!success) {
                    onFailure.run();
                  }
                  return success;
                },
                () -> {
                  logger.log(
                      Level.INFO,
                      format("%s: poller: Deadline expired for %s", name, operationName));
                  onFailure.run();
                },
                deadline);
          }

          @Override
          public DigestUtil getDigestUtil() {
            return casInstance.getDigestUtil();
          }

          @Override
          public void match(MatchListener listener) throws InterruptedException {
            oq.match(listener);
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
          public int getInputFetchStageWidth() {
            return config.getInputFetchStageWidth();
          }

          @Override
          public int getExecuteStageWidth() {
            return config.getExecuteStageWidth();
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
              Digest actionDigest,
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
          public QueuedOperation getQueuedOperation(QueueEntry queueEntry)
              throws IOException, InterruptedException {
            Digest queuedOperationDigest = queueEntry.getQueuedOperationDigest();
            ByteString queuedOperationBlob =
                getBlob(
                    casInstance,
                    queuedOperationDigest,
                    queueEntry.getExecuteEntry().getRequestMetadata());
            if (queuedOperationBlob == null) {
              return null;
            }
            try {
              return QueuedOperation.parseFrom(queuedOperationBlob);
            } catch (InvalidProtocolBufferException e) {
              logger.log(
                  Level.WARNING,
                  format(
                      "invalid queued operation: %s(%s)",
                      queueEntry.getExecuteEntry().getOperationName(),
                      DigestUtil.toString(queuedOperationDigest)));
              return null;
            }
          }

          @Override
          public Path createExecDir(
              String operationName,
              Map<Digest, Directory> directoriesIndex,
              Action action,
              Command command)
              throws IOException, InterruptedException {
            OutputDirectory outputDirectory =
                OutputDirectory.parse(
                    command.getOutputFilesList(),
                    command.getOutputDirectoriesList(),
                    command.getEnvironmentVariablesList());

            Path execDir = root.resolve(operationName);
            if (Files.exists(execDir)) {
              Directories.remove(execDir);
            }
            Files.createDirectories(execDir);

            ImmutableList.Builder<String> inputFiles = new ImmutableList.Builder<>();
            ImmutableList.Builder<Digest> inputDirectories = new ImmutableList.Builder<>();

            boolean fetched = false;
            try {
              fetchInputs(
                  execDir,
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

            rootInputFiles.put(execDir, inputFiles.build());
            rootInputDirectories.put(execDir, inputDirectories.build());

            boolean stamped = false;
            try {
              outputDirectory.stamp(execDir);
              stamped = true;
            } finally {
              if (!stamped) {
                destroyExecDir(execDir);
              }
            }

            return execDir;
          }

          @Override
          public void destroyExecDir(Path execDir) throws IOException, InterruptedException {
            Iterable<String> inputFiles = rootInputFiles.remove(execDir);
            Iterable<Digest> inputDirectories = rootInputDirectories.remove(execDir);

            fileCache.decrementReferences(inputFiles, inputDirectories);
            Directories.remove(execDir);
          }

          @Override
          public Iterable<ExecutionPolicy> getExecutionPolicies(String name) {
            return policies.get(name);
          }

          @Override
          public boolean putOperation(Operation operation, Action action)
              throws InterruptedException {
            return oq.put(operation);
          }

          // doesn't belong in CAS or AC, must be in OQ
          @Override
          public Write getOperationStreamWrite(String name) {
            return oq.getStreamWrite(name);
          }

          @Override
          public void blacklistAction(String actionId) {
            // ignore
          }

          @Override
          public void putActionResult(ActionKey actionKey, ActionResult actionResult)
              throws InterruptedException {
            try {
              retrier.execute(
                  () -> {
                    acInstance.putActionResult(actionKey, actionResult);
                    return null;
                  });
            } catch (IOException e) {
              Throwable cause = e.getCause();
              if (cause == null) {
                throw new RuntimeException(e);
              }
              Throwables.throwIfUnchecked(cause);
              throw new RuntimeException(cause);
            }
          }

          @Override
          public int getStandardOutputLimit() {
            return 100 * 1024 * 1024; // 100 MiB
          }

          @Override
          public int getStandardErrorLimit() {
            return 100 * 1024 * 1024; // 100 MiB
          }

          @Override
          public void createExecutionLimits() {}

          @Override
          public void destroyExecutionLimits() {}

          @Override
          public IOResource limitExecution(
              String operationName, ImmutableList.Builder<String> arguments, Command command) {
            return new IOResource() {
              @Override
              public void close() {}

              @Override
              public boolean isReferenced() {
                return false;
              }
            };
          }

          @Override
          public int commandExecutionClaims(Command command) {
            return 1;
          }
        };

    PipelineStage completeStage =
        new PutOperationStage((operation) -> oq.deactivate(operation.getName()));
    PipelineStage errorStage = completeStage; /* new ErrorStage(); */
    PipelineStage reportResultStage = new ReportResultStage(context, completeStage, errorStage);
    PipelineStage executeActionStage =
        new ExecuteActionStage(context, reportResultStage, errorStage);
    PipelineStage inputFetchStage =
        new InputFetchStage(context, executeActionStage, new PutOperationStage(oq::requeue));
    PipelineStage matchStage = new MatchStage(context, inputFetchStage, errorStage);

    pipeline = new Pipeline();
    // pipeline.add(errorStage, 0);
    pipeline.add(matchStage, 4);
    pipeline.add(inputFetchStage, 3);
    pipeline.add(executeActionStage, 2);
    pipeline.add(reportResultStage, 1);
    pipeline.start();
    pipeline.join(); // uninterruptable
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
    stop();
  }

  @Override
  protected void onShutdown() throws InterruptedException {
    stop();
  }

  private void stop() throws InterruptedException {
    boolean interrupted = Thread.interrupted();
    if (pipeline != null) {
      logger.log(Level.INFO, "Closing the pipeline");
      try {
        pipeline.close();
      } catch (InterruptedException e) {
        Thread.interrupted();
        interrupted = true;
      }
      pipeline = null;
    }
    if (!shutdownAndAwaitTermination(retryScheduler, 1, MINUTES)) {
      logger.log(SEVERE, "unable to terminate retry scheduler");
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
      throw new InterruptedException();
    }
  }

  private static WorkerConfig toWorkerConfig(Readable input, WorkerOptions options)
      throws IOException {
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
    logger.log(Level.INFO, "Usage: CONFIG_PATH");
    logger.log(
        Level.INFO,
        parser.describeOptions(Collections.emptyMap(), OptionsParser.HelpVerbosity.LONG));
  }

  /** returns success or failure */
  static boolean workerMain(String[] args) {
    OptionsParser parser = OptionsParser.newOptionsParser(WorkerOptions.class);
    parser.parseAndExitUponError(args);
    List<String> residue = parser.getResidue();
    if (residue.isEmpty()) {
      printUsage(parser);
      return false;
    }
    Path configPath = Paths.get(residue.get(0));
    try (InputStream configInputStream = Files.newInputStream(configPath)) {
      Worker worker =
          new Worker(
              toWorkerConfig(
                  new InputStreamReader(configInputStream),
                  parser.getOptions(WorkerOptions.class)));
      configInputStream.close();
      worker.start();
      return true;
    } catch (IOException e) {
      System.err.println("error: " + formatIOError(e));
    } catch (ConfigurationException e) {
      System.err.println("error: " + e.getMessage());
    } catch (InterruptedException e) {
      System.err.println("error: interrupted");
    }
    return false;
  }

  public static void main(String[] args) {
    try {
      System.exit(workerMain(args) ? 0 : 1);
    } catch (Exception e) {
      logger.log(SEVERE, "exception caught", e);
      System.exit(1);
    }
  }
}
