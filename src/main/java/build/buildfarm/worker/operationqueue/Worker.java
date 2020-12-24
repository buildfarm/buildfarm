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

import static build.buildfarm.common.io.Utils.formatIOError;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.cas.cfc.CASFileCache;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.common.LoggingMain;
import build.buildfarm.common.grpc.Retrier;
import build.buildfarm.common.grpc.Retrier.Backoff;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.v1test.InstanceEndpoint;
import build.buildfarm.v1test.WorkerConfig;
import build.buildfarm.worker.ExecuteActionStage;
import build.buildfarm.worker.InputFetchStage;
import build.buildfarm.worker.MatchStage;
import build.buildfarm.worker.Pipeline;
import build.buildfarm.worker.PipelineStage;
import build.buildfarm.worker.PutOperationStage;
import build.buildfarm.worker.ReportResultStage;
import build.buildfarm.worker.WorkerContext;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.devtools.common.options.OptionsParser;
import com.google.protobuf.TextFormat;
import com.google.protobuf.util.Durations;
import io.grpc.Channel;
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
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;
import javax.naming.ConfigurationException;

public class Worker extends LoggingMain {
  private static final Logger logger = Logger.getLogger(Worker.class.getName());

  private final Instance casInstance;
  private final Instance operationQueueInstance;
  private final ByteStreamUploader uploader;
  private final WorkerConfig config;
  private final Path root;
  private final CASFileCache fileCache;
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
    casInstance =
        newStubInstance(
            casEndpoint.getInstanceName(),
            casChannel,
            digestUtil,
            casEndpoint.getDeadlineAfterSeconds());
    uploader = createStubUploader(casInstance.getName(), casChannel, retrier);
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

    Instance acInstance = newStubInstance(config.getActionCache(), casInstance.getDigestUtil());
    WorkerContext context =
        new OperationQueueWorkerContext(
            config, casInstance, acInstance, oq, uploader, fileCache, root, retrier);

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
      logger.log(INFO, "Closing the pipeline");
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
    logger.log(INFO, "Usage: CONFIG_PATH");
    logger.log(
        INFO, parser.describeOptions(Collections.emptyMap(), OptionsParser.HelpVerbosity.LONG));
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
