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

import static build.buildfarm.cas.ContentAddressableStorages.createGrpcCAS;
import static build.buildfarm.common.io.Utils.getUser;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.backplane.Backplane;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.cas.MemoryCAS;
import build.buildfarm.cas.cfc.CASFileCache;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.common.LoggingMain;
import build.buildfarm.common.Size;
import build.buildfarm.common.Write;
import build.buildfarm.common.function.IOSupplier;
import build.buildfarm.common.io.FeedbackOutputStream;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.shard.RedisShardBackplane;
import build.buildfarm.instance.shard.RemoteInputStreamFactory;
import build.buildfarm.instance.shard.WorkerStubs;
import build.buildfarm.metrics.prometheus.PrometheusPublisher;
import build.buildfarm.server.ByteStreamService;
import build.buildfarm.server.ContentAddressableStorageService;
import build.buildfarm.v1test.AdminGrpc;
import build.buildfarm.v1test.ContentAddressableStorageConfig;
import build.buildfarm.v1test.DisableScaleInProtectionRequest;
import build.buildfarm.v1test.FilesystemCASConfig;
import build.buildfarm.v1test.ShardWorker;
import build.buildfarm.v1test.ShardWorkerConfig;
import build.buildfarm.worker.DequeueMatchSettings;
import build.buildfarm.worker.ExecuteActionStage;
import build.buildfarm.worker.FuseCAS;
import build.buildfarm.worker.InputFetchStage;
import build.buildfarm.worker.MatchStage;
import build.buildfarm.worker.Pipeline;
import build.buildfarm.worker.PipelineStage;
import build.buildfarm.worker.PutOperationStage;
import build.buildfarm.worker.ReportResultStage;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.devtools.common.options.OptionsParser;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.TextFormat;
import com.google.protobuf.util.Durations;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.services.HealthStatusManager;
import io.prometheus.client.Counter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.UserPrincipal;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import javax.naming.ConfigurationException;

public class Worker extends LoggingMain {
  private static final java.util.logging.Logger nettyLogger =
      java.util.logging.Logger.getLogger("io.grpc.netty");
  private static final Logger logger = Logger.getLogger(Worker.class.getName());
  private static final Counter healthCheckMetric =
      Counter.build()
          .name("health_check")
          .labelNames("lifecycle")
          .help("Service health check.")
          .register();

  private static final int shutdownWaitTimeInSeconds = 10;
  private final boolean isCasShard;

  private boolean inGracefulShutdown = false;

  private final ShardWorkerConfig config;
  private final ShardWorkerInstance instance;

  @SuppressWarnings("deprecation")
  private final HealthStatusManager healthStatusManager = new HealthStatusManager();

  private final Server server;
  private final Path root;
  private final DigestUtil digestUtil;
  private final ExecFileSystem execFileSystem;
  private final Pipeline pipeline;
  private final Backplane backplane;
  private final LoadingCache<String, Instance> workerStubs;

  class LocalCasWriter implements CasWriter {
    public void write(Digest digest, Path file) throws IOException, InterruptedException {
      insertStream(digest, () -> Files.newInputStream(file));
    }

    public void insertBlob(Digest digest, ByteString content)
        throws IOException, InterruptedException {
      insertStream(digest, content::newInput);
    }

    private Write getLocalWrite(Digest digest) throws IOException {
      return execFileSystem
          .getStorage()
          .getWrite(digest, UUID.randomUUID(), RequestMetadata.getDefaultInstance());
    }

    private void insertStream(Digest digest, IOSupplier<InputStream> suppliedStream)
        throws IOException, InterruptedException {
      Write write = getLocalWrite(digest);

      try (OutputStream out =
              write.getOutput(/* deadlineAfter=*/ 1, /* deadlineAfterUnits=*/ DAYS, () -> {});
          InputStream in = suppliedStream.get()) {
        ByteStreams.copy(in, out);
      } catch (IOException e) {
        if (!write.isComplete()) {
          write.reset(); // we will not attempt retry with current behavior, abandon progress
          throw new IOException(Status.RESOURCE_EXHAUSTED.withCause(e).asRuntimeException());
        }
      }
    }
  }

  class RemoteCasWriter implements CasWriter {
    public void write(Digest digest, Path file) throws IOException, InterruptedException {
      insertFileToCasMember(digest, file);
    }

    private void insertFileToCasMember(Digest digest, Path file)
        throws IOException, InterruptedException {
      try (InputStream in = Files.newInputStream(file)) {
        writeToCasMember(digest, in);
      } catch (ExecutionException e) {
        throw new IOException(Status.RESOURCE_EXHAUSTED.withCause(e).asRuntimeException());
      }
    }

    private void writeToCasMember(Digest digest, InputStream in)
        throws IOException, InterruptedException, ExecutionException {
      // create a write for inserting into another CAS member.
      String workerName = getRandomWorker();
      Write write = getCasMemberWrite(digest, workerName);

      streamIntoWriteFuture(in, write, digest).get();
    }

    private Write getCasMemberWrite(Digest digest, String workerName) throws IOException {
      Instance casMember = workerStub(workerName);

      return casMember.getBlobWrite(
          digest, UUID.randomUUID(), RequestMetadata.getDefaultInstance());
    }

    public void insertBlob(Digest digest, ByteString content)
        throws IOException, InterruptedException {
      insertBlobToCasMember(digest, content);
    }

    private void insertBlobToCasMember(Digest digest, ByteString content)
        throws IOException, InterruptedException {
      try (InputStream in = content.newInput()) {
        writeToCasMember(digest, in);
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        Throwables.throwIfUnchecked(cause);
        Throwables.throwIfInstanceOf(cause, IOException.class);
        Status status = Status.fromThrowable(cause);
        throw new IOException(status.asException());
      }
    }
  }

  public Worker(String session, ShardWorkerConfig config) throws ConfigurationException {
    this(session, ServerBuilder.forPort(config.getPort()), config);
  }

  /**
   * The method will prepare the worker for graceful shutdown and send out grpc request to disable
   * scale in protection when the worker is ready. If unexpected errors happened, it will cancel the
   * graceful shutdown progress make the worker available again.
   */
  public void prepareWorkerForGracefulShutdown() {
    inGracefulShutdown = true;
    logger.log(
        Level.INFO,
        "The current worker will not be registered again and should be shutdown gracefully!");
    pipeline.stopMatchingOperations();
    int scanRate = 30; // check every 30 seconds
    int timeWaited = 0;
    int timeOut = 60 * 15; // 15 minutes

    try {
      while (!pipeline.isEmpty() && timeWaited < timeOut) {
        SECONDS.sleep(scanRate);
        timeWaited += scanRate;
        logger.log(
            INFO, String.format("Pipeline is still not empty after %d seconds.", timeWaited));
      }
    } catch (InterruptedException e) {
      logger.log(Level.SEVERE, "The worker gracefully shutdown is interrupted: " + e.getMessage());
    } finally {
      // make a grpc call to disable scale protection
      String clusterEndpoint = config.getAdminConfig().getClusterEndpoint();
      logger.log(
          INFO,
          String.format(
              "It took the worker %d seconds to %s",
              timeWaited,
              pipeline.isEmpty() ? "finish all actions" : "but still cannot finish all actions"));
      try {
        disableScaleInProtection(clusterEndpoint, config.getPublicName());
      } catch (Exception e) {
        logger.log(
            SEVERE,
            String.format(
                "gRPC call to AdminService to disable scale in protection failed with exception: %s and stacktrace %s",
                e.getMessage(), Arrays.toString(e.getStackTrace())));
        // Gracefully shutdown cannot be performed successfully because of error in
        // AdminService side. Under this scenario, the worker has to be added back to the worker
        // pool.
        inGracefulShutdown = false;
      }
    }
  }

  /**
   * Make grpc call to Buildfarm endpoint to disable the scale in protection of the host with
   * instanceIp.
   *
   * @param clusterEndpoint the current Buildfarm endpoint.
   * @param instanceIp Ip of the the instance that we want to disable scale in protection.
   */
  @SuppressWarnings("ResultOfMethodCallIgnored")
  private void disableScaleInProtection(String clusterEndpoint, String instanceIp) {
    ManagedChannel channel = null;
    try {
      NettyChannelBuilder builder =
          NettyChannelBuilder.forTarget(clusterEndpoint).negotiationType(NegotiationType.PLAINTEXT);
      channel = builder.build();
      AdminGrpc.AdminBlockingStub adminBlockingStub = AdminGrpc.newBlockingStub(channel);
      adminBlockingStub.disableScaleInProtection(
          DisableScaleInProtectionRequest.newBuilder().setInstanceName(instanceIp).build());
    } finally {
      if (channel != null) {
        channel.shutdown();
      }
    }
  }

  private static Path getValidRoot(ShardWorkerConfig config) throws ConfigurationException {
    addMissingRoot(config);
    verifyRootConfiguration(config);
    return Paths.get(config.getRoot());
  }

  private static void addMissingRoot(ShardWorkerConfig config) {
    Path root = Paths.get(config.getRoot());
    if (!Files.isDirectory(root)) {
      try {
        Files.createDirectories(root);
      } catch (IOException e) {
        logger.log(Level.SEVERE, e.toString());
      }
    }
  }

  private static void verifyRootConfiguration(ShardWorkerConfig config)
      throws ConfigurationException {
    String rootValue = config.getRoot();

    // Configuration error if no root is specified.
    if (Strings.isNullOrEmpty(rootValue)) {
      throw new ConfigurationException("root value in config missing");
    }

    // Configuration error if root does not exist.
    Path root = Paths.get(rootValue);
    if (!Files.isDirectory(root)) {
      throw new ConfigurationException("root [" + root.toString() + "] is not directory");
    }
  }

  private static Path getValidFilesystemCASPath(FilesystemCASConfig config, Path root)
      throws ConfigurationException {
    String pathValue = config.getPath();
    if (Strings.isNullOrEmpty(pathValue)) {
      throw new ConfigurationException("Cas cache directory value in config missing");
    }
    return root.resolve(pathValue);
  }

  private static HashFunction getValidHashFunction(ShardWorkerConfig config)
      throws ConfigurationException {
    try {
      return HashFunction.get(config.getDigestFunction());
    } catch (IllegalArgumentException e) {
      throw new ConfigurationException("hash_function value unrecognized");
    }
  }

  private Operation stripOperation(Operation operation) {
    return instance.stripOperation(operation);
  }

  private Operation stripQueuedOperation(Operation operation) {
    return instance.stripQueuedOperation(operation);
  }

  @SuppressWarnings({"deprecation", "unchecked"})
  public Worker(String session, ServerBuilder<?> serverBuilder, ShardWorkerConfig config)
      throws ConfigurationException {
    super("BuildFarmShardWorker");
    this.config = config;
    isCasShard = config.getCapabilities().getCas();
    String identifier = "buildfarm-worker-" + config.getPublicName() + "-" + session;
    root = getValidRoot(config);
    if (config.getPublicName().isEmpty()) {
      throw new ConfigurationException("worker's public name should not be empty");
    }

    digestUtil = new DigestUtil(getValidHashFunction(config));

    ShardWorkerConfig.BackplaneCase backplaneCase = config.getBackplaneCase();
    switch (backplaneCase) {
      default:
      case BACKPLANE_NOT_SET:
        throw new IllegalArgumentException("Shard Backplane not set in config");
      case REDIS_SHARD_BACKPLANE_CONFIG:
        backplane =
            new RedisShardBackplane(
                config.getRedisShardBackplaneConfig(),
                identifier,
                this::stripOperation,
                this::stripQueuedOperation);
        break;
    }

    workerStubs = WorkerStubs.create(digestUtil, getGrpcTimeout(config));

    ExecutorService removeDirectoryService =
        newFixedThreadPool(
            /* nThreads=*/ 32,
            new ThreadFactoryBuilder().setNameFormat("remove-directory-pool-%d").build());
    ExecutorService accessRecorder = newSingleThreadExecutor();

    InputStreamFactory remoteInputStreamFactory =
        new RemoteInputStreamFactory(
            config.getPublicName(),
            backplane,
            new Random(),
            workerStubs,
            (worker, t, context) -> {});
    ContentAddressableStorage storage =
        createStorages(
            remoteInputStreamFactory, removeDirectoryService, accessRecorder, config.getCasList());
    execFileSystem =
        createExecFileSystem(
            remoteInputStreamFactory, removeDirectoryService, accessRecorder, storage);

    instance = new ShardWorkerInstance(config.getPublicName(), digestUtil, backplane, storage);

    // Create the appropriate writer for the context
    CasWriter writer;
    if (!isCasShard) {
      writer = new RemoteCasWriter();
    } else {
      writer = new LocalCasWriter();
    }

    DequeueMatchSettings matchSettings = new DequeueMatchSettings();
    matchSettings.acceptEverything = config.getDequeueMatchSettings().getAcceptEverything();
    matchSettings.allowUnmatched = config.getDequeueMatchSettings().getAllowUnmatched();

    ShardWorkerContext context =
        new ShardWorkerContext(
            config.getPublicName(),
            matchSettings,
            config.getDequeueMatchSettings().getPlatform(),
            config.getOperationPollPeriod(),
            backplane::pollOperation,
            config.getInputFetchStageWidth(),
            config.getExecuteStageWidth(),
            config.getInputFetchDeadline(),
            backplane,
            execFileSystem,
            new EmptyInputStreamFactory(
                new FailoverInputStreamFactory(
                    execFileSystem.getStorage(), remoteInputStreamFactory)),
            config.getExecutionPoliciesList(),
            instance,
            /* deadlineAfter=*/
            /* deadlineAfterUnits=*/ config.getDefaultActionTimeout(),
            config.getMaximumActionTimeout(),
            config.getLimitExecution(),
            config.getLimitGlobalExecution(),
            config.getOnlyMulticoreTests(),
            config.getErrorOperationRemainingResources(),
            writer);

    PipelineStage completeStage =
        new PutOperationStage((operation) -> context.deactivate(operation.getName()));
    PipelineStage reportResultStage = new ReportResultStage(context, completeStage, completeStage);
    PipelineStage executeActionStage =
        new ExecuteActionStage(context, reportResultStage, completeStage);
    PipelineStage inputFetchStage =
        new InputFetchStage(context, executeActionStage, new PutOperationStage(context::requeue));
    PipelineStage matchStage = new MatchStage(context, inputFetchStage, completeStage);

    pipeline = new Pipeline();
    // pipeline.add(errorStage, 0);
    pipeline.add(matchStage, 4);
    pipeline.add(inputFetchStage, 3);
    pipeline.add(executeActionStage, 2);
    pipeline.add(reportResultStage, 1);

    server =
        serverBuilder
            .addService(healthStatusManager.getHealthService())
            .addService(
                new ContentAddressableStorageService(
                    instance, /* deadlineAfter=*/ 1, DAYS
                    /* requestLogLevel=*/ ))
            .addService(new ByteStreamService(instance, /* writeDeadlineAfter=*/ 1, DAYS))
            .addService(
                new WorkerProfileService(
                    storage,
                    inputFetchStage,
                    executeActionStage,
                    context,
                    completeStage,
                    backplane))
            .addService(new ShutDownWorkerGracefully(this, config))
            .build();

    logger.log(INFO, String.format("%s initialized", identifier));
  }

  private static Duration getGrpcTimeout(ShardWorkerConfig config) {
    // return the configured
    if (config.getShardWorkerInstanceConfig().hasGrpcTimeout()) {
      Duration configured = config.getShardWorkerInstanceConfig().getGrpcTimeout();
      if (configured.getSeconds() > 0 || configured.getNanos() > 0) {
        return configured;
      }
    }

    // return a default
    Duration defaultDuration = Durations.fromSeconds(60);
    logger.log(
        INFO, "grpc timeout not configured.  Setting to: " + defaultDuration.getSeconds() + "s");
    return defaultDuration;
  }

  private ListenableFuture<Long> streamIntoWriteFuture(InputStream in, Write write, Digest digest)
      throws IOException {
    SettableFuture<Long> writtenFuture = SettableFuture.create();
    int chunkSizeBytes = (int) Size.kbToBytes(128);

    // The following callback is performed each time the write stream is ready.
    // For each callback we only transfer a small part of the input stream in order to avoid
    // accumulating a large buffer.  When the file is done being transfered,
    // the callback closes the stream and prepares the future.
    FeedbackOutputStream out =
        write.getOutput(
            /* deadlineAfter=*/ 1,
            /* deadlineAfterUnits=*/ DAYS,
            () -> {
              try {
                FeedbackOutputStream outStream = (FeedbackOutputStream) write;
                while (outStream.isReady()) {
                  if (!CopyBytes(in, outStream, chunkSizeBytes)) {
                    return;
                  }
                }

              } catch (IOException e) {
                if (!write.isComplete()) {
                  write.reset();
                  logger.log(Level.SEVERE, "unexpected error transferring file for " + digest, e);
                }
              }
            });

    write
        .getFuture()
        .addListener(
            () -> {
              try {
                try {
                  out.close();
                } catch (IOException e) {
                  // ignore
                }
                long committedSize = write.getCommittedSize();
                if (committedSize != digest.getSizeBytes()) {
                  logger.log(
                      Level.WARNING,
                      format(
                          "committed size %d did not match expectation for digestUtil",
                          committedSize));
                }
                writtenFuture.set(digest.getSizeBytes());
              } catch (RuntimeException e) {
                writtenFuture.setException(e);
              }
            },
            directExecutor());

    return writtenFuture;
  }

  private boolean CopyBytes(InputStream in, OutputStream out, int bytesAmount) throws IOException {
    byte[] buf = new byte[bytesAmount];
    int n = in.read(buf);
    if (n > 0) {
      out.write(buf, 0, n);
      return true;
    }
    return false;
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  private String getRandomWorker() throws IOException {
    Set<String> workerSet = backplane.getWorkers();
    synchronized (workerSet) {
      if (workerSet.isEmpty()) {
        throw new RuntimeException("no available workers");
      }
      Random rand = new Random();
      int index = rand.nextInt(workerSet.size());
      // best case no allocation average n / 2 selection
      Iterator<String> iter = workerSet.iterator();
      String worker = null;
      while (iter.hasNext() && index-- >= 0) {
        worker = iter.next();
      }
      return worker;
    }
  }

  private Instance workerStub(String worker) {
    try {
      return workerStubs.get(worker);
    } catch (ExecutionException e) {
      logger.log(Level.SEVERE, "error getting worker stub for " + worker, e.getCause());
      throw new IllegalStateException("stub instance creation must not fail");
    }
  }

  private ExecFileSystem createFuseExecFileSystem(
      InputStreamFactory remoteInputStreamFactory, ContentAddressableStorage storage) {
    InputStreamFactory storageInputStreamFactory =
        (digest, offset) -> storage.get(digest).getData().substring((int) offset).newInput();

    InputStreamFactory localPopulatingInputStreamFactory =
        (blobDigest, offset) -> {
          // FIXME use write
          ByteString content =
              ByteString.readFrom(remoteInputStreamFactory.newInput(blobDigest, offset));

          if (offset == 0) {
            // extra computations
            Blob blob = new Blob(content, digestUtil);
            // here's hoping that our digest matches...
            storage.put(blob);
          }

          return content.newInput();
        };
    return new FuseExecFileSystem(
        root,
        new FuseCAS(
            root,
            new EmptyInputStreamFactory(
                new FailoverInputStreamFactory(
                    storageInputStreamFactory, localPopulatingInputStreamFactory))),
        storage);
  }

  private @Nullable UserPrincipal getOwner(FileSystem fileSystem) throws ConfigurationException {
    try {
      return getUser(config.getExecOwner(), fileSystem);
    } catch (IOException e) {
      ConfigurationException configException =
          new ConfigurationException("Could not locate exec_owner");
      configException.initCause(e);
      throw configException;
    }
  }

  private ExecFileSystem createExecFileSystem(
      InputStreamFactory remoteInputStreamFactory,
      ExecutorService removeDirectoryService,
      ExecutorService accessRecorder,
      ContentAddressableStorage storage)
      throws ConfigurationException {
    checkState(storage != null, "no exec fs cas specified");
    if (storage instanceof CASFileCache) {
      CASFileCache cfc = (CASFileCache) storage;
      UserPrincipal owner = getOwner(cfc.getRoot().getFileSystem());
      return createCFCExecFileSystem(removeDirectoryService, accessRecorder, cfc, owner);
    } else {
      // FIXME not the only fuse backing capacity...
      return createFuseExecFileSystem(remoteInputStreamFactory, storage);
    }
  }

  private ContentAddressableStorage createStorage(
      InputStreamFactory remoteInputStreamFactory,
      ExecutorService removeDirectoryService,
      Executor accessRecorder,
      ContentAddressableStorageConfig config,
      ContentAddressableStorage delegate)
      throws ConfigurationException {
    switch (config.getTypeCase()) {
      default:
      case TYPE_NOT_SET:
        throw new IllegalArgumentException("Invalid cas type specified");
      case MEMORY:
      case FUSE: // FIXME have FUSE refer to a name for storage backing, and topo
        return new MemoryCAS(config.getMemory().getMaxSizeBytes(), this::onStoragePut, delegate);
      case GRPC:
        checkState(delegate == null, "grpc cas cannot delegate");
        return createGrpcCAS(config.getGrpc());
      case FILESYSTEM:
        FilesystemCASConfig fsCASConfig = config.getFilesystem();
        return new ShardCASFileCache(
            remoteInputStreamFactory,
            root.resolve(getValidFilesystemCASPath(fsCASConfig, root)),
            fsCASConfig.getMaxSizeBytes(),
            fsCASConfig.getMaxEntrySizeBytes(),
            fsCASConfig.getHexBucketLevels(),
            fsCASConfig.getFileDirectoriesIndexInMemory(),
            digestUtil,
            removeDirectoryService,
            accessRecorder,
            this::onStoragePut,
            delegate == null ? this::onStorageExpire : (digests) -> {},
            delegate);
    }
  }

  private ContentAddressableStorage createStorages(
      InputStreamFactory remoteInputStreamFactory,
      ExecutorService removeDirectoryService,
      Executor accessRecorder,
      List<ContentAddressableStorageConfig> configs)
      throws ConfigurationException {
    ImmutableList.Builder<ContentAddressableStorage> storages = ImmutableList.builder();
    // must construct delegates first
    ContentAddressableStorage storage = null;
    ContentAddressableStorage delegate = null;
    for (ContentAddressableStorageConfig config : Lists.reverse(configs)) {
      storage =
          createStorage(
              remoteInputStreamFactory, removeDirectoryService, accessRecorder, config, delegate);
      storages.add(storage);
      delegate = storage;
    }
    return storage;
  }

  private ExecFileSystem createCFCExecFileSystem(
      ExecutorService removeDirectoryService,
      ExecutorService accessRecorder,
      CASFileCache fileCache,
      @Nullable UserPrincipal owner) {
    return new CFCExecFileSystem(
        root,
        fileCache,
        owner,
        config.getLinkInputDirectories(),
        removeDirectoryService,
        accessRecorder
        /* deadlineAfter=*/
        /* deadlineAfterUnits=*/ );
  }

  @SuppressWarnings({"deprecation", "ResultOfMethodCallIgnored"})
  public void stop() throws InterruptedException {
    boolean interrupted = Thread.interrupted();
    if (pipeline != null) {
      logger.log(INFO, "Closing the pipeline");
      try {
        pipeline.close();
      } catch (InterruptedException e) {
        Thread.interrupted();
        interrupted = true;
      }
    }
    healthStatusManager.setStatus(
        HealthStatusManager.SERVICE_NAME_ALL_SERVICES, ServingStatus.NOT_SERVING);
    healthCheckMetric.labels("stop").inc();
    if (execFileSystem != null) {
      logger.log(INFO, "Stopping exec filesystem");
      execFileSystem.stop();
    }
    if (server != null) {
      logger.log(INFO, "Shutting down the server");
      server.shutdown();

      try {
        server.awaitTermination(shutdownWaitTimeInSeconds, SECONDS);
      } catch (InterruptedException e) {
        interrupted = true;
        logger.log(SEVERE, "interrupted while waiting for server shutdown", e);
      } finally {
        server.shutdownNow();
      }
    }
    if (backplane != null) {
      try {
        backplane.stop();
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    if (workerStubs != null) {
      workerStubs.invalidateAll();
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
      throw new InterruptedException();
    }
  }

  private void onStoragePut(Digest digest) {
    try {
      // if the worker is a CAS member, it can send/modify blobs in the backplane.
      if (isCasShard) {
        backplane.addBlobLocation(digest, config.getPublicName());
      }
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  private void onStorageExpire(Iterable<Digest> digests) {
    if (isCasShard) {
      try {
        // if the worker is a CAS member, it can send/modify blobs in the backplane.
        backplane.removeBlobsLocation(digests, config.getPublicName());
      } catch (IOException e) {
        throw Status.fromThrowable(e).asRuntimeException();
      }
    }
  }

  private void blockUntilShutdown() throws InterruptedException {
    // should really be waiting for either server or pipeline shutdown
    try {
      pipeline.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    stop();
  }

  private void removeWorker(String name) {
    try {
      backplane.removeWorker(name, "removing self prior to initialization");
    } catch (IOException e) {
      Status status = Status.fromThrowable(e);
      if (status.getCode() != Code.UNAVAILABLE && status.getCode() != Code.DEADLINE_EXCEEDED) {
        throw status.asRuntimeException();
      }
      logger.log(INFO, "backplane was unavailable or overloaded, deferring removeWorker");
    }
  }

  private void addBlobsLocation(List<Digest> digests, String name) {
    while (!backplane.isStopped()) {
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
    throw Status.UNAVAILABLE.withDescription("backplane was stopped").asRuntimeException();
  }

  private void addWorker(ShardWorker worker) {
    while (!backplane.isStopped()) {
      try {
        backplane.addWorker(worker);
        return;
      } catch (IOException e) {
        Status status = Status.fromThrowable(e);
        if (status.getCode() != Code.UNAVAILABLE && status.getCode() != Code.DEADLINE_EXCEEDED) {
          throw status.asRuntimeException();
        }
      }
    }
    throw Status.UNAVAILABLE.withDescription("backplane was stopped").asRuntimeException();
  }

  private void startFailsafeRegistration() {
    String endpoint = config.getPublicName();
    ShardWorker.Builder worker = ShardWorker.newBuilder().setEndpoint(endpoint);
    int registrationIntervalMillis = 10000;
    int registrationOffsetMillis = registrationIntervalMillis * 3;
    new Thread(
            new Runnable() {
              long workerRegistrationExpiresAt = 0;

              ShardWorker nextRegistration(long now) {
                return worker.setExpireAt(now + registrationOffsetMillis).build();
              }

              long nextInterval(long now) {
                return now + registrationIntervalMillis;
              }

              void registerIfExpired() {
                long now = System.currentTimeMillis();
                if (now >= workerRegistrationExpiresAt && !inGracefulShutdown) {
                  // worker must be registered to match
                  addWorker(nextRegistration(now));
                  // update every 10 seconds
                  workerRegistrationExpiresAt = nextInterval(now);
                }
              }

              @Override
              public void run() {
                try {
                  while (!server.isShutdown()) {
                    registerIfExpired();
                    SECONDS.sleep(1);
                  }
                } catch (InterruptedException e) {
                  // ignore
                } finally {
                  try {
                    stop();
                  } catch (InterruptedException ie) {
                    logger.log(SEVERE, "interrupted while stopping worker", ie);
                    // ignore
                  }
                }
              }
            })
        .start();
  }

  @SuppressWarnings("deprecation")
  public void start() throws InterruptedException {
    try {
      backplane.start(config.getPublicName());

      removeWorker(config.getPublicName());

      boolean skipLoad = config.getCasList().get(0).getSkipLoad();
      execFileSystem.start(
          (digests) -> addBlobsLocation(digests, config.getPublicName()), skipLoad);

      server.start();
      healthStatusManager.setStatus(
          HealthStatusManager.SERVICE_NAME_ALL_SERVICES, ServingStatus.SERVING);
      PrometheusPublisher.startHttpServer(config.getPrometheusConfig().getPort());
      // Not all workers need to be registered and visible in the backplane.
      // For example, a GPU worker may wish to perform work that we do not want to cache locally for
      // other workers.
      if (isCasShard) {
        startFailsafeRegistration();
      } else {
        logger.log(INFO, "Skipping worker registration");
      }
    } catch (Exception e) {
      stop();
      logger.log(SEVERE, "error starting worker", e);
      return;
    }
    pipeline.start();
    healthCheckMetric.labels("start").inc();
  }

  @Override
  protected void onShutdown() throws InterruptedException {
    logger.log(SEVERE, "*** shutting down gRPC server since JVM is shutting down");
    PrometheusPublisher.stopHttpServer();
    stop();
    logger.log(SEVERE, "*** server shut down");
  }

  private static ShardWorkerConfig toShardWorkerConfig(Readable input, WorkerOptions options)
      throws IOException {
    ShardWorkerConfig.Builder builder = ShardWorkerConfig.newBuilder();
    TextFormat.merge(input, builder);
    if (!Strings.isNullOrEmpty(options.root)) {
      builder.setRoot(options.root);
    }
    if (!Strings.isNullOrEmpty(options.publicName)) {
      builder.setPublicName(options.publicName);
    }
    return builder.build();
  }

  private static void printUsage(OptionsParser parser) {
    logger.log(INFO, "Usage: CONFIG_PATH");
    logger.log(
        INFO, parser.describeOptions(Collections.emptyMap(), OptionsParser.HelpVerbosity.LONG));
  }

  public static void main(String[] args) {
    try {
      startWorker(args);
    } catch (Exception e) {
      logger.log(SEVERE, "exception caught", e);
      System.exit(1);
    }
  }

  @SuppressWarnings("ConstantConditions")
  public static void startWorker(String[] args) throws Exception {
    // Only log severe log messages from Netty. Otherwise it logs warnings that look like this:
    //
    // 170714 08:16:28.552:WT 18 [io.grpc.netty.NettyServerHandler.onStreamError] Stream Error
    // io.netty.handler.codec.http2.Http2Exception$StreamException: Received DATA frame for an
    // unknown stream 11369
    nettyLogger.setLevel(SEVERE);

    OptionsParser parser = OptionsParser.newOptionsParser(WorkerOptions.class);
    parser.parseAndExitUponError(args);
    List<String> residue = parser.getResidue();
    if (residue.isEmpty()) {
      printUsage(parser);
      throw new IllegalArgumentException("Missing CONFIG_PATH");
    }
    Path configPath = Paths.get(residue.get(0));
    String session = UUID.randomUUID().toString();
    Worker worker;
    try (InputStream configInputStream = Files.newInputStream(configPath)) {
      ShardWorkerConfig config =
          toShardWorkerConfig(
              new InputStreamReader(configInputStream), parser.getOptions(WorkerOptions.class));
      worker = new Worker(session, config);
    }
    worker.start();
    worker.blockUntilShutdown();
    System.exit(0); // bullet to the head in case anything is stuck
  }
}
