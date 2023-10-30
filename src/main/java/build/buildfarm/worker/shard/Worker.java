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
import static build.buildfarm.common.config.Backplane.BACKPLANE_TYPE.SHARD;
import static build.buildfarm.common.io.Utils.formatIOError;
import static build.buildfarm.common.io.Utils.getUser;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;

import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.backplane.Backplane;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.cas.MemoryCAS;
import build.buildfarm.cas.cfc.CASFileCache;
import build.buildfarm.common.BuildfarmExecutors;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.common.LoggingMain;
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.common.config.Cas;
import build.buildfarm.common.config.GrpcMetrics;
import build.buildfarm.common.grpc.Retrier;
import build.buildfarm.common.grpc.Retrier.Backoff;
import build.buildfarm.common.grpc.TracingMetadataUtils.ServerHeadersInterceptor;
import build.buildfarm.common.services.ByteStreamService;
import build.buildfarm.common.services.ContentAddressableStorageService;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.shard.RedisShardBackplane;
import build.buildfarm.instance.shard.RemoteInputStreamFactory;
import build.buildfarm.instance.shard.WorkerStubs;
import build.buildfarm.metrics.prometheus.PrometheusPublisher;
import build.buildfarm.v1test.ShardWorker;
import build.buildfarm.worker.ExecuteActionStage;
import build.buildfarm.worker.FuseCAS;
import build.buildfarm.worker.InputFetchStage;
import build.buildfarm.worker.MatchStage;
import build.buildfarm.worker.Pipeline;
import build.buildfarm.worker.PipelineStage;
import build.buildfarm.worker.PutOperationStage;
import build.buildfarm.worker.ReportResultStage;
import build.buildfarm.worker.SuperscalarPipelineStage;
import build.buildfarm.worker.resources.LocalResourceSetUtils;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.services.HealthStatusManager;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.UserPrincipal;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import javax.annotation.Nullable;
import javax.naming.ConfigurationException;
import lombok.extern.java.Log;

@Log
public final class Worker extends LoggingMain {
  private static final java.util.logging.Logger nettyLogger =
      java.util.logging.Logger.getLogger("io.grpc.netty");
  private static final Counter healthCheckMetric =
      Counter.build()
          .name("health_check")
          .labelNames("lifecycle")
          .help("Service health check.")
          .register();
  private static final Counter workerPausedMetric =
      Counter.build().name("worker_paused").help("Worker paused.").register();
  private static final Gauge executionSlotsTotal =
      Gauge.build()
          .name("execution_slots_total")
          .help("Total execution slots configured on worker.")
          .register();
  private static final Gauge inputFetchSlotsTotal =
      Gauge.build()
          .name("input_fetch_slots_total")
          .help("Total input fetch slots configured on worker.")
          .register();

  private static final int shutdownWaitTimeInSeconds = 10;

  private static BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  private boolean inGracefulShutdown = false;
  private boolean isPaused = false;

  private WorkerInstance instance;

  @SuppressWarnings("deprecation")
  private final HealthStatusManager healthStatusManager = new HealthStatusManager();

  private Server server;
  private Path root;
  private DigestUtil digestUtil;
  private ExecFileSystem execFileSystem;
  private Pipeline pipeline;
  private Backplane backplane;
  private LoadingCache<String, Instance> workerStubs;
  private AtomicBoolean released = new AtomicBoolean(true);

  private Worker() {
    super("BuildFarmShardWorker");
  }

  private Operation stripOperation(Operation operation) {
    return instance.stripOperation(operation);
  }

  private Operation stripQueuedOperation(Operation operation) {
    return instance.stripQueuedOperation(operation);
  }

  private Server createServer(
      ServerBuilder<?> serverBuilder,
      @Nullable CASFileCache storage,
      Instance instance,
      Pipeline pipeline,
      ShardWorkerContext context) {
    serverBuilder.addService(healthStatusManager.getHealthService());
    serverBuilder.addService(new ContentAddressableStorageService(instance));
    serverBuilder.addService(new ByteStreamService(instance));
    serverBuilder.addService(new ShutDownWorkerGracefully(this));
    serverBuilder.addService(ProtoReflectionService.newInstance());

    // We will build a worker's server based on it's capabilities.
    // A worker that is capable of execution will construct an execution pipeline.
    // It will use various execution phases for it's profile service.
    // On the other hand, a worker that is only capable of CAS storage does not need a pipeline.
    if (configs.getWorker().getCapabilities().isExecution()) {
      PutOperationStage completeStage =
          new PutOperationStage(operation -> context.deactivate(operation.getName()));
      PipelineStage errorStage = completeStage; /* new ErrorStage(); */
      PipelineStage reportResultStage = new ReportResultStage(context, completeStage, errorStage);
      SuperscalarPipelineStage executeActionStage =
          new ExecuteActionStage(context, reportResultStage, errorStage);
      SuperscalarPipelineStage inputFetchStage =
          new InputFetchStage(context, executeActionStage, new PutOperationStage(context::requeue));
      PipelineStage matchStage = new MatchStage(context, inputFetchStage, errorStage);

      pipeline.add(matchStage, 4);
      pipeline.add(inputFetchStage, 3);
      pipeline.add(executeActionStage, 2);
      pipeline.add(reportResultStage, 1);

      serverBuilder.addService(
          new WorkerProfileService(
              storage,
              matchStage,
              inputFetchStage,
              executeActionStage,
              reportResultStage,
              completeStage,
              backplane));
    }
    GrpcMetrics.handleGrpcMetricIntercepts(serverBuilder, configs.getWorker().getGrpcMetrics());
    serverBuilder.intercept(new ServerHeadersInterceptor());

    return serverBuilder.build();
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  private ExecFileSystem createFuseExecFileSystem(
      InputStreamFactory remoteInputStreamFactory, ContentAddressableStorage storage) {
    InputStreamFactory storageInputStreamFactory =
        (compressor, digest, offset) -> {
          checkArgument(compressor == Compressor.Value.IDENTITY);
          return storage.get(digest).getData().substring((int) offset).newInput();
        };

    InputStreamFactory localPopulatingInputStreamFactory =
        (compressor, blobDigest, offset) -> {
          // FIXME use write
          ByteString content =
              ByteString.readFrom(
                  remoteInputStreamFactory.newInput(compressor, blobDigest, offset));

          // needs some treatment for compressor
          if (offset == 0) {
            // extra computations
            Blob blob = new Blob(content, digestUtil);
            // here's hoping that our digest matches...
            try {
              storage.put(blob);
            } catch (InterruptedException e) {
              throw new IOException(e);
            }
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
      return getUser(configs.getWorker().getExecOwner(), fileSystem);
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

  private ContentAddressableStorage createStorages(
      InputStreamFactory remoteInputStreamFactory,
      ExecutorService removeDirectoryService,
      Executor accessRecorder,
      List<Cas> storages)
      throws ConfigurationException {
    ContentAddressableStorage storage = null;
    ContentAddressableStorage delegate = null;
    boolean delegateSkipLoad = false;
    for (Cas cas : Lists.reverse(storages)) {
      storage =
          createStorage(
              remoteInputStreamFactory,
              removeDirectoryService,
              accessRecorder,
              cas,
              delegate,
              delegateSkipLoad);
      delegate = storage;
      delegateSkipLoad = cas.isSkipLoad();
    }
    return storage;
  }

  private ContentAddressableStorage createStorage(
      InputStreamFactory remoteInputStreamFactory,
      ExecutorService removeDirectoryService,
      Executor accessRecorder,
      Cas cas,
      ContentAddressableStorage delegate,
      boolean delegateSkipLoad)
      throws ConfigurationException {
    switch (cas.getType()) {
      default:
        throw new IllegalArgumentException("Invalid cas type specified");
      case MEMORY:
      case FUSE: // FIXME have FUSE refer to a name for storage backing, and topo
        return new MemoryCAS(cas.getMaxSizeBytes(), this::onStoragePut, delegate);
      case GRPC:
        checkState(delegate == null, "grpc cas cannot delegate");
        return createGrpcCAS(cas);
      case FILESYSTEM:
        return new ShardCASFileCache(
            remoteInputStreamFactory,
            root.resolve(cas.getValidPath(root)),
            cas.getMaxSizeBytes(),
            configs.getMaxEntrySizeBytes(), // TODO make this a configurable value for each cas
            // delegate level
            cas.getHexBucketLevels(),
            cas.isFileDirectoriesIndexInMemory(),
            cas.isExecRootCopyFallback(),
            digestUtil,
            removeDirectoryService,
            accessRecorder,
            this::onStoragePut,
            delegate == null ? this::onStorageExpire : (digests) -> {},
            delegate,
            delegateSkipLoad);
    }
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
        configs.getWorker().isLinkInputDirectories(),
        configs.getWorker().getLinkedInputDirectories(),
        configs.isAllowSymlinkTargetAbsolute(),
        removeDirectoryService,
        accessRecorder
        /* deadlineAfter=*/
        /* deadlineAfterUnits=*/ );
  }

  private void onStoragePut(Digest digest) {
    try {
      // if the worker is a CAS member, it can send/modify blobs in the backplane.
      if (configs.getWorker().getCapabilities().isCas()) {
        backplane.addBlobLocation(digest, configs.getWorker().getPublicName());
      }
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  private void onStorageExpire(Iterable<Digest> digests) {
    if (configs.getWorker().getCapabilities().isCas()) {
      try {
        // if the worker is a CAS member, it can send/modify blobs in the backplane.
        backplane.removeBlobsLocation(digests, configs.getWorker().getPublicName());
      } catch (IOException e) {
        throw Status.fromThrowable(e).asRuntimeException();
      }
    }
  }

  private void removeWorker(String name) {
    try {
      backplane.removeWorker(name, "removing self prior to initialization");
    } catch (IOException e) {
      Status status = Status.fromThrowable(e);
      if (status.getCode() != Code.UNAVAILABLE && status.getCode() != Code.DEADLINE_EXCEEDED) {
        throw status.asRuntimeException();
      }
      log.log(INFO, "backplane was unavailable or overloaded, deferring removeWorker");
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
    String endpoint = configs.getWorker().getPublicName();
    ShardWorker.Builder worker = ShardWorker.newBuilder().setEndpoint(endpoint);
    worker.setWorkerType(configs.getWorker().getWorkerType());
    worker.setFirstRegisteredAt(loadWorkerStartTimeInMillis());
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

              boolean isWorkerPausedFromNewWork() {
                try {
                  File pausedFile = new File(configs.getWorker().getRoot() + "/.paused");
                  if (pausedFile.exists() && !isPaused) {
                    isPaused = true;
                    log.log(Level.INFO, "The current worker is paused from taking on new work!");
                    pipeline.stopMatchingOperations();
                    workerPausedMetric.inc();
                  }
                } catch (Exception e) {
                  log.log(Level.WARNING, "Could not open .paused file.", e);
                }
                return isPaused;
              }

              void registerIfExpired() {
                long now = System.currentTimeMillis();
                if (now >= workerRegistrationExpiresAt
                    && !inGracefulShutdown
                    && !isWorkerPausedFromNewWork()) {
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
                }
              }
            },
            "Worker.failsafeRegistration")
        .start();
  }

  private long loadWorkerStartTimeInMillis() {
    try {
      File cache = new File(configs.getWorker().getRoot() + "/cache");
      return Files.readAttributes(cache.toPath(), BasicFileAttributes.class)
          .creationTime()
          .toMillis();
    } catch (IOException e) {
      return System.currentTimeMillis();
    }
  }

  public void start() throws ConfigurationException, InterruptedException, IOException {
    released.set(false);
    String session = UUID.randomUUID().toString();
    ServerBuilder<?> serverBuilder = ServerBuilder.forPort(configs.getWorker().getPort());
    String identifier = "buildfarm-worker-" + configs.getWorker().getPublicName() + "-" + session;
    root = configs.getWorker().getValidRoot();
    if (configs.getWorker().getPublicName().isEmpty()) {
      throw new ConfigurationException("worker's public name should not be empty");
    }

    digestUtil = new DigestUtil(configs.getDigestFunction());

    if (SHARD.equals(configs.getBackplane().getType())) {
      backplane =
          new RedisShardBackplane(
              identifier,
              /* subscribeToBackplane=*/ false,
              /* runFailsafeOperation=*/ false,
              this::stripOperation,
              this::stripQueuedOperation);
      backplane.start(configs.getWorker().getPublicName());
    } else {
      throw new IllegalArgumentException("Shard Backplane not set in config");
    }

    workerStubs =
        WorkerStubs.create(
            digestUtil,
            Duration.newBuilder().setSeconds(configs.getServer().getGrpcTimeout()).build());

    ExecutorService removeDirectoryService = BuildfarmExecutors.getRemoveDirectoryPool();
    ExecutorService accessRecorder = newSingleThreadExecutor();

    InputStreamFactory remoteInputStreamFactory =
        new RemoteInputStreamFactory(
            configs.getWorker().getPublicName(),
            backplane,
            new Random(),
            workerStubs,
            (worker, t, context) -> {});
    ContentAddressableStorage storage =
        createStorages(
            remoteInputStreamFactory,
            removeDirectoryService,
            accessRecorder,
            configs.getWorker().getStorages());
    execFileSystem =
        createExecFileSystem(
            remoteInputStreamFactory, removeDirectoryService, accessRecorder, storage);

    instance =
        new WorkerInstance(configs.getWorker().getPublicName(), digestUtil, backplane, storage);

    // Create the appropriate writer for the context
    CasWriter writer;
    if (!configs.getWorker().getCapabilities().isCas()) {
      Retrier retrier = new Retrier(Backoff.sequential(5), Retrier.DEFAULT_IS_RETRIABLE);
      writer = new RemoteCasWriter(backplane.getStorageWorkers(), workerStubs, retrier);
    } else {
      writer = new LocalCasWriter(execFileSystem);
    }

    ShardWorkerContext context =
        new ShardWorkerContext(
            configs.getWorker().getPublicName(),
            Duration.newBuilder().setSeconds(configs.getWorker().getOperationPollPeriod()).build(),
            backplane::pollOperation,
            configs.getWorker().getInputFetchStageWidth(),
            configs.getWorker().getExecuteStageWidth(),
            configs.getWorker().getInputFetchDeadline(),
            backplane,
            execFileSystem,
            new EmptyInputStreamFactory(
                new FailoverInputStreamFactory(
                    execFileSystem.getStorage(), remoteInputStreamFactory)),
            Arrays.asList(configs.getWorker().getExecutionPolicies()),
            instance,
            Duration.newBuilder().setSeconds(configs.getDefaultActionTimeout()).build(),
            Duration.newBuilder().setSeconds(configs.getMaximumActionTimeout()).build(),
            configs.getWorker().getDefaultMaxCores(),
            configs.getWorker().isLimitGlobalExecution(),
            configs.getWorker().isOnlyMulticoreTests(),
            configs.getWorker().isAllowBringYourOwnContainer(),
            configs.getWorker().isErrorOperationRemainingResources(),
            LocalResourceSetUtils.create(configs.getWorker().getResources()),
            writer);

    pipeline = new Pipeline();
    server = createServer(serverBuilder, (CASFileCache) storage, instance, pipeline, context);

    removeWorker(configs.getWorker().getPublicName());

    boolean skipLoad = configs.getWorker().getStorages().get(0).isSkipLoad();
    execFileSystem.start(
        (digests) -> addBlobsLocation(digests, configs.getWorker().getPublicName()), skipLoad);

    server.start();
    healthStatusManager.setStatus(
        HealthStatusManager.SERVICE_NAME_ALL_SERVICES, ServingStatus.SERVING);
    PrometheusPublisher.startHttpServer(configs.getPrometheusPort());
    startFailsafeRegistration();

    pipeline.start();
    healthCheckMetric.labels("start").inc();
    executionSlotsTotal.set(configs.getWorker().getExecuteStageWidth());
    inputFetchSlotsTotal.set(configs.getWorker().getInputFetchStageWidth());

    log.log(INFO, String.format("%s initialized", identifier));
  }

  @Override
  protected void onShutdown() throws InterruptedException {
    initiateShutdown();
    awaitRelease();
  }

  private void awaitTermination() throws InterruptedException {
    pipeline.join();
    server.awaitTermination();
  }

  public void initiateShutdown() {
    pipeline.stopMatchingOperations();
    if (server != null) {
      server.shutdown();
    }
  }

  private synchronized void awaitRelease() throws InterruptedException {
    while (!released.get()) {
      wait();
    }
  }

  public synchronized void stop() throws InterruptedException {
    try {
      shutdown();
    } finally {
      released.set(true);
      notify();
    }
  }

  private void shutdown() throws InterruptedException {
    log.info("*** shutting down gRPC server since JVM is shutting down");
    PrometheusPublisher.stopHttpServer();
    boolean interrupted = Thread.interrupted();
    if (pipeline != null) {
      log.log(INFO, "Closing the pipeline");
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
    executionSlotsTotal.set(0);
    inputFetchSlotsTotal.set(0);
    if (execFileSystem != null) {
      log.info("Stopping exec filesystem");
      execFileSystem.stop();
      execFileSystem = null;
    }
    if (server != null) {
      log.info("Shutting down the server");
      server.shutdown();

      try {
        server.awaitTermination(shutdownWaitTimeInSeconds, SECONDS);
      } catch (InterruptedException e) {
        interrupted = true;
        log.log(SEVERE, "interrupted while waiting for server shutdown", e);
      } finally {
        server.shutdownNow();
      }
      server = null;
    }
    if (backplane != null) {
      try {
        backplane.stop();
        backplane = null;
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    if (workerStubs != null) {
      workerStubs.invalidateAll();
      workerStubs = null;
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
      throw new InterruptedException();
    }
    log.info("*** server shut down");
  }

  public static void main(String[] args) throws Exception {
    // Only log severe log messages from Netty. Otherwise it logs warnings that look like this:
    //
    // 170714 08:16:28.552:WT 18 [io.grpc.netty.NettyServerHandler.onStreamError] Stream Error
    // io.netty.handler.codec.http2.Http2Exception$StreamException: Received DATA frame for an
    // unknown stream 11369
    nettyLogger.setLevel(SEVERE);

    configs = BuildfarmConfigs.loadWorkerConfigs(args);
    Worker worker = new Worker();
    try {
      worker.start();
      worker.awaitTermination();
    } catch (IOException e) {
      log.severe(formatIOError(e));
    } catch (InterruptedException e) {
      log.log(Level.WARNING, "interrupted", e);
    } finally {
      worker.stop();
    }
  }
}
