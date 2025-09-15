// Copyright 2017 The Buildfarm Authors. All rights reserved.
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
import static build.buildfarm.common.Claim.Stage.REPORT_RESULT_STAGE;
import static build.buildfarm.common.config.Backplane.BACKPLANE_TYPE.SHARD;
import static build.buildfarm.common.io.Utils.formatIOError;
import static build.buildfarm.common.io.Utils.getUser;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;

import build.bazel.remote.execution.v2.Compressor;
import build.buildfarm.backplane.Backplane;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.cas.MemoryCAS;
import build.buildfarm.cas.cfc.CASFileCache;
import build.buildfarm.cas.cfc.DirectoryEntryCFC;
import build.buildfarm.cas.cfc.LegacyDirectoryCFC;
import build.buildfarm.common.BuildfarmExecutors;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.Dispenser;
import build.buildfarm.common.EmptyInputStreamFactory;
import build.buildfarm.common.FailoverInputStreamFactory;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.common.LoggingMain;
import build.buildfarm.common.ZstdDecompressingOutputStream.FixedBufferPool;
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.common.config.Cas;
import build.buildfarm.common.config.GrpcMetrics;
import build.buildfarm.common.function.InterruptingConsumer;
import build.buildfarm.common.grpc.Retrier;
import build.buildfarm.common.grpc.Retrier.Backoff;
import build.buildfarm.common.grpc.TracingMetadataUtils.ServerHeadersInterceptor;
import build.buildfarm.common.services.ByteStreamService;
import build.buildfarm.common.services.ContentAddressableStorageService;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.shard.RedisShardBackplane;
import build.buildfarm.instance.shard.RemoteInputStreamFactory;
import build.buildfarm.instance.shard.WorkerStubs;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.metrics.prometheus.PrometheusPublisher;
import build.buildfarm.v1test.Digest;
import build.buildfarm.v1test.PipelineChange;
import build.buildfarm.v1test.ShardWorker;
import build.buildfarm.worker.CFCExecFileSystem;
import build.buildfarm.worker.CFCLinkExecFileSystem;
import build.buildfarm.worker.ExecFileSystem;
import build.buildfarm.worker.ExecuteActionStage;
import build.buildfarm.worker.ExecutionContext;
import build.buildfarm.worker.FuseCAS;
import build.buildfarm.worker.InputFetchStage;
import build.buildfarm.worker.MatchStage;
import build.buildfarm.worker.Pipeline;
import build.buildfarm.worker.PipelineStage;
import build.buildfarm.worker.PutOperationStage;
import build.buildfarm.worker.ReportResultStage;
import build.buildfarm.worker.SuperscalarPipelineStage;
import build.buildfarm.worker.cgroup.Group;
import build.buildfarm.worker.resources.LocalResourceSet;
import build.buildfarm.worker.resources.LocalResourceSet.PoolResource;
import build.buildfarm.worker.resources.LocalResourceSetUtils;
import com.google.common.base.Strings;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.UserPrincipal;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
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
  private static final Gauge reportResultSlotsTotal =
      Gauge.build()
          .name("report_result_slots_total")
          .help("Total report result slots configured on worker.")
          .register();

  private static final int shutdownWaitTimeInSeconds = 10;

  private static BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  private boolean isPaused = false;

  private WorkerInstance instance;

  private final HealthStatusManager healthStatusManager = new HealthStatusManager();

  private Server server;
  private Path root;
  private ExecFileSystem execFileSystem;
  private Pipeline pipeline;
  private PipelineStage matchStage;
  private ShardWorkerContext context;
  private Backplane backplane;
  private LoadingCache<String, StubInstance> workerStubs;
  private AtomicBoolean released = new AtomicBoolean(true);
  private AtomicBoolean shutdownInitiated = new AtomicBoolean(false);
  private boolean startWritable = true;

  /**
   * The method will prepare the worker for graceful shutdown when the worker is ready. Note on
   * using stderr here instead of log. By the time this is called in PreDestroy, the log is no
   * longer available and is not logging messages.
   */
  public void prepareWorkerForGracefulShutdown() {
    if (configs.getWorker().getGracefulShutdownSeconds() == 0) {
      log.info(
          "Graceful Shutdown is not enabled. Worker is shutting down without finishing executions"
              + " in progress.");
    } else {
      log.info(
          "Graceful Shutdown - The current worker will not be registered again and should be"
              + " shutdown gracefully!");
      context.prepareForGracefulShutdown();
      int scanRate = 30; // check every 30 seconds
      int timeWaited = 0;
      int timeOut = configs.getWorker().getGracefulShutdownSeconds();
      try {
        if (pipeline.isEmpty()) {
          log.info("Graceful Shutdown - no work in the pipeline.");
        } else {
          log.info("Graceful Shutdown - waiting for executions to finish.");
        }
        while (!pipeline.isEmpty() && timeWaited < timeOut) {
          SECONDS.sleep(scanRate);
          timeWaited += scanRate;
          log.info(
              String.format(
                  "Graceful Shutdown - Pipeline is still not empty after %d seconds.", timeWaited));
        }
      } catch (InterruptedException e) {
        log.info(
            "Graceful Shutdown - The worker gracefully shutdown is interrupted: " + e.getMessage());
      } finally {
        log.info(
            String.format(
                "Graceful Shutdown - It took the worker %d seconds to %s",
                timeWaited,
                pipeline.isEmpty()
                    ? "finish all actions"
                    : "gracefully shutdown but still cannot finish all actions"));
      }
    }
  }

  private Worker() {
    super("BuildFarmShardWorker");
  }

  private Operation stripOperation(Operation operation) {
    return instance.stripOperation(operation);
  }

  private static class ReleaseClaimStage extends PutOperationStage {
    public ReleaseClaimStage(InterruptingConsumer<Operation> onPut) {
      super(onPut);
    }

    @Override
    public void put(ExecutionContext executionContext) throws InterruptedException {
      executionContext.claim.release();
      super.put(executionContext);
    }
  }

  private Server createServer(
      ServerBuilder<?> serverBuilder,
      Instance instance,
      WorkerProfileService workerProfileService) {
    serverBuilder.addService(healthStatusManager.getHealthService());
    serverBuilder.addService(new ContentAddressableStorageService(instance));
    serverBuilder.addService(new ByteStreamService(instance));
    serverBuilder.addService(new WorkerControl(this));
    serverBuilder.addService(ProtoReflectionServiceV1.newInstance());
    serverBuilder.addService(workerProfileService);

    GrpcMetrics.handleGrpcMetricIntercepts(serverBuilder, configs.getWorker().getGrpcMetrics());
    serverBuilder.intercept(new ServerHeadersInterceptor(meta -> {}));
    if (configs.getServer().getMaxInboundMessageSizeBytes() != 0) {
      serverBuilder.maxInboundMessageSize(configs.getServer().getMaxInboundMessageSizeBytes());
    }
    if (configs.getServer().getMaxInboundMetadataSize() != 0) {
      serverBuilder.maxInboundMetadataSize(configs.getServer().getMaxInboundMetadataSize());
    }
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
            Blob blob =
                new Blob(content, new DigestUtil(HashFunction.get(blobDigest.getDigestFunction())));
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

  private @Nullable UserPrincipal getOwner(String name, FileSystem fileSystem)
      throws ConfigurationException {
    try {
      return getUser(name, fileSystem);
    } catch (IOException e) {
      ConfigurationException configException =
          new ConfigurationException("Could not locate " + name);
      configException.initCause(e);
      throw configException;
    }
  }

  private ExecFileSystem createExecFileSystem(
      InputStreamFactory remoteInputStreamFactory,
      ExecutorService removeDirectoryService,
      ExecutorService accessRecorder,
      ExecutorService fetchService,
      ContentAddressableStorage storage,
      LocalResourceSet resourceSet,
      String ownerName,
      List<String> ownerNames)
      throws ConfigurationException {
    checkState(storage != null, "no exec fs cas specified");
    if (storage instanceof CASFileCache cfc) {
      FileSystem fileSystem = cfc.getRoot().getFileSystem();
      PoolResource execOwnerIndexResource = null;
      ImmutableMap<String, UserPrincipal> owners = ImmutableMap.of();
      // there's some sense that ownerNames might be specifiable as 1, but not 2, and 3 being the
      // minimum passing the config validation
      if (ownerNames.isEmpty()) {
        if (!Strings.isNullOrEmpty(ownerName)) {
          owners = ImmutableMap.of(ownerName, getOwner(ownerName, fileSystem));
          execOwnerIndexResource =
              new PoolResource(new Dispenser<>(ownerName), REPORT_RESULT_STAGE);
        }
      } else {
        ImmutableMap.Builder<String, UserPrincipal> builder = ImmutableMap.builder();
        for (String name : ownerNames) {
          UserPrincipal owner = getOwner(name, fileSystem);
          if (owner == null) {
            throw new ConfigurationException("could not locate user " + name);
          }
          builder.put(name, getOwner(name, fileSystem));
        }
        owners = builder.build();
        execOwnerIndexResource =
            new PoolResource(new ArrayDeque<>(owners.keySet()), REPORT_RESULT_STAGE);
      }
      if (execOwnerIndexResource != null) {
        resourceSet.poolResources.put(
            ShardWorkerContext.EXEC_OWNER_RESOURCE_NAME, execOwnerIndexResource);
      }

      return createCFCExecFileSystem(
          removeDirectoryService, accessRecorder, fetchService, cfc, owners);
    } else {
      // FIXME not the only fuse backing capacity...
      return createFuseExecFileSystem(remoteInputStreamFactory, storage);
    }
  }

  private ContentAddressableStorage createStorages(
      InputStreamFactory remoteInputStreamFactory,
      ExecutorService removeDirectoryService,
      Executor accessRecorder,
      FixedBufferPool zstdBufferPool,
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
              zstdBufferPool,
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
      FixedBufferPool zstdBufferPool,
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
        return createCASFileCache(
            root.resolve(cas.getValidPath(root)),
            cas,
            configs.getMaxEntrySizeBytes(), // TODO make this a configurable value for each cas
            removeDirectoryService,
            accessRecorder,
            /* storage= */ Maps.newConcurrentMap(),
            zstdBufferPool,
            this::onStoragePut,
            delegate == null ? this::onStorageExpire : (digests) -> {},
            delegate,
            delegateSkipLoad,
            remoteInputStreamFactory);
    }
  }

  private CASFileCache createCASFileCache(
      Path root,
      Cas cas,
      long maxEntrySizeInBytes,
      ExecutorService expireService,
      Executor accessRecorder,
      ConcurrentMap<String, CASFileCache.Entry> storage,
      FixedBufferPool zstdBufferPool,
      Consumer<Digest> onPut,
      Consumer<Iterable<Digest>> onExpire,
      @Nullable ContentAddressableStorage delegate,
      boolean delegateSkipLoad,
      InputStreamFactory externalInputStreamFactory) {
    if (configs.getWorker().isLegacyDirectoryFileCache()) {
      return new LegacyDirectoryCFC(
          root,
          cas.getMaxSizeBytes(),
          maxEntrySizeInBytes, // TODO make this a configurable value for each cas
          cas.getHexBucketLevels(),
          cas.isFileDirectoriesIndexInMemory(),
          cas.isExecRootCopyFallback(),
          expireService,
          accessRecorder,
          storage,
          LegacyDirectoryCFC.DEFAULT_DIRECTORIES_INDEX_NAME,
          zstdBufferPool,
          onPut,
          onExpire,
          delegate,
          delegateSkipLoad,
          externalInputStreamFactory);
    }
    return new DirectoryEntryCFC(
        root,
        cas.getMaxSizeBytes(),
        maxEntrySizeInBytes,
        cas.getHexBucketLevels(),
        expireService,
        accessRecorder,
        storage,
        zstdBufferPool,
        onPut,
        onExpire,
        delegate,
        delegateSkipLoad,
        externalInputStreamFactory);
  }

  private ExecFileSystem createCFCExecFileSystem(
      ExecutorService removeDirectoryService,
      ExecutorService accessRecorder,
      ExecutorService fetchService,
      CASFileCache fileCache,
      ImmutableMap<String, UserPrincipal> owners) {
    if (configs.getWorker().isLinkExecFileSystem()) {
      return new CFCLinkExecFileSystem(
          root,
          fileCache,
          owners,
          configs.getWorker().isLinkInputDirectories(),
          configs.getWorker().getLinkedInputDirectories(),
          configs.isAllowSymlinkTargetAbsolute(),
          removeDirectoryService,
          accessRecorder,
          fetchService);
    } else {
      return new CFCExecFileSystem(
          root,
          fileCache,
          owners,
          configs.isAllowSymlinkTargetAbsolute(),
          removeDirectoryService,
          accessRecorder,
          fetchService);
    }
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
        if (configs.getWorker().getCapabilities().isCas()) {
          backplane.addBlobsLocation(digests, name);
        }
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
                    context.prepareForGracefulShutdown();
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
                    && !context.inGracefulShutdown()
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
                  while (server != null && !server.isShutdown()) {
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

    workerStubs =
        WorkerStubs.create(
            Duration.newBuilder().setSeconds(configs.getServer().getGrpcTimeout()).build());

    if (SHARD.equals(configs.getBackplane().getType())) {
      backplane =
          new RedisShardBackplane(
              identifier,
              /* subscribeToBackplane= */ true,
              /* runFailsafeOperation= */ false,
              this::stripOperation);
      backplane.start(configs.getWorker().getPublicName(), workerStubs::invalidate);
    } else {
      throw new IllegalArgumentException("Shard Backplane not set in config");
    }

    ExecutorService removeDirectoryService = BuildfarmExecutors.getRemoveDirectoryPool();
    ExecutorService accessRecorder = newSingleThreadExecutor();
    ExecutorService fetchService = BuildfarmExecutors.getFetchServicePool();
    FixedBufferPool zstdBufferPool =
        new FixedBufferPool(configs.getWorker().getZstdBufferPoolSize());
    Gauge.build()
        .name("zstd_buffer_pool_used")
        .help("Current number of Zstd decompression buffers active")
        .create()
        .setChild(
            new Gauge.Child() {
              @Override
              public double get() {
                return zstdBufferPool.getNumActive();
              }
            })
        .register();

    int inputFetchStageWidth = configs.getWorker().getInputFetchStageWidth();
    int executeStageWidth = configs.getWorker().getExecuteStageWidth();
    int reportResultStageWidth = configs.getWorker().getReportResultStageWidth();
    LocalResourceSet resourceSet = LocalResourceSetUtils.create(configs.getWorker().getResources());
    List<String> execOwners = configs.getWorker().getExecOwners();
    if (!execOwners.isEmpty()
        && execOwners.size() < inputFetchStageWidth + executeStageWidth + reportResultStageWidth) {
      throw new ConfigurationException(
          "execOwners is not large enough to fill requested stage widths");
    }

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
            zstdBufferPool,
            configs.getWorker().getStorages());
    // may modify resourceSet to provide additional resources
    execFileSystem =
        createExecFileSystem(
            remoteInputStreamFactory,
            removeDirectoryService,
            accessRecorder,
            fetchService,
            storage,
            resourceSet,
            configs.getWorker().getExecOwner(),
            execOwners);

    instance = new WorkerInstance(configs.getWorker().getPublicName(), backplane, storage);

    // Create the appropriate writer for the context
    CasWriter writer;
    if (!configs.getWorker().getCapabilities().isCas()) {
      Retrier retrier = new Retrier(Backoff.sequential(5), Retrier.DEFAULT_IS_RETRIABLE);
      writer = new RemoteCasWriter(backplane, workerStubs, retrier);
    } else {
      writer = new LocalCasWriter(execFileSystem);
    }

    context =
        new ShardWorkerContext(
            configs.getWorker().getPublicName(),
            Duration.newBuilder().setSeconds(configs.getWorker().getOperationPollPeriod()).build(),
            backplane::pollExecution,
            inputFetchStageWidth,
            executeStageWidth,
            reportResultStageWidth,
            configs.getWorker().getInputFetchDeadline(),
            backplane,
            execFileSystem,
            new EmptyInputStreamFactory(
                new FailoverInputStreamFactory(
                    execFileSystem.getStorage(), remoteInputStreamFactory)),
            configs.getWorker().getExecutionPolicies(),
            instance,
            Duration.newBuilder().setSeconds(configs.getDefaultActionTimeout()).build(),
            Duration.newBuilder().setSeconds(configs.getMaximumActionTimeout()).build(),
            configs.getWorker().getDefaultMaxCores(),
            configs.getWorker().isLimitGlobalExecution(),
            configs.getWorker().isOnlyMulticoreTests(),
            configs.getWorker().isAllowBringYourOwnContainer(),
            configs.getWorker().isErrorOperationRemainingResources(),
            configs.getWorker().isErrorOperationOutputSizeExceeded(),
            resourceSet,
            writer);

    pipeline = new Pipeline();
    SuperscalarPipelineStage inputFetchStage = null;
    SuperscalarPipelineStage executeActionStage = null;
    SuperscalarPipelineStage reportResultStage = null;
    PutOperationStage completeStage = null;
    if (configs.getWorker().getCapabilities().isExecution()) {
      // all claims should be released upon pipeline completion, but it is safe to ensure that
      // they are whether empty or not, so do so.
      completeStage = new ReleaseClaimStage(operation -> context.deactivate(operation.getName()));
      PipelineStage errorStage = completeStage; /* new ErrorStage(); */
      reportResultStage = new ReportResultStage(context, completeStage, errorStage);
      executeActionStage = new ExecuteActionStage(context, reportResultStage, errorStage);
      // FIXME this implies and requires that context::requeue performs the context::deactivate
      // function
      PipelineStage releaseClaimAndRequeueStage = new ReleaseClaimStage(context::requeue);
      inputFetchStage =
          new InputFetchStage(context, executeActionStage, releaseClaimAndRequeueStage);
      matchStage = new MatchStage(context, inputFetchStage, releaseClaimAndRequeueStage);

      pipeline.add(matchStage, 4);
      pipeline.add(inputFetchStage, 3);
      pipeline.add(executeActionStage, 2);
      pipeline.add(reportResultStage, 1);
    }

    String workerName = InetAddress.getLocalHost().getHostName();

    WorkerProfileService workerProfileService =
        new WorkerProfileService(
            workerName,
            configs.getWorker().getPublicName(),
            (CASFileCache) storage,
            matchStage,
            inputFetchStage,
            executeActionStage,
            reportResultStage,
            completeStage);
    server = createServer(serverBuilder, instance, workerProfileService);

    removeWorker(configs.getWorker().getPublicName());

    boolean skipLoad = configs.getWorker().getStorages().getFirst().isSkipLoad();
    ListenableFuture<Void> fileSystemStarted =
        execFileSystem.start(
            (digests) -> addBlobsLocation(digests, configs.getWorker().getPublicName()),
            skipLoad,
            startWritable);

    server.start();
    Futures.addCallback(
        fileSystemStarted,
        new FutureCallback<>() {
          @Override
          public void onSuccess(Void result) {
            healthStatusManager.setStatus(
                HealthStatusManager.SERVICE_NAME_ALL_SERVICES, ServingStatus.SERVING);
            PrometheusPublisher.startHttpServer(configs.getPrometheusPort());
          }

          @Override
          public void onFailure(Throwable t) {
            log.log(SEVERE, "execFileSystem start failure", t);
          }
        },
        directExecutor());
    startFailsafeRegistration();

    pipeline.start();
    healthCheckMetric.labels("start").inc();
    inputFetchSlotsTotal.set(inputFetchStageWidth);
    executionSlotsTotal.set(executeStageWidth);
    reportResultSlotsTotal.set(reportResultStageWidth);

    log.log(INFO, String.format("%s initialized", identifier));
  }

  @Override
  protected void onShutdown() throws InterruptedException {
    initiateShutdown();
    awaitRelease();
  }

  private void awaitTermination() throws InterruptedException {
    pipeline.join();
    if (server != null && !server.isTerminated()) {
      int retries = 5;
      while (retries > 0) {
        if (shutdownInitiated.get()) {
          server.shutdown();
          retries--;
        }
        if (server.awaitTermination(shutdownWaitTimeInSeconds, SECONDS)) {
          retries = 1;
          break;
        }
      }
      if (retries == 0) {
        server.shutdownNow();
        server.awaitTermination();
      }
    }
  }

  public Iterable<PipelineChange> pipelineChange(Iterable<PipelineChange> changes) {
    for (PipelineChange change : changes) {
      for (PipelineStage stage : pipeline) {
        if (change.getStage().equals(stage.getName())) {
          stage.setPaused(change.getPaused());
          if (change.getWidth() > 0) {
            stage.setWidth(change.getWidth());
          }
        }
      }
    }

    return Iterables.transform(
        pipeline,
        stage ->
            PipelineChange.newBuilder()
                .setStage(stage.getName())
                .setPaused(stage.isPaused())
                .setWidth(stage.getWidth())
                .build());
  }

  public void initiateShutdown() {
    if (context != null) {
      context.prepareForGracefulShutdown();
    }
    if (pipeline != null) {
      pipeline.interrupt(matchStage);
    }
    if (server != null) {
      server.shutdown();
    }
    shutdownInitiated.set(true);
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
    prepareWorkerForGracefulShutdown();
    // Clean-up any cgroups that were possibly created/mutated.
    Group.onShutdown();
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
      try {
        execFileSystem.stop();
      } catch (IOException e) {
        log.log(SEVERE, "error shutting down exec filesystem", e);
      }
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
    } catch (Exception e) {
      log.log(Level.SEVERE, "Error running application", e);
    } finally {
      worker.stop();
    }
  }
}
