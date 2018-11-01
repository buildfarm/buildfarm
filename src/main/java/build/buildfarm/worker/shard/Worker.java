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
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.common.ShardBackplane;
import build.buildfarm.instance.memory.MemoryLRUContentAddressableStorage;
import build.buildfarm.instance.shard.RedisShardBackplane;
import build.buildfarm.server.InstanceNotFoundException;
import build.buildfarm.server.Instances;
import build.buildfarm.server.ContentAddressableStorageService;
import build.buildfarm.server.ByteStreamService;
import build.buildfarm.worker.CASFileCache;
import build.buildfarm.worker.ExecuteActionStage;
import build.buildfarm.worker.FuseCAS;
import build.buildfarm.worker.InputFetchStage;
import build.buildfarm.worker.MatchStage;
import build.buildfarm.worker.Pipeline;
import build.buildfarm.worker.PipelineStage;
import build.buildfarm.worker.PutOperationStage;
import build.buildfarm.worker.ReportResultStage;
import build.buildfarm.worker.WorkerContext;
import build.buildfarm.v1test.QueuedOperationMetadata;
import build.buildfarm.v1test.ShardWorkerConfig;
import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import com.google.devtools.common.options.OptionsParser;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.TextFormat;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.Status.Code;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
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
  private final ExecFileSystem execFileSystem;
  private final Pipeline pipeline;
  private final ShardBackplane backplane;
  private long workerRegistrationExpiresAt = 0;
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

  private Operation stripOperation(Operation operation) {
    return instance.stripOperation(operation);
  }

  private Operation stripQueuedOperation(Operation operation) {
    return instance.stripQueuedOperation(operation);
  }

  public Worker(ServerBuilder<?> serverBuilder, ShardWorkerConfig config) throws ConfigurationException {
    this.config = config;
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

    execFileSystem = createExecFileSystem(
        new RemoteInputStreamFactory(
            config.getPublicName(),
            backplane,
            new Random(),
            digestUtil));

    instance = new ShardWorkerInstance(
        config.getPublicName(),
        digestUtil,
        backplane,
        execFileSystem.getStorage(),
        execFileSystem,
        execFileSystem,
        config.getShardWorkerInstanceConfig());

    Instances instances = Instances.singular(instance);
    server = serverBuilder
        .addService(new ContentAddressableStorageService(instances))
        .addService(new ByteStreamService(instances))
        .build();

    WorkerContext context = new ShardWorkerContext(
        config.getPublicName(),
        config.getPlatform(),
        config.getOperationPollPeriod(),
        backplane::pollOperation,
        config.getInlineContentLimit(),
        config.getExecuteStageWidth(),
        execFileSystem,
        instance);

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

  private ExecFileSystem createFuseExecFileSystem(InputStreamFactory remoteInputStreamFactory) {
    final ContentAddressableStorage storage = new MemoryLRUContentAddressableStorage(config.getCasMaxSizeBytes(), this::onStoragePut);
    InputStreamFactory storageInputStreamFactory = (digest, offset) -> storage.get(digest).getData().substring((int) offset).newInput();

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
    return new FuseExecFileSystem(
        root,
        new FuseCAS(root,
            new EmptyInputStreamFactory(
                new FailoverInputStreamFactory(storageInputStreamFactory, localPopulatingInputStreamFactory))),
        storage);
  }

  private ExecFileSystem createExecFileSystem(InputStreamFactory remoteInputStreamFactory) throws ConfigurationException {
    if (config.getUseFuseCas()) {
      return createFuseExecFileSystem(remoteInputStreamFactory);
    }
    return createCFCExecFileSystem(
        remoteInputStreamFactory,
        getValidCasCacheDirectory(config, root));
  }

  private ExecFileSystem createCFCExecFileSystem(InputStreamFactory remoteInputStreamFactory, Path casCacheDirectory) {
    CASFileCache fileCache = new ShardCASFileCache(
        remoteInputStreamFactory,
        root.resolve(casCacheDirectory),
        config.getCasCacheMaxSizeBytes(),
        digestUtil,
        this::onStoragePut,
        this::onStorageExpire);

    return new CFCExecFileSystem(root, fileCache, config.getLinkInputDirectories());
  }

  public void stop() throws InterruptedException {
    System.err.println("Closing the pipeline");
    try {
      pipeline.close();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    System.err.println("Stopping exec filesystem");
    execFileSystem.stop();
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
    // should really be waiting for either server or pipeline shutdown
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
    execFileSystem.stop();
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

  private void startFailsafeRegistration() {
    new Thread(() -> {
      try {
        while (!server.isShutdown()) {
          long now = System.currentTimeMillis();
          if (now >= workerRegistrationExpiresAt) {
            // worker must be registered to match
            addWorker(config.getPublicName());
            // update every 10 seconds
            workerRegistrationExpiresAt = now + 10000;
          }
          TimeUnit.SECONDS.sleep(1);
        }
      } catch (InterruptedException e) {
        try {
          stop();
        } catch (InterruptedException ie) {
          // ignore
        }
      }
    }).start();
  }

  public void start() throws InterruptedException {
    try {
      backplane.start();

      removeWorker(config.getPublicName());

      execFileSystem.start((digests) -> addBlobsLocation(digests, config.getPublicName()));

      server.start();
      startFailsafeRegistration();
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
