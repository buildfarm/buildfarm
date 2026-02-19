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

package build.buildfarm.server;

import static build.buildfarm.common.io.Utils.formatIOError;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;

import build.buildfarm.common.LoggingMain;
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.common.config.GrpcMetrics;
import build.buildfarm.common.grpc.TracingMetadataUtils.ServerHeadersInterceptor;
import build.buildfarm.common.services.ByteStreamService;
import build.buildfarm.common.services.ContentAddressableStorageService;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.shard.ServerInstance;
import build.buildfarm.metrics.prometheus.PrometheusPublisher;
import build.buildfarm.server.services.ActionCacheService;
import build.buildfarm.server.services.CapabilitiesService;
import build.buildfarm.server.services.ExecutionService;
import build.buildfarm.server.services.FetchService;
import build.buildfarm.server.services.OperationQueueService;
import build.buildfarm.server.services.OperationsService;
import build.buildfarm.server.services.PublishBuildEventService;
import build.buildfarm.server.services.WorkerControlProxyService;
import build.buildfarm.server.services.WorkerProfileService;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.util.TransmitStatusRuntimeExceptionInterceptor;
import io.prometheus.client.Counter;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.Security;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.naming.ConfigurationException;
import lombok.extern.java.Log;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

@Log
public class BuildFarmServer extends LoggingMain {
  private static final java.util.logging.Logger nettyLogger =
      java.util.logging.Logger.getLogger("io.grpc.netty");
  private static final Counter healthCheckMetric =
      Counter.build()
          .name("health_check")
          .labelNames("lifecycle")
          .help("Service health check.")
          .register();

  private final ScheduledExecutorService keepaliveScheduler = newSingleThreadScheduledExecutor();
  private Instance instance;
  private HealthStatusManager healthStatusManager;
  private io.grpc.Server server;
  private static BuildfarmConfigs configs = BuildfarmConfigs.getInstance();
  private AtomicBoolean shutdownInitiated = new AtomicBoolean(true);
  private AtomicBoolean released = new AtomicBoolean(true);
  private InvocationsCollector invocationsCollector;
  private Thread invocationsCollectorThread;

  BuildFarmServer() {
    super("BuildFarmServer");
  }

  /**
   * The method will prepare the server for graceful shutdown when the server is ready. Current
   * implementation waits specified period of time. Future improvements could be to keep track of
   * open connections and shutdown when there are not left. Note on using stderr here instead of
   * log. By the time this is called in PreDestroy, the log is no longer available and is not
   * logging messages.
   */
  public void prepareServerForGracefulShutdown() {
    if (configs.getServer().getGracefulShutdownSeconds() == 0) {
      log.severe("Graceful Shutdown is not enabled. Server is shutting down immediately.");
    } else {
      try {
        log.info(
            String.format(
                "Graceful Shutdown - Waiting %d to allow connections to drain.",
                configs.getServer().getGracefulShutdownSeconds()));
        SECONDS.sleep(configs.getServer().getGracefulShutdownSeconds());
      } catch (InterruptedException e) {
        log.severe(
            "Graceful Shutdown - The server graceful shutdown is interrupted: " + e.getMessage());
      } finally {
        log.info(
            String.format(
                "Graceful Shutdown - It took the server %d seconds to shutdown",
                configs.getServer().getGracefulShutdownSeconds()));
      }
    }
  }

  private ServerInstance createInstance()
      throws IOException, ConfigurationException, InterruptedException {
    return new ServerInstance(
        configs.getServer().getName(),
        configs.getServer().getSession() + "-" + configs.getServer().getName(),
        this::initiateShutdown);
  }

  public synchronized void start(ServerBuilder<?> serverBuilder, String publicName)
      throws IOException, ConfigurationException, InterruptedException {
    shutdownInitiated.set(false);
    released.set(false);
    // FIXME change to instance = ...; instance.start();
    ServerInstance serverInstance = createInstance();
    instance = serverInstance;

    healthStatusManager = new HealthStatusManager();

    invocationsCollector = new InvocationsCollector(serverInstance);
    invocationsCollectorThread = new Thread(invocationsCollector);
    invocationsCollectorThread.start();

    ServerInterceptor headersInterceptor = new ServerHeadersInterceptor(invocationsCollector::add);
    if (configs.getServer().getSslCertificatePath() != null
        && configs.getServer().getSslPrivateKeyPath() != null) {
      // There are different Public Key Cryptography Standards (PKCS) that users may format their
      // certificate files in.  By default, the JDK cannot parse all of them.  In particular, it
      // cannot parse PKCS #1 (RSA Cryptography Standard).  When enabling TLS for GRPC, java's
      // underlying Security module is used. To improve the robustness of this parsing and the
      // overall accepted certificate formats, we add an additional security provider. BouncyCastle
      // is a library that will parse additional formats and allow users to provide certificates in
      // an otherwise unsupported format.
      Security.addProvider(new BouncyCastleProvider());
      File ssl_certificate = new File(configs.getServer().getSslCertificatePath());
      File ssl_private_key = new File(configs.getServer().getSslPrivateKeyPath());
      serverBuilder.useTransportSecurity(ssl_certificate, ssl_private_key);
    }

    serverBuilder
        .addService(healthStatusManager.getHealthService())
        .addService(new ActionCacheService(instance, !configs.getServer().isActionCacheReadOnly()))
        .addService(new CapabilitiesService(instance))
        .addService(new ContentAddressableStorageService(instance))
        .addService(new ByteStreamService(instance))
        .addService(new ExecutionService(instance, keepaliveScheduler))
        .addService(new OperationQueueService(instance))
        .addService(new OperationsService(instance))
        .addService(new FetchService(instance))
        .addService(ProtoReflectionService.newInstance())
        .addService(new PublishBuildEventService())
        .addService(new WorkerProfileService(instance))
        .addService(new WorkerControlProxyService(instance))
        .intercept(TransmitStatusRuntimeExceptionInterceptor.instance())
        .intercept(headersInterceptor);
    GrpcMetrics.handleGrpcMetricIntercepts(serverBuilder, configs.getServer().getGrpcMetrics());

    if (configs.getServer().getMaxInboundMessageSizeBytes() != 0) {
      serverBuilder.maxInboundMessageSize(configs.getServer().getMaxInboundMessageSizeBytes());
    }
    if (configs.getServer().getMaxInboundMetadataSize() != 0) {
      serverBuilder.maxInboundMetadataSize(configs.getServer().getMaxInboundMetadataSize());
    }
    server = serverBuilder.build();

    log.info(String.format("%s initialized", configs.getServer().getSession()));

    instance.start(publicName);
    server.start();

    healthStatusManager.setStatus(
        HealthStatusManager.SERVICE_NAME_ALL_SERVICES, ServingStatus.SERVING);
    PrometheusPublisher.startHttpServer(configs.getPrometheusPort());
    healthCheckMetric.labels("start").inc();
  }

  private synchronized void awaitRelease() throws InterruptedException {
    while (!released.get()) {
      wait();
    }
  }

  synchronized void stop() throws InterruptedException {
    try {
      shutdown();
    } finally {
      released.set(true);
      notify();
    }
  }

  private void shutdown() throws InterruptedException {
    log.info("*** shutting down gRPC server since JVM is shutting down");
    prepareServerForGracefulShutdown();
    if (healthStatusManager != null) {
      healthStatusManager.setStatus(
          HealthStatusManager.SERVICE_NAME_ALL_SERVICES, ServingStatus.NOT_SERVING);
    }
    PrometheusPublisher.stopHttpServer();
    healthCheckMetric.labels("stop").inc();
    try {
      initiateShutdown();
      instance.stop();
      if (server != null && server.awaitTermination(10, TimeUnit.SECONDS)) {
        server = null;
      }
    } catch (InterruptedException e) {
      if (server != null) {
        server.shutdownNow();
        server = null;
      }
      throw e;
    } catch (RuntimeException e) {
      log.log(SEVERE, "error stopping instance", e);
    }
    if (!shutdownAndAwaitTermination(keepaliveScheduler, 10, TimeUnit.SECONDS)) {
      log.warning("could not shut down keepalive scheduler");
    }
    if (invocationsCollector != null) {
      invocationsCollector.clear();
      invocationsCollectorThread.interrupt();
      invocationsCollectorThread.join();
    }
    log.info("*** server shut down");
  }

  @Override
  protected void onShutdown() throws InterruptedException {
    initiateShutdown();
    awaitRelease();
  }

  private void initiateShutdown() {
    shutdownInitiated.set(true);
    if (server != null) {
      server.shutdown();
    }
  }

  private void awaitTermination() throws InterruptedException {
    while (!shutdownInitiated.get()) {
      if (server != null && server.awaitTermination(1, TimeUnit.SECONDS)) {
        server = null;
        shutdownInitiated.set(true);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    // Only log severe log messages from Netty. Otherwise it logs warnings that look like this:
    //
    // 170714 08:16:28.552:WT 18 [io.grpc.netty.NettyServerHandler.onStreamError] Stream Error
    // io.netty.handler.codec.http2.Http2Exception$StreamException: Received DATA frame for an
    // unknown stream 11369
    nettyLogger.setLevel(SEVERE);

    configs = BuildfarmConfigs.loadServerConfigs(args);

    BuildFarmServer server = new BuildFarmServer();
    SocketAddress socketAddress;
    if (configs.getServer().getBindAddress().isEmpty()) {
      socketAddress = new InetSocketAddress(configs.getServer().getPort());
    } else {
      socketAddress =
          new InetSocketAddress(
              configs.getServer().getBindAddress(), configs.getServer().getPort());
    }

    try {
      server.start(
          NettyServerBuilder.forAddress(socketAddress), configs.getServer().getPublicName());
      server.awaitTermination();
    } catch (IOException e) {
      log.severe("error: " + formatIOError(e));
    } catch (InterruptedException e) {
      log.log(WARNING, "interrupted", e);
    } catch (Exception e) {
      log.log(SEVERE, "Error running application", e);
    } finally {
      server.stop();
    }
  }
}
