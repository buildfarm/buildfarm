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

package build.buildfarm.server;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.logging.Level.SEVERE;

import build.buildfarm.common.LoggingMain;
import build.buildfarm.common.grpc.TracingMetadataUtils.ServerHeadersInterceptor;
import build.buildfarm.metrics.MetricsPublisher;
import build.buildfarm.metrics.aws.AwsMetricsPublisher;
import build.buildfarm.metrics.gcp.GcpMetricsPublisher;
import build.buildfarm.metrics.log.LogMetricsPublisher;
import build.buildfarm.metrics.prometheus.PrometheusPublisher;
import build.buildfarm.v1test.BuildFarmServerConfig;
import build.buildfarm.v1test.MetricsConfig;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.services.HealthStatusManager;
import io.grpc.util.TransmitStatusRuntimeExceptionInterceptor;
import io.prometheus.client.Counter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.naming.ConfigurationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class BuildFarmServer extends LoggingMain {
  // We need to keep references to the root and netty loggers to prevent them from being garbage
  // collected, which would cause us to loose their configuration.
  private static final java.util.logging.Logger nettyLogger =
      java.util.logging.Logger.getLogger("io.grpc.netty");
  private static final Logger logger = Logger.getLogger(BuildFarmServer.class.getName());
  private static final Counter healthCheckMetric =
      Counter.build()
          .name("health_check")
          .labelNames("lifecycle")
          .help("Service health check.")
          .register();

  private final ScheduledExecutorService keepaliveScheduler = newSingleThreadScheduledExecutor();
  private final Instances instances;
  private final HealthStatusManager healthStatusManager;
  private final Server server;
  private boolean stopping = false;
  private final PrometheusPublisher prometheusPublisher;

  @Autowired
  BuildFarmServerOptions options;

  @Autowired
  BuildFarmServerConfig config;

  @Autowired
  public BuildFarmServer(String session, BuildFarmServerConfig config)
          throws InterruptedException, ConfigurationException, IOException {
    this(session, ServerBuilder.forPort(config.getPort()), config);
  }

  public BuildFarmServer(
      String session, ServerBuilder<?> serverBuilder, BuildFarmServerConfig config)
          throws InterruptedException, ConfigurationException, IOException {
    super("BuildFarmServer");
    System.out.println("In constructor");
    nettyLogger.setLevel(SEVERE);
    String defaultInstanceName = config.getDefaultInstanceName();
    instances =
        new BuildFarmInstances(session, config.getInstancesList(), defaultInstanceName, this::stop);

    healthStatusManager = new HealthStatusManager();

    ServerInterceptor headersInterceptor = new ServerHeadersInterceptor();

    server =
        serverBuilder
            .addService(healthStatusManager.getHealthService())
            .addService(new ActionCacheService(instances))
            .addService(new CapabilitiesService(instances))
            .addService(
                new ContentAddressableStorageService(
                    instances,
                    /* deadlineAfter=*/ 1,
                    TimeUnit.DAYS,
                    /* requestLogLevel=*/ Level.INFO))
            .addService(new ByteStreamService(instances, /* writeDeadlineAfter=*/ 1, TimeUnit.DAYS))
            .addService(
                new ExecutionService(
                    instances,
                    config.getExecuteKeepaliveAfterSeconds(),
                    TimeUnit.SECONDS,
                    keepaliveScheduler,
                    getMetricsPublisher(config.getMetricsConfig())))
            .addService(new OperationQueueService(instances))
            .addService(new OperationsService(instances))
            .addService(new AdminService(config.getAdminConfig(), instances))
            .addService(new FetchService(instances))
            .addService(ProtoReflectionService.newInstance())
            .addService(new PublishBuildEventService(config.getBuildEventConfig()))
            .intercept(TransmitStatusRuntimeExceptionInterceptor.instance())
            .intercept(headersInterceptor)
            .build();

    prometheusPublisher = new PrometheusPublisher();

    this.start(options.publicName, config.getPrometheusConfig().getPort());
    this.blockUntilShutdown();
    this.stop();
    System.exit(0);

    logger.log(Level.INFO, String.format("%s initialized", session));
  }

  private static MetricsPublisher getMetricsPublisher(MetricsConfig metricsConfig) {
    switch (metricsConfig.getMetricsDestination()) {
      default:
        return new LogMetricsPublisher(metricsConfig);
      case "aws":
        return new AwsMetricsPublisher(metricsConfig);
      case "gcp":
        return new GcpMetricsPublisher(metricsConfig);
    }
  }

  public synchronized void start(String publicName, int prometheusPort) throws IOException {
    checkState(!stopping, "must not call start after stop");
    instances.start(publicName);
    server.start();
    healthStatusManager.setStatus(
        HealthStatusManager.SERVICE_NAME_ALL_SERVICES, ServingStatus.SERVING);
    prometheusPublisher.startHttpServer(prometheusPort);
    healthCheckMetric.labels("start").inc();
  }

  @Override
  protected void onShutdown() {
    System.err.println("*** shutting down gRPC server since JVM is shutting down");
    stop();
    System.err.println("*** server shut down");
  }

  public void stop() {
    synchronized (this) {
      if (stopping) {
        return;
      }
      stopping = true;
    }
    healthStatusManager.setStatus(
        HealthStatusManager.SERVICE_NAME_ALL_SERVICES, ServingStatus.NOT_SERVING);
    prometheusPublisher.stopHttpServer();
    healthCheckMetric.labels("stop").inc();
    try {
      if (server != null) {
        server.shutdown();
      }
      instances.stop();
      server.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      if (server != null) {
        server.shutdownNow();
      }
    }
    if (!shutdownAndAwaitTermination(keepaliveScheduler, 10, TimeUnit.SECONDS)) {
      logger.log(Level.WARNING, "could not shut down keepalive scheduler");
    }
  }

  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  @EventListener(ApplicationReadyEvent.class)
  private void init() {

  }
}
