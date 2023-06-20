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

import static build.buildfarm.common.io.Utils.formatIOError;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.logging.Level.SEVERE;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.common.config.GrpcMetrics;
import build.buildfarm.common.grpc.TracingMetadataUtils.ServerHeadersInterceptor;
import build.buildfarm.common.services.ByteStreamService;
import build.buildfarm.common.services.ContentAddressableStorageService;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.shard.ShardInstance;
import build.buildfarm.metrics.prometheus.PrometheusPublisher;
import build.buildfarm.server.controllers.WebController;
import build.buildfarm.server.services.ActionCacheService;
import build.buildfarm.server.services.AdminService;
import build.buildfarm.server.services.CapabilitiesService;
import build.buildfarm.server.services.ExecutionService;
import build.buildfarm.server.services.FetchService;
import build.buildfarm.server.services.OperationQueueService;
import build.buildfarm.server.services.OperationsService;
import build.buildfarm.server.services.PublishBuildEventService;
import com.google.devtools.common.options.OptionsParsingException;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.services.HealthStatusManager;
import io.grpc.util.TransmitStatusRuntimeExceptionInterceptor;
import io.prometheus.client.Counter;
import java.io.File;
import java.io.IOException;
import java.security.Security;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.naming.ConfigurationException;
import lombok.extern.java.Log;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SuppressWarnings("deprecation")
@Log
@SpringBootApplication
@ComponentScan("build.buildfarm")
public class BuildFarmServer {
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
  private boolean stopping = false;
  private static BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  private ShardInstance createInstance()
      throws IOException, ConfigurationException, InterruptedException {
    return new ShardInstance(
        configs.getServer().getName(),
        configs.getServer().getSession() + "-" + configs.getServer().getName(),
        new DigestUtil(configs.getDigestFunction()),
        this::stop);
  }

  public synchronized void start(ServerBuilder<?> serverBuilder, String publicName)
      throws IOException, ConfigurationException, InterruptedException {
    instance = createInstance();

    healthStatusManager = new HealthStatusManager();

    ServerInterceptor headersInterceptor = new ServerHeadersInterceptor();
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
        .addService(new ActionCacheService(instance))
        .addService(new CapabilitiesService(instance))
        .addService(new ContentAddressableStorageService(instance))
        .addService(new ByteStreamService(instance))
        .addService(new ExecutionService(instance, keepaliveScheduler))
        .addService(new OperationQueueService(instance))
        .addService(new OperationsService(instance))
        .addService(new AdminService(instance))
        .addService(new FetchService(instance))
        .addService(ProtoReflectionService.newInstance())
        .addService(new PublishBuildEventService())
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

    checkState(!stopping, "must not call start after stop");
    instance.start(publicName);
    WebController.setInstance((ShardInstance) instance);
    server.start();

    healthStatusManager.setStatus(
        HealthStatusManager.SERVICE_NAME_ALL_SERVICES, ServingStatus.SERVING);
    PrometheusPublisher.startHttpServer(configs.getPrometheusPort());
    healthCheckMetric.labels("start").inc();
  }

  @PreDestroy
  public void stop() {
    synchronized (this) {
      if (stopping) {
        return;
      }
      stopping = true;
    }
    System.err.println("*** shutting down gRPC server since JVM is shutting down");
    healthStatusManager.setStatus(
        HealthStatusManager.SERVICE_NAME_ALL_SERVICES, ServingStatus.NOT_SERVING);
    PrometheusPublisher.stopHttpServer();
    healthCheckMetric.labels("stop").inc();
    try {
      if (server != null) {
        server.shutdown();
      }
      instance.stop();
      server.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      if (server != null) {
        server.shutdownNow();
      }
    }
    if (!shutdownAndAwaitTermination(keepaliveScheduler, 10, TimeUnit.SECONDS)) {
      log.warning("could not shut down keepalive scheduler");
    }
    System.err.println("*** server shut down");
  }

  @PostConstruct
  public void init() throws OptionsParsingException {
    // Only log severe log messages from Netty. Otherwise it logs warnings that look like this:
    //
    // 170714 08:16:28.552:WT 18 [io.grpc.netty.NettyServerHandler.onStreamError] Stream Error
    // io.netty.handler.codec.http2.Http2Exception$StreamException: Received DATA frame for an
    // unknown stream 11369
    nettyLogger.setLevel(SEVERE);

    try {
      start(
          ServerBuilder.forPort(configs.getServer().getPort()),
          configs.getServer().getPublicName());
    } catch (IOException e) {
      System.err.println("error: " + formatIOError(e));
    } catch (InterruptedException e) {
      System.err.println("error: interrupted");
    } catch (ConfigurationException e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) throws ConfigurationException {
    configs = BuildfarmConfigs.loadServerConfigs(args);

    // Configure Spring
    SpringApplication app = new SpringApplication(BuildFarmServer.class);
    Map<String, Object> springConfig = new HashMap<>();

    // Disable Logback
    System.setProperty("org.springframework.boot.logging.LoggingSystem", "none");

    springConfig.put("ui.frontend.enable", configs.getUi().isEnable());
    springConfig.put("server.port", configs.getUi().getPort());
    app.setDefaultProperties(springConfig);

    System.out.println("Starting Application");
    try {
      app.run(args);
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }
}
