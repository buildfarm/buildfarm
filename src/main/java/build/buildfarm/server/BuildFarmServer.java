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
import build.buildfarm.common.config.ServerOptions;
import build.buildfarm.common.grpc.TracingMetadataUtils.ServerHeadersInterceptor;
import build.buildfarm.common.services.ByteStreamService;
import build.buildfarm.common.services.ContentAddressableStorageService;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.shard.ShardInstance;
import build.buildfarm.metrics.prometheus.PrometheusPublisher;
import build.buildfarm.server.services.ActionCacheService;
import build.buildfarm.server.services.AdminService;
import build.buildfarm.server.services.CapabilitiesService;
import build.buildfarm.server.services.ExecutionService;
import build.buildfarm.server.services.FetchService;
import build.buildfarm.server.services.OperationQueueService;
import build.buildfarm.server.services.OperationsService;
import build.buildfarm.server.services.PublishBuildEventService;
import com.google.devtools.common.options.OptionsParser;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.naming.ConfigurationException;
import lombok.extern.java.Log;
import me.dinowernli.grpc.prometheus.Configuration;
import me.dinowernli.grpc.prometheus.MonitoringServerInterceptor;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SuppressWarnings("deprecation")
@Log
@SpringBootApplication
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

  private static String[] args;
  private static BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  private void printUsage(OptionsParser parser) {
    log.log(Level.INFO, "Usage: CONFIG_PATH");
    log.log(
        Level.INFO,
        parser.describeOptions(Collections.emptyMap(), OptionsParser.HelpVerbosity.LONG));
  }

  public synchronized void start(ServerBuilder<?> serverBuilder, String publicName)
      throws IOException, ConfigurationException, InterruptedException {
    instance =
        new ShardInstance(
            configs.getServer().getName(),
            configs.getServer().getSession() + "-" + configs.getServer().getName(),
            new DigestUtil(configs.getDigestFunction()),
            this::stop);

    healthStatusManager = new HealthStatusManager();

    ServerInterceptor headersInterceptor = new ServerHeadersInterceptor();
    if (configs.getServer().getSslCertificatePath() != null) {
      // There are different Public Key Cryptography Standards (PKCS) that users may format their
      // certificate files in.  By default, the JDK cannot parse all of them.  In particular, it
      // cannot parse PKCS #1 (RSA Cryptography Standard).  When enabling TLS for GRPC, java's
      // underlying Security module is used. To improve the robustness of this parsing and the
      // overall accepted certificate formats, we add an additional security provider. BouncyCastle
      // is a library that will parse additional formats and allow users to provide certificates in
      // an otherwise unsupported format.
      Security.addProvider(new BouncyCastleProvider());
      File ssl_certificate_path = new File(configs.getServer().getSslCertificatePath());
      serverBuilder.useTransportSecurity(ssl_certificate_path, ssl_certificate_path);
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
    handleGrpcMetricIntercepts(serverBuilder);
    server = serverBuilder.build();

    log.log(Level.INFO, String.format("%s initialized", configs.getServer().getSession()));

    checkState(!stopping, "must not call start after stop");
    instance.start(publicName);
    server.start();
    healthStatusManager.setStatus(
        HealthStatusManager.SERVICE_NAME_ALL_SERVICES, ServingStatus.SERVING);
    PrometheusPublisher.startHttpServer(configs.getPrometheusPort());
    healthCheckMetric.labels("start").inc();
  }

  public void handleGrpcMetricIntercepts(ServerBuilder<?> serverBuilder) {
    // Decide how to capture GRPC Prometheus metrics.
    // By default, we don't capture any.
    if (configs.getServer().getGrpcMetrics().isEnabled()) {
      // Assume core metrics.
      // Core metrics include send/receive totals tagged with return codes.  No latencies.
      Configuration grpcConfig = Configuration.cheapMetricsOnly();

      // Enable latency buckets.
      if (configs.getServer().getGrpcMetrics().isProvideLatencyHistograms()) {
        grpcConfig = grpcConfig.allMetrics();
      }

      // Apply config to create an interceptor and apply it to the GRPC server.
      MonitoringServerInterceptor monitoringInterceptor =
          MonitoringServerInterceptor.create(grpcConfig);
      serverBuilder.intercept(monitoringInterceptor);
    }
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
      log.log(Level.WARNING, "could not shut down keepalive scheduler");
    }
    System.err.println("*** server shut down");
  }

  @PostConstruct
  public void init() {
    // Only log severe log messages from Netty. Otherwise it logs warnings that look like this:
    //
    // 170714 08:16:28.552:WT 18 [io.grpc.netty.NettyServerHandler.onStreamError] Stream Error
    // io.netty.handler.codec.http2.Http2Exception$StreamException: Received DATA frame for an
    // unknown stream 11369
    nettyLogger.setLevel(SEVERE);

    OptionsParser parser = OptionsParser.newOptionsParser(ServerOptions.class);
    parser.parseAndExitUponError(args);
    List<String> residue = parser.getResidue();
    if (residue.isEmpty()) {
      printUsage(parser);
    }

    ServerOptions options = parser.getOptions(ServerOptions.class);

    try {
      configs = BuildfarmConfigs.loadConfigs(residue.get(0));
    } catch (IOException e) {
      log.severe("Could not parse yml configuration file." + e);
    }

    if (!options.publicName.isEmpty()) {
      configs.getServer().setPublicName(options.publicName);
    }
    if (options.port > 0) {
      configs.getServer().setPort(options.port);
    }
    log.info(configs.toString());
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

  public static void main(String[] args) {
    BuildFarmServer.args = args;
    SpringApplication.run(BuildFarmServer.class, args);
  }
}
