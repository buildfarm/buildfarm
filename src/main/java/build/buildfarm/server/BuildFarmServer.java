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

import build.buildfarm.common.LoggingMain;
import build.buildfarm.common.config.ConfigAdjuster;
import build.buildfarm.common.config.ServerOptions;
import build.buildfarm.common.grpc.TracingMetadataUtils.ServerHeadersInterceptor;
import build.buildfarm.instance.Instance;
import build.buildfarm.metrics.MetricsPublisher;
import build.buildfarm.metrics.aws.AwsMetricsPublisher;
import build.buildfarm.metrics.gcp.GcpMetricsPublisher;
import build.buildfarm.metrics.log.LogMetricsPublisher;
import build.buildfarm.metrics.prometheus.PrometheusPublisher;
import build.buildfarm.v1test.BuildFarmServerConfig;
import build.buildfarm.v1test.MetricsConfig;
import com.google.devtools.common.options.OptionsParser;
import com.google.protobuf.TextFormat;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.services.HealthStatusManager;
import io.grpc.util.TransmitStatusRuntimeExceptionInterceptor;
import io.prometheus.client.Counter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Security;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.naming.ConfigurationException;
import me.dinowernli.grpc.prometheus.Configuration;
import me.dinowernli.grpc.prometheus.MonitoringServerInterceptor;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

@SuppressWarnings("deprecation")
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
  private final Instance instance;
  private final HealthStatusManager healthStatusManager;
  private final Server server;
  private boolean stopping = false;

  public BuildFarmServer(String session, BuildFarmServerConfig config)
      throws InterruptedException, ConfigurationException {
    this(session, ServerBuilder.forPort(config.getPort()), config);
  }

  public BuildFarmServer(
      String session, ServerBuilder<?> serverBuilder, BuildFarmServerConfig config)
      throws InterruptedException, ConfigurationException {
    super("BuildFarmServer");

    instance = BuildFarmInstances.createInstance(session, config.getInstance(), this::stop);

    healthStatusManager = new HealthStatusManager();

    ServerInterceptor headersInterceptor = new ServerHeadersInterceptor();
    if (!config.getSslCertificatePath().equals("")) {
      // There are different Public Key Cryptography Standards (PKCS) that users may format their
      // certificate files in.  By default, the JDK cannot parse all of them.  In particular, it
      // cannot parse PKCS #1 (RSA Cryptography Standard).  When enabling TLS for GRPC, java's
      // underlying Security module is used. To improve the robustness of this parsing and the
      // overall accepted certificate formats, we add an additional security provider. BouncyCastle
      // is a library that will parse additional formats and allow users to provide certificates in
      // an otherwise unsupported format.
      Security.addProvider(new BouncyCastleProvider());
      File ssl_certificate_path = new File(config.getSslCertificatePath());
      serverBuilder.useTransportSecurity(ssl_certificate_path, ssl_certificate_path);
    }

    serverBuilder
        .addService(healthStatusManager.getHealthService())
        .addService(new ActionCacheService(instance, config.getAcPolicy()))
        .addService(new CapabilitiesService(instance))
        .addService(
            new ContentAddressableStorageService(
                instance,
                /* deadlineAfter=*/ config.getCasWriteTimeout().getSeconds(),
                TimeUnit.SECONDS
                /* requestLogLevel=*/ ))
        .addService(
            new ByteStreamService(
                instance,
                /* writeDeadlineAfter=*/ config.getBytestreamTimeout().getSeconds(),
                TimeUnit.SECONDS))
        .addService(
            new ExecutionService(
                instance,
                config.getExecuteKeepaliveAfterSeconds(),
                TimeUnit.SECONDS,
                keepaliveScheduler,
                getMetricsPublisher(config.getMetricsConfig())))
        .addService(new OperationQueueService(instance))
        .addService(new OperationsService(instance))
        .addService(new AdminService(config.getAdminConfig(), instance))
        .addService(new FetchService(instance))
        .addService(ProtoReflectionService.newInstance())
        .addService(new PublishBuildEventService(config.getBuildEventConfig()))
        .intercept(TransmitStatusRuntimeExceptionInterceptor.instance())
        .intercept(headersInterceptor);
    handleGrpcMetricIntercepts(serverBuilder, config);
    server = serverBuilder.build();

    logger.log(Level.INFO, String.format("%s initialized", session));
  }

  public static void handleGrpcMetricIntercepts(
      ServerBuilder<?> serverBuilder, BuildFarmServerConfig config) {
    // Decide how to capture GRPC Prometheus metrics.
    // By default, we don't capture any.
    if (config.getGrpcMetrics().getEnabled()) {
      // Assume core metrics.
      // Core metrics include send/receive totals tagged with return codes.  No latencies.
      Configuration grpcConfig = Configuration.cheapMetricsOnly();

      // Enable latency buckets.
      if (config.getGrpcMetrics().getProvideLatencyHistograms()) {
        grpcConfig = grpcConfig.allMetrics();
      }

      // Apply config to create an interceptor and apply it to the GRPC server.
      MonitoringServerInterceptor monitoringInterceptor =
          MonitoringServerInterceptor.create(grpcConfig);
      serverBuilder.intercept(monitoringInterceptor);
    }
  }

  private static BuildFarmServerConfig toBuildFarmServerConfig(
      Readable input, ServerOptions options) throws IOException {
    BuildFarmServerConfig.Builder builder = BuildFarmServerConfig.newBuilder();
    TextFormat.merge(input, builder);
    ConfigAdjuster.adjust(builder, options);
    return builder.build();
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
    instance.start(publicName);
    server.start();
    healthStatusManager.setStatus(
        HealthStatusManager.SERVICE_NAME_ALL_SERVICES, ServingStatus.SERVING);
    PrometheusPublisher.startHttpServer(prometheusPort);
    healthCheckMetric.labels("start").inc();
  }

  @Override
  protected void onShutdown() {
    System.err.println("*** shutting down gRPC server since JVM is shutting down");
    stop();
    System.err.println("*** server shut down");
  }

  @SuppressWarnings("ConstantConditions")
  public void stop() {
    synchronized (this) {
      if (stopping) {
        return;
      }
      stopping = true;
    }
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
      logger.log(Level.WARNING, "could not shut down keepalive scheduler");
    }
  }

  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  private static void printUsage(OptionsParser parser) {
    logger.log(Level.INFO, "Usage: CONFIG_PATH");
    logger.log(
        Level.INFO,
        parser.describeOptions(Collections.emptyMap(), OptionsParser.HelpVerbosity.LONG));
  }

  /** returns success or failure */
  @SuppressWarnings("ConstantConditions")
  static boolean serverMain(String[] args) {
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
      return false;
    }

    Path configPath = Paths.get(residue.get(0));
    ServerOptions options = parser.getOptions(ServerOptions.class);

    String session = "buildfarm-server";
    if (!options.publicName.isEmpty()) {
      session += "-" + options.publicName;
    }
    session += "-" + UUID.randomUUID();
    BuildFarmServer server;
    try (InputStream configInputStream = Files.newInputStream(configPath)) {
      BuildFarmServerConfig config =
          toBuildFarmServerConfig(new InputStreamReader(configInputStream), options);
      server = new BuildFarmServer(session, config);
      configInputStream.close();
      server.start(options.publicName, config.getPrometheusConfig().getPort());
      server.blockUntilShutdown();
      server.stop();
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
    System.exit(serverMain(args) ? 0 : 1);
  }
}
