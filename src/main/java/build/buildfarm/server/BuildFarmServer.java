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
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.common.config.ServerOptions;
import build.buildfarm.common.grpc.TracingMetadataUtils.ServerHeadersInterceptor;
import build.buildfarm.instance.Instance;
import build.buildfarm.metrics.prometheus.PrometheusPublisher;
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
  private final io.grpc.Server server;
  private boolean stopping = false;

  public BuildFarmServer(String session)
      throws InterruptedException, ConfigurationException, IOException {
    this(session, ServerBuilder.forPort(BuildfarmConfigs.getInstance().getServer().getPort()));
  }

  public BuildFarmServer(String session, ServerBuilder<?> serverBuilder)
      throws InterruptedException, ConfigurationException {
    super("BuildFarmServer");

    instance = BuildFarmInstances.createInstance(session, this::stop);

    healthStatusManager = new HealthStatusManager();

    ServerInterceptor headersInterceptor = new ServerHeadersInterceptor();
    if (BuildfarmConfigs.getInstance().getServer().getSslCertificatePath() != null) {
      // There are different Public Key Cryptography Standards (PKCS) that users may format their
      // certificate files in.  By default, the JDK cannot parse all of them.  In particular, it
      // cannot parse PKCS #1 (RSA Cryptography Standard).  When enabling TLS for GRPC, java's
      // underlying Security module is used. To improve the robustness of this parsing and the
      // overall accepted certificate formats, we add an additional security provider. BouncyCastle
      // is a library that will parse additional formats and allow users to provide certificates in
      // an otherwise unsupported format.
      Security.addProvider(new BouncyCastleProvider());
      File ssl_certificate_path =
          new File(BuildfarmConfigs.getInstance().getServer().getSslCertificatePath());
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

    logger.log(Level.INFO, String.format("%s initialized", session));
  }

  public static void handleGrpcMetricIntercepts(ServerBuilder<?> serverBuilder) {
    // Decide how to capture GRPC Prometheus metrics.
    // By default, we don't capture any.
    if (BuildfarmConfigs.getInstance().getServer().getGrpcMetrics().isEnabled()) {
      // Assume core metrics.
      // Core metrics include send/receive totals tagged with return codes.  No latencies.
      Configuration grpcConfig = Configuration.cheapMetricsOnly();

      // Enable latency buckets.
      if (BuildfarmConfigs.getInstance()
          .getServer()
          .getGrpcMetrics()
          .isProvideLatencyHistograms()) {
        grpcConfig = grpcConfig.allMetrics();
      }

      // Apply config to create an interceptor and apply it to the GRPC server.
      MonitoringServerInterceptor monitoringInterceptor =
          MonitoringServerInterceptor.create(grpcConfig);
      serverBuilder.intercept(monitoringInterceptor);
    }
  }

  public synchronized void start(String publicName) throws IOException {
    checkState(!stopping, "must not call start after stop");
    instance.start(publicName);
    server.start();
    healthStatusManager.setStatus(
        HealthStatusManager.SERVICE_NAME_ALL_SERVICES, ServingStatus.SERVING);
    PrometheusPublisher.startHttpServer(
        BuildfarmConfigs.getInstance().getServer().getPrometheusPort());
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

    ServerOptions options = parser.getOptions(ServerOptions.class);

    try {
      BuildfarmConfigs.loadConfigs(residue.get(0));
    } catch (ConfigurationException e) {
      logger.severe("Could not parse yml configuration file." + e);
    }

    String session = "buildfarm-server";
    if (!options.publicName.isEmpty()) {
      session += "-" + options.publicName;
      BuildfarmConfigs.getInstance().getServer().setPublicName(options.publicName);
    }
    if (options.port > 0) {
      BuildfarmConfigs.getInstance().getServer().setPort(options.port);
    }
    session += "-" + UUID.randomUUID();
    BuildFarmServer server;
    try {
      server = new BuildFarmServer(session);
      server.start(options.publicName);
      server.blockUntilShutdown();
      server.stop();
      return true;
    } catch (IOException e) {
      System.err.println("error: " + formatIOError(e));
    } catch (InterruptedException e) {
      System.err.println("error: interrupted");
    } catch (ConfigurationException e) {
      throw new RuntimeException(e);
    }
    return false;
  }

  public static void main(String[] args) {
    System.exit(serverMain(args) ? 0 : 1);
  }
}
