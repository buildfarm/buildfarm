package build.buildfarm.common.config;

import build.buildfarm.common.DigestUtil;
import com.google.common.base.Strings;
import com.google.devtools.common.options.OptionsParser;
import com.google.devtools.common.options.OptionsParsingException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import javax.naming.ConfigurationException;

import io.grpc.ServerBuilder;
import lombok.Data;
import lombok.extern.java.Log;
import me.dinowernli.grpc.prometheus.Configuration;
import me.dinowernli.grpc.prometheus.MonitoringServerInterceptor;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

@Data
@Log
public final class BuildfarmConfigs {
  private static BuildfarmConfigs buildfarmConfigs;

  private DigestUtil.HashFunction digestFunction = DigestUtil.HashFunction.SHA256;
  private long defaultActionTimeout = 600;
  private long maximumActionTimeout = 3600;
  private int prometheusPort = 9090;
  private Server server = new Server();
  private Backplane backplane = new Backplane();
  private Worker worker = new Worker();

  private BuildfarmConfigs() {}

  public static BuildfarmConfigs getInstance() {
    if (buildfarmConfigs == null) {
      buildfarmConfigs = new BuildfarmConfigs();
    }
    return buildfarmConfigs;
  }

  public static BuildfarmConfigs loadConfigs(String configLocation) throws IOException {
    try (InputStream inputStream = new FileInputStream(new File(configLocation))) {
      Yaml yaml = new Yaml(new Constructor(buildfarmConfigs.getClass()));
      buildfarmConfigs = yaml.load(inputStream);
      log.info(buildfarmConfigs.toString());
    }
    return buildfarmConfigs;
  }

  public static void handleGrpcMetricIntercepts(
      ServerBuilder<?> serverBuilder, GrpcMetrics grpcMetrics) {

    // Decide how to capture GRPC Prometheus metrics.
    // By default, we don't capture any.
    if (grpcMetrics.isEnabled()) {
      // Assume core metrics.
      // Core metrics include send/receive totals tagged with return codes.  No latencies.
      Configuration grpcConfig = Configuration.cheapMetricsOnly();

      // Enable latency buckets.
      if (grpcMetrics.isProvideLatencyHistograms()) {
        grpcConfig = grpcConfig.allMetrics();
      }

      // Apply config to create an interceptor and apply it to the GRPC server.
      MonitoringServerInterceptor monitoringInterceptor =
          MonitoringServerInterceptor.create(grpcConfig);
      serverBuilder.intercept(monitoringInterceptor);
    }
  }

  public void loadConfigs(Path configLocation) throws IOException {
    try (InputStream inputStream = Files.newInputStream(configLocation)) {
      Yaml yaml = new Yaml(new Constructor(buildfarmConfigs.getClass()));
      buildfarmConfigs = yaml.load(inputStream);
      log.info(buildfarmConfigs.toString());
    }
  }

  private static OptionsParser getOptionsParser(Class clazz, String[] args)
      throws ConfigurationException {
    verifyArgs(args);
    OptionsParser parser = OptionsParser.newOptionsParser(clazz);
    try {
      parser.parse(args);
    } catch (OptionsParsingException e) {
      log.severe("Could not parse options provided." + e);
      throw new RuntimeException(e);
    }
    List<String> residue = parser.getResidue();
    if (residue.isEmpty()) {
      log.info("Usage: CONFIG_PATH");
      log.info(parser.describeOptions(Collections.emptyMap(), OptionsParser.HelpVerbosity.LONG));
    }
    return parser;
  }

  private static void verifyArgs(String[] args) throws ConfigurationException {
    if (args.length == 0) {
      throw new ConfigurationException("A valid path to a configuration file must be provided.");
    }
  }

  public static BuildfarmConfigs loadServerConfigs(String[] args) throws ConfigurationException {
    OptionsParser parser = getOptionsParser(ServerOptions.class, args);
    ServerOptions options = parser.getOptions(ServerOptions.class);
    try {
      buildfarmConfigs = BuildfarmConfigs.loadConfigs(parser.getResidue().get(0));
    } catch (IOException e) {
      log.severe("Could not parse yml configuration file." + e);
      throw new RuntimeException(e);
    }
    if (!options.publicName.isEmpty()) {
      buildfarmConfigs.getServer().setPublicName(options.publicName);
    }
    if (options.port > 0) {
      buildfarmConfigs.getServer().setPort(options.port);
    }
    return buildfarmConfigs;
  }

  public static BuildfarmConfigs loadWorkerConfigs(String[] args) throws ConfigurationException {
    OptionsParser parser = getOptionsParser(ShardWorkerOptions.class, args);
    ShardWorkerOptions options = parser.getOptions(ShardWorkerOptions.class);
    try {
      buildfarmConfigs = BuildfarmConfigs.loadConfigs(parser.getResidue().get(0));
    } catch (IOException e) {
      log.severe("Could not parse yml configuration file." + e);
      throw new RuntimeException(e);
    }
    if (!Strings.isNullOrEmpty(options.publicName)) {
      buildfarmConfigs.getWorker().setPublicName(options.publicName);
    }
    return buildfarmConfigs;
  }
}
