package build.buildfarm.common.config;

import build.buildfarm.common.DigestUtil;
import com.google.common.base.Strings;
import com.google.devtools.common.options.OptionsParser;
import com.google.devtools.common.options.OptionsParsingException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import javax.naming.ConfigurationException;
import lombok.Data;
import lombok.extern.java.Log;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

@Data
@Log
public final class BuildfarmConfigs {
  private static BuildfarmConfigs buildfarmConfigs;

  private static final long DEFAULT_CAS_SIZE = 2147483648L; // 2 * 1024 * 1024 * 1024

  private DigestUtil.HashFunction digestFunction = DigestUtil.HashFunction.SHA256;
  private long defaultActionTimeout = 600;
  private long maximumActionTimeout = 3600;
  private long maxEntrySizeBytes = 2147483648L; // 2 * 1024 * 1024 * 1024
  private int prometheusPort = 9090;
  private Server server = new Server();
  private Backplane backplane = new Backplane();
  private Worker worker = new Worker();
  private ExecutionWrappers executionWrappers = new ExecutionWrappers();

  private BuildfarmConfigs() {}

  public static BuildfarmConfigs getInstance() {
    if (buildfarmConfigs == null) {
      buildfarmConfigs = new BuildfarmConfigs();
    }
    return buildfarmConfigs;
  }

  public static BuildfarmConfigs loadConfigs(Path configLocation) throws IOException {
    try (InputStream inputStream = Files.newInputStream(configLocation)) {
      Yaml yaml = new Yaml(new Constructor(buildfarmConfigs.getClass()));
      buildfarmConfigs = yaml.load(inputStream);
      log.info(buildfarmConfigs.toString());
      return buildfarmConfigs;
    }
  }

  public static BuildfarmConfigs loadServerConfigs(String[] args) throws ConfigurationException {
    OptionsParser parser = getOptionsParser(ServerOptions.class, args);
    ServerOptions options = parser.getOptions(ServerOptions.class);
    try {
      buildfarmConfigs = loadConfigs(getConfigurationPath(parser));
      adjustServerConfigs(buildfarmConfigs);
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
      buildfarmConfigs = loadConfigs(getConfigurationPath(parser));
      adjustWorkerConfigs(buildfarmConfigs);
    } catch (IOException e) {
      log.severe("Could not parse yml configuration file." + e);
      throw new RuntimeException(e);
    }
    if (!Strings.isNullOrEmpty(options.publicName)) {
      buildfarmConfigs.getWorker().setPublicName(options.publicName);
    }
    return buildfarmConfigs;
  }

  private static OptionsParser getOptionsParser(Class clazz, String[] args)
      throws ConfigurationException {
    OptionsParser parser = OptionsParser.newOptionsParser(clazz);
    try {
      parser.parse(args);
    } catch (OptionsParsingException e) {
      log.severe("Could not parse options provided." + e);
      throw new RuntimeException(e);
    }

    return parser;
  }

  private static Path getConfigurationPath(OptionsParser parser) throws ConfigurationException {
    // source config from env variable
    if (!Strings.isNullOrEmpty(System.getenv("CONFIG_PATH"))) {
      return Paths.get(System.getenv("CONFIG_PATH"));
    }

    // source config from cli
    List<String> residue = parser.getResidue();
    if (residue.isEmpty()) {
      log.info("Usage: CONFIG_PATH");
      log.info(parser.describeOptions(Collections.emptyMap(), OptionsParser.HelpVerbosity.LONG));
      throw new ConfigurationException("A valid path to a configuration file must be provided.");
    }

    return Paths.get(residue.get(0));
  }

  private static void adjustServerConfigs(BuildfarmConfigs configs) {
    adjustPublicName(configs.getServer().getPublicName(), configs.getServer().getPort());
    adjustRedisUri(configs);
  }

  private static void adjustWorkerConfigs(BuildfarmConfigs configs) {
    adjustPublicName(configs.getWorker().getPublicName(), configs.getWorker().getPort());
    adjustRedisUri(configs);

    // Automatically set disk space to 90% of available space on the worker volume.
    // User configured value in .yaml will always take presedence.
    for (Cas storage : configs.getWorker().getStorages()) {
      deriveCasStorage(storage);
    }

    adjustExecuteStageWidth(configs);
    adjustInputFetchStageWidth(configs);
  }

  private static void adjustExecuteStageWidth(BuildfarmConfigs configs) {
    if (!Strings.isNullOrEmpty(System.getenv("EXECUTION_STAGE_WIDTH"))) {
      configs
          .getWorker()
          .setExecuteStageWidth(Integer.parseInt(System.getenv("EXECUTION_STAGE_WIDTH")));
      log.info(
          String.format(
              "executeStageWidth overwritten to %d", configs.getWorker().getExecuteStageWidth()));
      return;
    }

    if (configs.getWorker().getExecuteStageWidth() == 0) {
      configs
          .getWorker()
          .setExecuteStageWidth(
              Math.max(
                  1,
                  Runtime.getRuntime().availableProcessors()
                      - configs.getWorker().getExecuteStageWidthOffset()));
      log.info(
          String.format(
              "executeStageWidth modified to %d", configs.getWorker().getExecuteStageWidth()));
    }
  }

  private static void adjustInputFetchStageWidth(BuildfarmConfigs configs) {
    if (configs.getWorker().getInputFetchStageWidth() == 0) {
      configs
          .getWorker()
          .setInputFetchStageWidth(Math.max(1, configs.getWorker().getExecuteStageWidth() / 5));
      log.info(
          String.format(
              "executeInputFetchWidth modified to %d",
              configs.getWorker().getInputFetchStageWidth()));
    }
  }

  private static void adjustPublicName(String publicName, int port) {
    // use configured value
    if (!Strings.isNullOrEmpty(publicName)) {
      return;
    }

    // use environment override (useful for containerized deployment)
    if (!Strings.isNullOrEmpty(System.getenv("INSTANCE_NAME"))) {
      publicName = System.getenv("INSTANCE_NAME");
      log.info(String.format("publicName overwritten to %s", publicName));
      return;
    }

    // derive a value
    if (Strings.isNullOrEmpty(publicName)) {
      try {
        publicName = InetAddress.getLocalHost().getHostAddress() + ":" + port;
        log.info(String.format("publicName derived to %s", publicName));
      } catch (Exception e) {
        log.severe("publicName could not be derived:" + e);
      }
    }
  }

  private static void adjustRedisUri(BuildfarmConfigs configs) {
    // use environment override (useful for containerized deployment)
    if (!Strings.isNullOrEmpty(System.getenv("REDIS_URI"))) {
      configs.getBackplane().setRedisUri(System.getenv("REDIS_URI"));
      log.info(String.format("RedisUri modified to %s", configs.getBackplane().getRedisUri()));
    }
  }

  private static void deriveCasStorage(Cas storage) {
    if (storage.getMaxSizeBytes() == 0) {
      try {
        storage.setMaxSizeBytes(
            (long)
                (BuildfarmConfigs.getInstance().getWorker().getValidRoot().toFile().getTotalSpace()
                    * 0.9));
      } catch (Exception e) {
        storage.setMaxSizeBytes(DEFAULT_CAS_SIZE);
      }
      log.info(String.format("CAS size changed to %d", storage.getMaxSizeBytes()));
    }
  }
}
