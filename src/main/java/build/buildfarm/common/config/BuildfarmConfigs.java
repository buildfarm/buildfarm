package build.buildfarm.common.config;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.ExecutionProperties;
import build.buildfarm.common.ExecutionWrapperProperties;
import build.buildfarm.common.SystemProcessors;
import com.google.common.base.Strings;
import com.google.devtools.common.options.OptionsParser;
import com.google.devtools.common.options.OptionsParsingException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.naming.ConfigurationException;
import lombok.Data;
import lombok.extern.java.Log;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

@Data
@Log
public final class BuildfarmConfigs {
  private static BuildfarmConfigs buildfarmConfigs;

  private static final long DEFAULT_CAS_SIZE = 2147483648L; // 2 * 1024 * 1024 * 1024

  private DigestUtil.HashFunction digestFunction;
  private long defaultActionTimeout;
  private long maximumActionTimeout;
  private long maxEntrySizeBytes;
  private int prometheusPort;
  private boolean allowSymlinkTargetAbsolute;
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

  private static void mergeBaseAndCustomConfigs(
      Map<String, Object> baseConfigs, Map<String, Object> newConfigs) {
    newConfigs.forEach(
        (key, value) -> {
          if (value instanceof LinkedHashMap) {
            LinkedHashMap<String, Object> newConfigsValue = (LinkedHashMap<String, Object>) value;
            LinkedHashMap<String, Object> baseConfigsValue =
                (LinkedHashMap<String, Object>)
                    baseConfigs.getOrDefault(key, new LinkedHashMap<>());
            mergeBaseAndCustomConfigs(baseConfigsValue, newConfigsValue);
          } else {
            baseConfigs.put(key, value);
          }
        });
  }

  public static BuildfarmConfigs loadConfigs(String configLocations) throws IOException {
    try (InputStream baseInputStream = new FileInputStream("./examples/config.yml")) {
      Yaml yaml = new Yaml();
      // Load base config.yaml to initialize with default values
      Map<String, Object> baseConfigs = yaml.load(baseInputStream);
      // Load custom configs provided by the user to overwrite defaults
      List<String> customConfigFiles = Arrays.asList(configLocations.split(","));
      for (String customConfigFile : customConfigFiles) {
        try (InputStream customInputStream = new FileInputStream(customConfigFile)) {
          Map<String, Object> customConfigs = yaml.load(customInputStream);
          // Merge the base and custom configs
          mergeBaseAndCustomConfigs(baseConfigs, customConfigs);
          log.info("Merged base config with custom config file " + customConfigFile);
        } catch (IOException ioe) {
          log.severe("Could not parse yml configuration file." + ioe);
          throw new RuntimeException(ioe);
        }
      }
      // Load the merged configs into the buildfarmConfigs object
      yaml = new Yaml(new Constructor(buildfarmConfigs.getClass(), new LoaderOptions()));
      buildfarmConfigs = yaml.load(yaml.dump(baseConfigs));
      log.info(buildfarmConfigs.toString());
      log.info(yaml.dump(buildfarmConfigs));
      return buildfarmConfigs;
    }
  }

  public static BuildfarmConfigs loadServerConfigs(String[] args) throws ConfigurationException {
    OptionsParser parser = getOptionsParser(ServerOptions.class, args);
    ServerOptions options = parser.getOptions(ServerOptions.class);
    try {
      buildfarmConfigs = loadConfigs(getConfigurationPath(parser));
    } catch (IOException e) {
      log.severe("Could not parse yml configuration file." + e);
      throw new RuntimeException(e);
    }
    if (!Strings.isNullOrEmpty(options.publicName)) {
      buildfarmConfigs.getServer().setPublicName(options.publicName);
    }
    if (options.port > 0) {
      buildfarmConfigs.getServer().setPort(options.port);
    }
    if (options.prometheusPort >= 0) {
      buildfarmConfigs.setPrometheusPort(options.prometheusPort);
    }
    if (!Strings.isNullOrEmpty(options.redisUri)) {
      buildfarmConfigs.getBackplane().setRedisUri(options.redisUri);
    }
    adjustServerConfigs(buildfarmConfigs);
    return buildfarmConfigs;
  }

  public static BuildfarmConfigs loadWorkerConfigs(String[] args) throws ConfigurationException {
    OptionsParser parser = getOptionsParser(ShardWorkerOptions.class, args);
    ShardWorkerOptions options = parser.getOptions(ShardWorkerOptions.class);
    try {
      buildfarmConfigs = loadConfigs(getConfigurationPath(parser));
    } catch (IOException e) {
      log.severe("Could not parse yml configuration file." + e);
      throw new RuntimeException(e);
    }
    if (!Strings.isNullOrEmpty(options.publicName)) {
      buildfarmConfigs.getWorker().setPublicName(options.publicName);
    }
    if (options.port >= 0) {
      buildfarmConfigs.getWorker().setPort(options.port);
    }
    if (options.prometheusPort >= 0) {
      buildfarmConfigs.setPrometheusPort(options.prometheusPort);
    }
    if (!Strings.isNullOrEmpty(options.redisUri)) {
      buildfarmConfigs.getBackplane().setRedisUri(options.redisUri);
    }
    if (!Strings.isNullOrEmpty(options.root)) {
      buildfarmConfigs.getWorker().setRoot(options.root);
    }
    adjustWorkerConfigs(buildfarmConfigs);
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

  private static String getConfigurationPath(OptionsParser parser) throws ConfigurationException {
    // source config from env variable
    if (!Strings.isNullOrEmpty(System.getenv("CONFIG_PATH"))) {
      return System.getenv("CONFIG_PATH");
    }

    // source config from cli
    List<String> residue = parser.getResidue();
    if (residue.isEmpty()) {
      log.info("Usage: CONFIG_PATH");
      log.info(parser.describeOptions(Collections.emptyMap(), OptionsParser.HelpVerbosity.LONG));
      throw new ConfigurationException(
          "A valid comma separated path(s) to configuration file(s) must be provided.");
    }

    return residue.getFirst();
  }

  private static void adjustServerConfigs(BuildfarmConfigs configs) {
    configs
        .getServer()
        .setPublicName(
            adjustPublicName(configs.getServer().getPublicName(), configs.getServer().getPort()));
    adjustRedisUri(configs);
  }

  private static void adjustWorkerConfigs(BuildfarmConfigs configs) {
    configs
        .getWorker()
        .setPublicName(
            adjustPublicName(configs.getWorker().getPublicName(), configs.getWorker().getPort()));
    adjustRedisUri(configs);

    // Automatically set disk space to 90% of available space on the worker volume.
    // User configured value in .yaml will always take presedence.
    for (Cas storage : configs.getWorker().getStorages()) {
      deriveCasStorage(storage);
    }

    adjustExecuteStageWidth(configs);
    adjustInputFetchStageWidth(configs);

    checkExecutionWrapperAvailability(configs);
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
                  1, SystemProcessors.get() - configs.getWorker().getExecuteStageWidthOffset()));
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

  private static String adjustPublicName(String publicName, int port) {
    // use configured value
    if (!Strings.isNullOrEmpty(publicName)) {
      return publicName;
    }

    // use environment override (useful for containerized deployment)
    if (!Strings.isNullOrEmpty(System.getenv("INSTANCE_NAME"))) {
      publicName = System.getenv("INSTANCE_NAME");
      log.info(String.format("publicName overwritten to %s", publicName));
      return publicName;
    }

    // derive a value
    if (Strings.isNullOrEmpty(publicName)) {
      try {
        publicName = InetAddress.getLocalHost().getHostAddress() + ":" + port;
        log.info(String.format("publicName derived to %s", publicName));
        return publicName;
      } catch (Exception e) {
        log.severe("publicName could not be derived:" + e);
      }
    }

    return publicName;
  }

  private static void adjustRedisUri(BuildfarmConfigs configs) {
    // use environment override (useful for containerized deployment)
    if (!Strings.isNullOrEmpty(System.getenv("REDIS_URI"))) {
      configs.getBackplane().setRedisUri(System.getenv("REDIS_URI"));
      log.info(
          String.format("RedisUri modified to %s", configs.getBackplane().getRedisUriMasked()));
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

  @SuppressWarnings("PMD.ConfusingArgumentToVarargsMethod")
  private static ExecutionWrapperProperties createExecutionWrapperProperties(
      BuildfarmConfigs configs) {
    // Create a mapping from the execution wrappers to the features they enable.
    ExecutionWrapperProperties wrapperProperties = new ExecutionWrapperProperties();
    wrapperProperties.mapping.put(
        new ArrayList<String>(Arrays.asList(configs.getExecutionWrappers().getCgroups())),
        new ArrayList<String>(
            Arrays.asList(
                "limit_execution",
                ExecutionProperties.CORES,
                ExecutionProperties.MIN_CORES,
                ExecutionProperties.MAX_CORES,
                ExecutionProperties.MIN_MEM,
                ExecutionProperties.MAX_MEM)));

    wrapperProperties.mapping.put(
        new ArrayList<String>(Arrays.asList(configs.getExecutionWrappers().getLinuxSandbox())),
        new ArrayList<String>(
            Arrays.asList(
                ExecutionProperties.LINUX_SANDBOX,
                ExecutionProperties.BLOCK_NETWORK,
                ExecutionProperties.TMPFS)));

    wrapperProperties.mapping.put(
        new ArrayList<String>(Arrays.asList(configs.getExecutionWrappers().getAsNobody())),
        new ArrayList<String>(Arrays.asList(ExecutionProperties.AS_NOBODY)));

    wrapperProperties.mapping.put(
        new ArrayList<String>(Arrays.asList(configs.getExecutionWrappers().getProcessWrapper())),
        new ArrayList<String>(Arrays.asList(ExecutionProperties.PROCESS_WRAPPER)));

    wrapperProperties.mapping.put(
        new ArrayList<String>(
            Arrays.asList(
                configs.getExecutionWrappers().getSkipSleep(),
                configs.getExecutionWrappers().getSkipSleepPreload(),
                configs.getExecutionWrappers().getDelay())),
        new ArrayList<String>(
            Arrays.asList(ExecutionProperties.SKIP_SLEEP, ExecutionProperties.TIME_SHIFT)));

    return wrapperProperties;
  }

  private static void checkExecutionWrapperAvailability(BuildfarmConfigs configs) {
    ExecutionWrapperProperties wrapperProperties = createExecutionWrapperProperties(configs);

    // Find missing tools, and warn the user that missing tools mean missing features.
    wrapperProperties.mapping.forEach(
        (tools, features) ->
            tools.forEach(
                (tool) -> {
                  if (Files.notExists(Path.of(tool))) {
                    String message =
                        String.format(
                            "the execution wrapper %s is missing and therefore the following"
                                + " features will not be available: %s",
                            tool, String.join(", ", features));
                    log.warning(message);
                  }
                }));
  }
}
