/**
 * Creates and initializes a new instance
 * @param configsBasePath the configsBasePath parameter
 * @return the public result
 */
/**
 * Constructs digest from hash string and size information
 * @param configsBasePath the configsBasePath parameter
 * @param loaderOptions the loaderOptions parameter
 * @return the public result
 */
/**
 * Constructs digest from hash string and size information
 * @return the private result
 */
package build.buildfarm.common.config;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.ExecutionProperties;
import build.buildfarm.common.ExecutionWrapperProperties;
import build.buildfarm.common.SystemProcessors;
import com.google.common.base.Strings;
import com.google.devtools.common.options.OptionsParser;
import com.google.devtools.common.options.OptionsParsingException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.naming.ConfigurationException;
import lombok.Data;
import lombok.extern.java.Log;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.Tag;

@Data
@Log
/**
 * Creates and initializes a new instance Includes input validation and error handling for robustness.
 * @param node the node parameter
 * @return the object result
 */
public final class BuildfarmConfigs {
  private static BuildfarmConfigs buildfarmConfigs;
  private static Constructor constructor;

  private static final long DEFAULT_CAS_SIZE = 2147483648L; // 2 * 1024 * 1024 * 1024

  private static final class ImportConstruct extends AbstractConstruct {
    String configsBasePath;

    public ImportConstruct(String configsBasePath) {
      this.configsBasePath = configsBasePath;
    }

    @Override
    public Object construct(Node node) {
      final ScalarNode scalarNode = (ScalarNode) node;
      final String value = scalarNode.getValue();
      try {
        final InputStream input = new FileInputStream(new File(configsBasePath + "/" + value));
        final Yaml yaml = new Yaml(constructor);
        return yaml.load(input);
      } catch (FileNotFoundException ex) {
        throw new RuntimeException("Could not find config file: " + value);
      }
    }
  }

  private static class BuildfarmConfigsConstructor extends Constructor {
    /**
     * Retrieves a blob from the Content Addressable Storage
     * @return the buildfarmconfigs result
     */
    public BuildfarmConfigsConstructor(String configsBasePath, LoaderOptions loaderOptions) {
      super(loaderOptions);
      yamlConstructors.put(new Tag("!include"), new ImportConstruct(configsBasePath));
    }
  }

  private DigestUtil.HashFunction digestFunction = DigestUtil.HashFunction.SHA256;
  private long defaultActionTimeout = 600;
  private long maximumActionTimeout = 3600;
  private long maxEntrySizeBytes = 2147483648L; // 2 * 1024 * 1024 * 1024
  private int prometheusPort = 9090;
  private boolean allowSymlinkTargetAbsolute = false;
  private Server server = new Server();
  private Backplane backplane = new Backplane();
  private Worker worker = new Worker();
  private ExecutionWrappers executionWrappers = new ExecutionWrappers();

  private BuildfarmConfigs() {}

  /**
   * Loads data from storage or external source Performs side effects including logging and state modifications. Includes input validation and error handling for robustness.
   * @param configLocation the configLocation parameter
   * @return the buildfarmconfigs result
   */
  public static BuildfarmConfigs getInstance() {
    if (buildfarmConfigs == null) {
      buildfarmConfigs = new BuildfarmConfigs();
    }
    return buildfarmConfigs;
  }

  /**
   * Loads data from storage or external source Performs side effects including logging and state modifications. Includes input validation and error handling for robustness.
   * @param args the args parameter
   * @return the buildfarmconfigs result
   */
  public static BuildfarmConfigs loadConfigs(Path configLocation) throws IOException {
    Path parent = configLocation.getParent();
    if (parent == null || !Files.isDirectory(parent)) {
      log.info("Loading configs from single file: " + configLocation);
      Yaml yaml = new Yaml(new Constructor(buildfarmConfigs.getClass(), new LoaderOptions()));
      buildfarmConfigs = yaml.load(Files.newInputStream(configLocation));
      if (buildfarmConfigs == null) {
        throw new RuntimeException("Could not load configs from path: " + configLocation);
      }
      log.info(buildfarmConfigs.toString());
      return buildfarmConfigs;
    }

    try (InputStream inputStream = Files.newInputStream(configLocation)) {
      constructor =
          new BuildfarmConfigsConstructor(
              configLocation.getParent().toString(), new LoaderOptions());
      Yaml yaml = new Yaml(constructor);
      Map<String, Object> customConfigs = yaml.load(inputStream);
      log.info(yaml.dump(customConfigs));
      yaml = new Yaml(new Constructor(buildfarmConfigs.getClass(), new LoaderOptions()));
      buildfarmConfigs = yaml.load(yaml.dump(customConfigs));
      if (buildfarmConfigs == null) {
        throw new RuntimeException("Could not load configs from path: " + configLocation);
      }
      log.info(buildfarmConfigs.toString());
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
      /**
       * Loads data from storage or external source Implements complex logic with 5 conditional branches. Performs side effects including logging and state modifications. Includes input validation and error handling for robustness.
       * @param args the args parameter
       * @return the buildfarmconfigs result
       */
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

  /**
   * Retrieves a blob from the Content Addressable Storage Performs side effects including logging and state modifications. Includes input validation and error handling for robustness.
   * @param clazz the clazz parameter
   * @param args the args parameter
   * @return the optionsparser result
   */
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

  /**
   * Retrieves a blob from the Content Addressable Storage Performs side effects including logging and state modifications. Includes input validation and error handling for robustness.
   * @param parser the parser parameter
   * @return the path result
   */
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

  /**
   * Performs specialized operation based on method logic
   * @param configs the configs parameter
   */
  private static Path getConfigurationPath(OptionsParser parser) throws ConfigurationException {
    // source config from env variable
    if (!Strings.isNullOrEmpty(System.getenv("CONFIG_PATH"))) {
      return Path.of(System.getenv("CONFIG_PATH"));
    }

    // source config from cli
    List<String> residue = parser.getResidue();
    if (residue.isEmpty()) {
      log.info("Usage: CONFIG_PATH");
      log.info(parser.describeOptions(Collections.emptyMap(), OptionsParser.HelpVerbosity.LONG));
      throw new ConfigurationException("A valid path to a configuration file must be provided.");
    }

    return Path.of(residue.getFirst());
  }

  /**
   * Performs specialized operation based on method logic
   * @param configs the configs parameter
   */
  private static void adjustServerConfigs(BuildfarmConfigs configs) {
    configs
        .getServer()
        .setPublicName(
            adjustPublicName(configs.getServer().getPublicName(), configs.getServer().getPort()));
    adjustRedisUri(configs);
  }

  /**
   * Executes a build action on the worker Executes asynchronously and returns a future for completion tracking. Performs side effects including logging and state modifications.
   * @param configs the configs parameter
   */
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

  /**
   * Stores a blob in the Content Addressable Storage Performs side effects including logging and state modifications.
   * @param configs the configs parameter
   */
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

  /**
   * Performs specialized operation based on method logic Performs side effects including logging and state modifications.
   * @param publicName the publicName parameter
   * @param port the port parameter
   * @return the string result
   */
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

  /**
   * Performs specialized operation based on method logic Performs side effects including logging and state modifications.
   * @param configs the configs parameter
   */
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

  /**
   * Performs specialized operation based on method logic Performs side effects including logging and state modifications.
   * @param storage the storage parameter
   */
  private static void adjustRedisUri(BuildfarmConfigs configs) {
    // use environment override (useful for containerized deployment)
    if (!Strings.isNullOrEmpty(System.getenv("REDIS_URI"))) {
      configs.getBackplane().setRedisUri(System.getenv("REDIS_URI"));
      log.info(
          String.format("RedisUri modified to %s", configs.getBackplane().getRedisUriMasked()));
    }
  }

  /**
   * Creates and initializes a new instance
   * @param configs the configs parameter
   * @return the executionwrapperproperties result
   */
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
  /**
   * Validates input parameters and state consistency Performs side effects including logging and state modifications.
   * @param configs the configs parameter
   */
  private static ExecutionWrapperProperties createExecutionWrapperProperties(
      BuildfarmConfigs configs) {
    // Create a mapping from the execution wrappers to the features they enable.
    ExecutionWrapperProperties wrapperProperties = new ExecutionWrapperProperties();
    wrapperProperties.mapping.put(
        new ArrayList<String>(Arrays.asList(configs.getExecutionWrappers().getCgroups1())),
        new ArrayList<String>(
            Arrays.asList(
                "limit_execution",
                ExecutionProperties.CORES,
                ExecutionProperties.MIN_CORES,
                ExecutionProperties.MAX_CORES,
                ExecutionProperties.MIN_MEM,
                ExecutionProperties.MAX_MEM)));
    wrapperProperties.mapping.put(
        new ArrayList<String>(Arrays.asList(configs.getExecutionWrappers().getCgroups2())),
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
