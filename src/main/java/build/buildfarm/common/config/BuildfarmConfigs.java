package build.buildfarm.common.config;

import build.buildfarm.common.DigestUtil;
import com.google.common.base.Strings;
import com.google.devtools.common.options.OptionsParser;
import com.google.devtools.common.options.OptionsParsingException;
import java.io.IOException;
import java.io.InputStream;
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
      buildfarmConfigs = BuildfarmConfigs.loadConfigs(getConfigurationPath(parser));
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
      buildfarmConfigs = BuildfarmConfigs.loadConfigs(getConfigurationPath(parser));
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
}
