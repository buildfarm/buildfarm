package build.buildfarm.common.config;

import build.buildfarm.common.DigestUtil;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import javax.naming.ConfigurationException;
import lombok.Data;
import lombok.extern.java.Log;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

@Log
@Data
public final class BuildfarmConfigs {
  private static final String defaultConfigPath = "../build_buildfarm/examples/config.yml";
  private static final String alternateConfigPath = "/app/build_buildfarm/examples/config.yml";

  private static BuildfarmConfigs buildfarmConfigs;

  private DigestUtil.HashFunction digestFunction;
  private long defaultActionTimeout;
  private long maximumActionTimeout;
  private Server server;
  private Backplane backplane;
  private Worker worker;
  private Memory memory;

  private BuildfarmConfigs() {}

  public static BuildfarmConfigs getInstance() {
    return buildfarmConfigs;
  }

  public static void loadConfigs(String configLocation) throws ConfigurationException {
    try {
      Constructor yamlConstructor = new Constructor(BuildfarmConfigs.class);
      Yaml yaml = new Yaml(yamlConstructor);
      if (Files.exists(Paths.get(defaultConfigPath))) {
        log.info("Loaded default configuration from " + defaultConfigPath);
        buildfarmConfigs = yaml.load(Files.newInputStream(new File(defaultConfigPath).toPath()));
      } else {
        log.info("Loaded default configuration from " + alternateConfigPath);
        buildfarmConfigs = yaml.load(Files.newInputStream(new File(alternateConfigPath).toPath()));
      }
      if (configLocation != null) {
        buildfarmConfigs = yaml.load(Files.newInputStream(new File(configLocation).toPath()));
        log.info("Loaded user configuration from " + configLocation);
      }
      log.info(buildfarmConfigs.toString());
    } catch (Exception e) {
      log.severe("Could not load configuration file. " + e);
      throw new ConfigurationException(e.toString());
    }
  }

  public static void loadConfigs() throws ConfigurationException {
    loadConfigs(null);
  }
}
