package build.buildfarm.common.config;

import build.buildfarm.common.DigestUtil;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.Data;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

@Data
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
    }
    return buildfarmConfigs;
  }

  public void loadConfigs(Path configLocation) throws IOException {
    try (InputStream inputStream = Files.newInputStream(configLocation)) {
      Yaml yaml = new Yaml(new Constructor(buildfarmConfigs.getClass()));
      buildfarmConfigs = yaml.load(inputStream);
    }
  }
}
