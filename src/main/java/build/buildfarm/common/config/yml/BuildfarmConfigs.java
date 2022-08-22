package build.buildfarm.common.config.yml;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Logger;

public class BuildfarmConfigs {
    private static final Logger logger = Logger.getLogger(BuildfarmConfigs.class.getName());

    private static BuildfarmConfigs buildfarmConfigs;
    private static Server server;

    public static BuildfarmConfigs getInstance() {
        if(buildfarmConfigs == null) {
            buildfarmConfigs = new BuildfarmConfigs();
        }
        return buildfarmConfigs;
    }

    public void setServer(Server value) {
        this.server = value;
    }

    public Server getServer() {
        return this.server;
    }

    public void loadConfigs(String configLocation) throws IOException {
        try (InputStream inputStream = new FileInputStream(new File(configLocation))) {
            Yaml yaml = new Yaml(new Constructor(buildfarmConfigs.getClass()));
            buildfarmConfigs = yaml.load(inputStream);
            logger.info(buildfarmConfigs.toString());
        }
    }

    public String toString() {
        return "Effective Buildfarm Configs: \n" +
                server.toString() + "\n";
    }
}
