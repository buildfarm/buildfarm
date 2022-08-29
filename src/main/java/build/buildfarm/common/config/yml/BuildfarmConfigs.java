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

    private static String digestFunction = "SHA256";

    private static long defaultActionTimeout = 600;

    private static long maximumActionTimeout = 3600;
    private static Server server;

    private static Backplane backplane;

    private static Worker worker;

    private BuildfarmConfigs() {}

    public static BuildfarmConfigs getInstance() {
        if(buildfarmConfigs == null) {
            buildfarmConfigs = new BuildfarmConfigs();
        }
        return buildfarmConfigs;
    }

    public void loadConfigs(String configLocation) throws IOException {
        try (InputStream inputStream = new FileInputStream(new File(configLocation))) {
            Yaml yaml = new Yaml(new Constructor(buildfarmConfigs.getClass()));
            buildfarmConfigs = yaml.load(inputStream);
            logger.info(buildfarmConfigs.toString());
        }
    }

    public BuildfarmConfigs getBuildfarmConfigs() {
        return buildfarmConfigs;
    }

    public void setBuildfarmConfigs(BuildfarmConfigs buildfarmConfigs) {
        this.buildfarmConfigs = buildfarmConfigs;
    }

    public String getDigestFunction() {
        return digestFunction;
    }

    public void setDigestFunction(String digestFunction) {
        this.digestFunction = digestFunction;
    }

    public long getDefaultActionTimeout() {
        return defaultActionTimeout;
    }

    public void setDefaultActionTimeout(long defaultActionTimeout) {
        BuildfarmConfigs.defaultActionTimeout = defaultActionTimeout;
    }

    public long getMaximumActionTimeout() {
        return maximumActionTimeout;
    }

    public void setMaximumActionTimeout(long maximumActionTimeout) {
        BuildfarmConfigs.maximumActionTimeout = maximumActionTimeout;
    }

    public Server getServer() {
        return server;
    }

    public void setServer(Server server) {
        this.server = server;
    }

    public Backplane getBackplane() {
        return backplane;
    }

    public void setBackplane(Backplane backplane) {
        this.backplane = backplane;
    }

    public Worker getWorker() {
        return worker;
    }

    public void setWorker(Worker worker) {
        this.worker = worker;
    }

    @Override
    public String toString() {
        return "BuildfarmConfigs{" +
                "digestFunction='" + digestFunction + '\'' +
                ", defaultActionTimeout=" + defaultActionTimeout +
                ", maximumActionTimeout=" + maximumActionTimeout +
                ", server=" + server +
                ", worker=" + worker +
                ", backplane=" + backplane +
                '}';
    }
}
