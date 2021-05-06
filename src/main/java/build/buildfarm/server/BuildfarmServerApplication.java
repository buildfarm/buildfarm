package build.buildfarm.server;

import build.buildfarm.v1test.BuildFarmServerConfig;
import com.google.devtools.common.options.OptionsParser;
import com.google.protobuf.TextFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import static build.buildfarm.common.io.Utils.formatIOError;

@SpringBootApplication
public class BuildfarmServerApplication {
    private static final Logger logger = Logger.getLogger(BuildfarmServerApplication.class.getName());
    @Autowired
    private static ApplicationArguments args;

    public static void main(String[] args) {
        SpringApplication.run(BuildFarmServer.class, args);
    }

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
        System.out.println("In commandLineRunner");
        return args -> {
            System.out.println("Beans provided by Spring Boot:");
            String[] beanNames = ctx.getBeanDefinitionNames();
            Arrays.sort(beanNames);
            for (String beanName : beanNames) {
                System.out.println(beanName);
            }
        };
    }

    @Bean
    public String session() {
        System.out.println("In session");
        String session = "buildfarm-server";
        if (!options().publicName.isEmpty()) {
            session += "-" + options().publicName;
        }
        session += "-" + UUID.randomUUID();
        return session;
    }

    @Bean
    public BuildFarmServerOptions options() {
        System.out.println("In options");
        OptionsParser parser = OptionsParser.newOptionsParser(BuildFarmServerOptions.class);
        parser.parseAndExitUponError(args.getSourceArgs());
        List<String> residue = parser.getResidue();
        if (residue.isEmpty()) {
            logger.log(Level.INFO, "Usage: CONFIG_PATH");
            logger.log(
                    Level.INFO,
                    parser.describeOptions(Collections.emptyMap(), OptionsParser.HelpVerbosity.LONG));
            System.exit(-1);
        }
        Path configPath = Paths.get(residue.get(0));
        BuildFarmServerOptions options = parser.getOptions(BuildFarmServerOptions.class);
        return options;
    }

    @Bean
    public Path configPath() {
        System.out.println("In configPath");
        OptionsParser parser = OptionsParser.newOptionsParser(BuildFarmServerOptions.class);
        parser.parseAndExitUponError(args.getSourceArgs());
        List<String> residue = parser.getResidue();
        if (residue.isEmpty()) {
            logger.log(Level.INFO, "Usage: CONFIG_PATH");
            logger.log(
                    Level.INFO,
                    parser.describeOptions(Collections.emptyMap(), OptionsParser.HelpVerbosity.LONG));
            System.exit(-1);
        }
        return Paths.get(residue.get(0));
    }

    @Bean
    public BuildFarmServerConfig config() {
        System.out.println("In config");
        BuildFarmServerConfig config = null;
        try (InputStream configInputStream = Files.newInputStream(configPath())) {
            BuildFarmServerConfig.Builder builder = BuildFarmServerConfig.newBuilder();
            TextFormat.merge(new InputStreamReader(configInputStream), builder);
            if (options().port > 0) {
                builder.setPort(options().port);
            }
            config = builder.build();
        } catch (IOException e) {
            System.err.println("error: " + formatIOError(e));
        }
        return config;
    }
}
