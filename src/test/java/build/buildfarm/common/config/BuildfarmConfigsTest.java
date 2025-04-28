package build.buildfarm.common.config;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BuildfarmConfigsTest {
  private Path tempDir;

  @Before
  public void setUp() throws IOException {
    tempDir = Files.createTempDirectory("buildfarm-test");
    BuildfarmConfigs configs = BuildfarmConfigs.getInstance();
    configs.setServer(new Server());
    configs.setBackplane(new Backplane());
  }

  @After
  public void tearDown() throws IOException {
    Files.walk(tempDir)
        .sorted((a, b) -> -a.compareTo(b))
        .forEach(
            path -> {
              try {
                Files.delete(path);
              } catch (IOException e) {
              }
            });
  }

  @Test
  public void loadConfigs_withRelativePathNoParent_shouldNotThrowNPE() throws IOException {
    Path configFile = tempDir.resolve("server.yaml");
    String yamlContent = "server:\n  port: 8980\n";
    Files.write(configFile, yamlContent.getBytes());

    BuildfarmConfigs configs = BuildfarmConfigs.loadConfigs(configFile);
    assertNotNull(configs);
  }

  @Test
  public void loadConfigs_withNonExistentFile_shouldThrowException() {
    Path nonExistentFile = tempDir.resolve("nonexistent.yaml");
    assertThrows(NoSuchFileException.class, () -> BuildfarmConfigs.loadConfigs(nonExistentFile));
  }

  @Test
  public void loadConfigs_withInvalidYaml_shouldThrowException() throws IOException {
    Path configFile = tempDir.resolve("invalid.yaml");
    String invalidYaml = "invalid: yaml: content";
    Files.write(configFile, invalidYaml.getBytes());

    assertThrows(RuntimeException.class, () -> BuildfarmConfigs.loadConfigs(configFile));
  }

  @Test
  public void loadConfigs_withValidYaml_shouldLoadSuccessfully() throws IOException {
    Path configFile = tempDir.resolve("valid.yaml");
    String validYaml = "server:\n  port: 8980\nbackplane:\n  redisUri: redis://localhost:6379";
    Files.write(configFile, validYaml.getBytes());

    BuildfarmConfigs configs = BuildfarmConfigs.loadConfigs(configFile);
    assertNotNull(configs);
    assertNotNull(configs.getServer());
    assertNotNull(configs.getBackplane());
  }
}
