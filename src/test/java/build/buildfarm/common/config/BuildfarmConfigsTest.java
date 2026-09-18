package build.buildfarm.common.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import javax.naming.ConfigurationException;
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

  @Test
  public void validateCasStorageSizeConfig_bothSet_throws() {
    Cas cas = new Cas();
    cas.setMaxSizeBytes(1000);
    cas.setMaxSizePercent(50);
    // Both maxSizeBytes and maxSizePercent can't be non-zero simultaneously
    assertThrows(
        ConfigurationException.class, () -> BuildfarmConfigs.validateCasStorageSizeConfig(cas));
  }

  @Test
  public void validateCasStorageSizeConfig_bytesNegative_throws() {
    Cas cas = new Cas();
    cas.setMaxSizeBytes(-1);
    // maxSizeBytes must be non-negative
    assertThrows(
        ConfigurationException.class, () -> BuildfarmConfigs.validateCasStorageSizeConfig(cas));
  }

  @Test
  public void validateCasStorageSizeConfig_percentNegative_throws() {
    Cas cas = new Cas();
    cas.setMaxSizePercent(-1);
    // maxSizePercent must be positive
    assertThrows(
        ConfigurationException.class, () -> BuildfarmConfigs.validateCasStorageSizeConfig(cas));
  }

  @Test
  public void validateCasStorageSizeConfig_percentOver100_throws() {
    Cas cas = new Cas();
    cas.setMaxSizePercent(101);
    // maxSizePercent must be 100 or smaller
    assertThrows(
        ConfigurationException.class, () -> BuildfarmConfigs.validateCasStorageSizeConfig(cas));
  }

  private void setCasMaxSizePercentAndValidateSize(int maxSizePercent)
      throws ConfigurationException {
    Worker worker = new Worker();
    worker.setRoot(tempDir.toString());
    BuildfarmConfigs.getInstance().setWorker(worker);

    Cas cas = new Cas();
    cas.setMaxSizePercent(maxSizePercent);
    cas.setMaxSizeBytes(0);
    // When both maxSizePercent and maxSizeBytes are 0, maxSizePercent is preferred and set to
    // DEFAULT_MAX_SIZE_PERCENT
    if (maxSizePercent == 0) {
      maxSizePercent = BuildfarmConfigs.DEFAULT_MAX_SIZE_PERCENT;
    }
    BuildfarmConfigs.deriveCasStorage(cas);
    long expectedBytes = (long) (tempDir.toFile().getTotalSpace() * maxSizePercent / 100.0);
    assertEquals(expectedBytes, cas.getMaxSizeBytes());
  }

  @Test
  public void deriveCasStorage_validMaxSizePercent_computesCorrectBytes()
      throws ConfigurationException {
    // When valid maxSizePercent values are used, the CAS size is limited correctly
    setCasMaxSizePercentAndValidateSize(100);
    setCasMaxSizePercentAndValidateSize(1);
  }

  @Test
  public void deriveCasStorage_maxSizePercentAndMaxSizeBytesAt0_defaultsToDefaultMaxSizePercent()
      throws ConfigurationException {
    setCasMaxSizePercentAndValidateSize(0);
  }

  @Test
  public void deriveCasStorage_maxSizeBytesAlone_limitSetCorrectly() throws ConfigurationException {
    Cas cas = new Cas();
    cas.setMaxSizeBytes(1000);
    cas.setMaxSizePercent(0);
    BuildfarmConfigs.deriveCasStorage(cas);
    // When maxSizePercent is 0, maxSizeBytes is used and set correctly
    assertEquals(1000, cas.getMaxSizeBytes());
  }

  @Test
  public void loadConfigs_withMaxSizePercent_parsesCorrectly() throws IOException {
    Path configFile = tempDir.resolve("percent.yaml");
    String yamlContent = "worker:\n  storages:\n    - maxSizePercent: 75\n";
    Files.write(configFile, yamlContent.getBytes());

    BuildfarmConfigs configs = BuildfarmConfigs.loadConfigs(configFile);
    assertEquals(75, configs.getWorker().getStorages().get(0).getMaxSizePercent());
  }
}
