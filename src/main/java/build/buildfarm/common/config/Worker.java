package build.buildfarm.common.config;

import build.buildfarm.v1test.WorkerType;
import com.google.common.base.Strings;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import javax.naming.ConfigurationException;
import lombok.Data;
import lombok.extern.java.Log;

@Data
@Log
public class Worker {
  private int port = 8981;
  private GrpcMetrics grpcMetrics = new GrpcMetrics();
  private String publicName;
  private Capabilities capabilities = new Capabilities();
  private String root = "/tmp/worker";
  private int inlineContentLimit = 1048567; // 1024 * 1024
  private long operationPollPeriod = 1;
  private DequeueMatchSettings dequeueMatchSettings = new DequeueMatchSettings();
  private List<Cas> storages = Arrays.asList(new Cas());
  private int executeStageWidth = 0;
  private int executeStageWidthOffset = 0;
  private int inputFetchStageWidth = 0;
  private int inputFetchDeadline = 60;
  private boolean linkInputDirectories = true;
  private List<String> realInputDirectories = Arrays.asList("external");
  private String execOwner;
  private int defaultMaxCores = 0;
  private boolean limitGlobalExecution = false;
  private boolean onlyMulticoreTests = false;
  private boolean allowBringYourOwnContainer = false;
  private boolean errorOperationRemainingResources = false;
  private ExecutionPolicy[] executionPolicies = {};

  public String getPublicName() {
    // use environment override (useful for containerized deployment)
    if (!Strings.isNullOrEmpty(System.getenv("INSTANCE_NAME"))) {
      return System.getenv("INSTANCE_NAME");
    }

    // use configured value
    if (!Strings.isNullOrEmpty(publicName)) {
      return publicName;
    }

    // derive a value
    try {
      return InetAddress.getLocalHost().getHostAddress() + ":" + port;
    } catch (Exception e) {
      log.severe("publicName could not be derived:" + e);
      return publicName;
    }
  }

  public int getExecuteStageWidth() {
    if (executeStageWidth > 0) {
      return executeStageWidth;
    } else if (!Strings.isNullOrEmpty(System.getenv("EXECUTION_STAGE_WIDTH"))) {
      return Integer.parseInt(System.getenv("EXECUTION_STAGE_WIDTH"));
    } else {
      return Math.max(1, Runtime.getRuntime().availableProcessors() - executeStageWidthOffset);
    }
  }

  public int getInputFetchStageWidth() {
    if (inputFetchStageWidth > 0) {
      return inputFetchStageWidth;
    } else {
      return Math.max(1, getExecuteStageWidth() / 5);
    }
  }

  public ExecutionPolicy[] getExecutionPolicies() {
    if (executionPolicies != null) {
      return executionPolicies;
    } else {
      return new ExecutionPolicy[0];
    }
  }

  public WorkerType getWorkerType() {
    if (getCapabilities().isCas() && getCapabilities().isExecution()) {
      return WorkerType.EXECUTE_AND_STORAGE;
    }
    if (getCapabilities().isCas()) {
      return WorkerType.STORAGE;
    }
    return WorkerType.EXECUTE;
  }

  public Path getValidRoot() throws ConfigurationException {
    verifyRootConfiguration();
    addRootIfMissing();
    verifyRootLocation();
    return Paths.get(root);
  }

  private void addRootIfMissing() throws ConfigurationException {
    try {
      if (!Files.isDirectory(Paths.get(root))) {
        Files.createDirectories(Paths.get(root));
      }
    } catch (Exception e) {
      throw new ConfigurationException(e.toString());
    }
  }

  private void verifyRootConfiguration() throws ConfigurationException {
    // Configuration error if no root is specified.
    if (Strings.isNullOrEmpty(root)) {
      throw new ConfigurationException("root value in config missing");
    }
  }

  private void verifyRootLocation() throws ConfigurationException {
    // Configuration error if root does not exist.
    if (!Files.isDirectory(Paths.get(root))) {
      throw new ConfigurationException("root [" + root + "] is not directory");
    }
  }
}
