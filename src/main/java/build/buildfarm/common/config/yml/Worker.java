package build.buildfarm.common.config.yml;

import com.google.common.base.Strings;
import java.util.Arrays;
import java.util.List;
import lombok.Data;

@Data
public class Worker {
  private int port = 8981;
  private String publicName;
  private Capabilities capabilities = new Capabilities();
  private String root = "/tmp/worker";
  private int inlineContentLimit = 1048567; // 1024 * 1024
  private long operationPollPeriod = 1;
  private DequeueMatchSettings dequeueMatchSettings = new DequeueMatchSettings();
  private Cas cas = new Cas();
  private int executeStageWidth = 0;
  private int executeStageWidthOffset = 0;
  private int inputFetchStageWidth = 0;
  private int inputFetchDeadline = 60;
  private boolean linkInputDirectories = true;
  private List<String> realInputDirectories = Arrays.asList("external");
  private String execOwner;
  private int hexBucketLevels = 0;
  private int defaultMaxCores = 0;
  private boolean limitGlobalExecution = false;
  private boolean onlyMulticoreTests = false;
  private boolean allowBringYourOwnContainer = false;
  private boolean errorOperationRemainingResources = false;
  private ExecutionPolicy[] executionPolicies;

  public String getPublicName() {
    if (!Strings.isNullOrEmpty(publicName)) {
      return publicName;
    } else {
      return System.getenv("INSTANCE_NAME");
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
}
