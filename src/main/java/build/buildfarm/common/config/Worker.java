package build.buildfarm.common.config;

import com.google.common.base.Strings;
import java.util.List;
import lombok.Data;

@Data
public class Worker {
  private int port;
  private String publicName;
  private Capabilities capabilities = new Capabilities();
  private String root;
  private int inlineContentLimit;
  private long operationPollPeriod;
  private DequeueMatchSettings dequeueMatchSettings = new DequeueMatchSettings();
  private Cas cas = new Cas();
  private int executeStageWidth;
  private int executeStageWidthOffset;
  private int inputFetchStageWidth;
  private int inputFetchDeadline;
  private boolean linkInputDirectories;
  private List<String> realInputDirectories;
  private String execOwner;
  private int hexBucketLevels;
  private int defaultMaxCores;
  private boolean limitGlobalExecution;
  private boolean onlyMulticoreTests;
  private boolean allowBringYourOwnContainer;
  private boolean errorOperationRemainingResources;
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
