package build.buildfarm.common.config.yml;

import build.bazel.remote.execution.v2.Platform;
import lombok.Data;

@Data
public class Memory {
  private int listOperationsDefaultPageSize = 1024;
  private int listOperationsMaxPageSize = 16384;
  private int treeDefaultPageSize = 1024;
  private int treeMaxPageSize = 16384;
  private int operationPollTimeout = 30;
  private int operationCompletedDelay = 10;
  private boolean delegateCas = true;
  private String target;
  private int deadlineAfterSeconds = 60;
  private boolean streamStdout = true;
  private boolean streamStderr = true;
  private String casPolicy = "ALWAYS_INSERT";
  private int treePageSize = 0;
  private Platform platform = Platform.newBuilder().build();
  private Property[] properties;

  public Platform getPlatform() {
    Platform.Builder platformBuilder = Platform.newBuilder();
    for (Property property : this.properties) {
      platformBuilder.addProperties(
          Platform.Property.newBuilder()
              .setName(property.getName())
              .setValue(property.getValue())
              .build());
    }
    return platformBuilder.build();
  }

  public void setPlatform(Platform platform) {
    this.platform = Platform.newBuilder().build();
  }
}
