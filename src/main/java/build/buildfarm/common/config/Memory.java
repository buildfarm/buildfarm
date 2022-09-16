package build.buildfarm.common.config;

import build.bazel.remote.execution.v2.Platform;
import build.buildfarm.v1test.CASInsertionPolicy;
import lombok.Data;

@Data
public class Memory {
  private int listOperationsDefaultPageSize;
  private int listOperationsMaxPageSize;
  private int treeDefaultPageSize;
  private int treeMaxPageSize;
  private int operationPollTimeout;
  private int operationCompletedDelay;
  private boolean delegateCas;
  private String target;
  private int deadlineAfterSeconds;
  private boolean streamStdout;
  private boolean streamStderr;
  private CASInsertionPolicy casPolicy;
  private int treePageSize;
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
