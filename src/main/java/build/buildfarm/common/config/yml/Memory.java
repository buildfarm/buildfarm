package build.buildfarm.common.config.yml;

import build.bazel.remote.execution.v2.Platform;

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

  public int getListOperationsDefaultPageSize() {
    return listOperationsDefaultPageSize;
  }

  public void setListOperationsDefaultPageSize(int listOperationsDefaultPageSize) {
    this.listOperationsDefaultPageSize = listOperationsDefaultPageSize;
  }

  public int getListOperationsMaxPageSize() {
    return listOperationsMaxPageSize;
  }

  public void setListOperationsMaxPageSize(int listOperationsMaxPageSize) {
    this.listOperationsMaxPageSize = listOperationsMaxPageSize;
  }

  public int getTreeDefaultPageSize() {
    return treeDefaultPageSize;
  }

  public void setTreeDefaultPageSize(int treeDefaultPageSize) {
    this.treeDefaultPageSize = treeDefaultPageSize;
  }

  public int getTreeMaxPageSize() {
    return treeMaxPageSize;
  }

  public void setTreeMaxPageSize(int treeMaxPageSize) {
    this.treeMaxPageSize = treeMaxPageSize;
  }

  public int getOperationPollTimeout() {
    return operationPollTimeout;
  }

  public void setOperationPollTimeout(int operationPollTimeout) {
    this.operationPollTimeout = operationPollTimeout;
  }

  public int getOperationCompletedDelay() {
    return operationCompletedDelay;
  }

  public void setOperationCompletedDelay(int operationCompletedDelay) {
    this.operationCompletedDelay = operationCompletedDelay;
  }

  public boolean isDelegateCas() {
    return delegateCas;
  }

  public void setDelegateCas(boolean delegateCas) {
    this.delegateCas = delegateCas;
  }

  public String getTarget() {
    return target;
  }

  public void setTarget(String target) {
    this.target = target;
  }

  public int getDeadlineAfterSeconds() {
    return deadlineAfterSeconds;
  }

  public void setDeadlineAfterSeconds(int deadlineAfterSeconds) {
    this.deadlineAfterSeconds = deadlineAfterSeconds;
  }

  public boolean isStreamStdout() {
    return streamStdout;
  }

  public void setStreamStdout(boolean streamStdout) {
    this.streamStdout = streamStdout;
  }

  public boolean isStreamStderr() {
    return streamStderr;
  }

  public void setStreamStderr(boolean streamStderr) {
    this.streamStderr = streamStderr;
  }

  public String getCasPolicy() {
    return casPolicy;
  }

  public void setCasPolicy(String casPolicy) {
    this.casPolicy = casPolicy;
  }

  public int getTreePageSize() {
    return treePageSize;
  }

  public void setTreePageSize(int treePageSize) {
    this.treePageSize = treePageSize;
  }

  public void setProperties(Property[] properties) {
    this.properties = properties;
  }

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

  @Override
  public String toString() {
    return "Memory{"
        + "listOperationsDefaultPageSize="
        + listOperationsDefaultPageSize
        + ", listOperationsMaxPageSize="
        + listOperationsMaxPageSize
        + ", treeDefaultPageSize="
        + treeDefaultPageSize
        + ", treeMaxPageSize="
        + treeMaxPageSize
        + ", operationPollTimeout="
        + operationPollTimeout
        + ", operationCompletedDelay="
        + operationCompletedDelay
        + ", delegateCas="
        + delegateCas
        + ", target='"
        + target
        + '\''
        + ", deadlineAfterSeconds="
        + deadlineAfterSeconds
        + ", streamStdout="
        + streamStdout
        + ", streamStderr="
        + streamStderr
        + ", casPolicy='"
        + casPolicy
        + '\''
        + ", treePageSize="
        + treePageSize
        + ", platform="
        + platform
        + '}';
  }
}
