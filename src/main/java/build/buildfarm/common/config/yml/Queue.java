package build.buildfarm.common.config.yml;

import build.bazel.remote.execution.v2.Platform;
import java.util.List;

public class Queue {
  private String name;
  private boolean allowUnmatched = true;

  private Platform platform = Platform.newBuilder().build();

  private List<Property> properties;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public boolean isAllowUnmatched() {
    return allowUnmatched;
  }

  public void setAllowUnmatched(boolean allowUnmatched) {
    this.allowUnmatched = allowUnmatched;
  }

  public void setProperties(List<Property> properties) {
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
    return "Queue{"
        + "name='"
        + name
        + '\''
        + ", allowUnmatched="
        + allowUnmatched
        + ", properties="
        + properties
        + '}';
  }
}
