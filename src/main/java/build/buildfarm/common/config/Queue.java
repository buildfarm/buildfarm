package build.buildfarm.common.config;

import build.bazel.remote.execution.v2.Platform;
import java.util.List;
import lombok.Data;

@Data
public class Queue {
  public enum QUEUE_TYPE {
    priority,
    standard
  }

  @Data
  public static class Property {
    private String name;
    private String value;
  }

  private String name;
  private boolean allowUnmatched;
  private List<Property> properties;

  public Platform getPlatform() {
    Platform.Builder platformBuilder = Platform.newBuilder();
    for (Property property : properties) {
      platformBuilder.addProperties(
          Platform.Property.newBuilder()
              .setName(property.getName())
              .setValue(property.getValue())
              .build());
    }
    return platformBuilder.build();
  }
}
