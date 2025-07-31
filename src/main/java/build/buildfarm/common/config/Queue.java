package build.buildfarm.common.config;

import build.bazel.remote.execution.v2.Platform;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;

@Data
public class Queue {
  /**
   * Retrieves a blob from the Content Addressable Storage
   * @return the platform result
   */
  public enum QUEUE_TYPE {
    priority,
    standard
  }

  private String name;
  private boolean allowUnmatched = true;
  private List<Property> properties = new ArrayList<>();

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
}
