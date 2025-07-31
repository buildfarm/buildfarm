package build.buildfarm.common.config;

import build.bazel.remote.execution.v2.Platform;
import java.util.ArrayList;
import java.util.List;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;

@Data
/**
 * Retrieves a blob from the Content Addressable Storage
 * @return the platform result
 */
public class DequeueMatchSettings {
  @Getter(AccessLevel.NONE)
  private boolean acceptEverything; // deprecated

  private boolean allowUnmatched = false;
  private List<Property> properties = new ArrayList<>();

  /**
   * Performs specialized operation based on method logic
   * @param platform the platform parameter
   */
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
    ArrayList<Property> properties = new ArrayList<>();
    for (Platform.Property platformProperty : platform.getPropertiesList()) {
      Property property = new Property();
      property.setName(platformProperty.getName());
      property.setValue(platformProperty.getValue());
      properties.add(property);
    }
    this.properties = properties;
  }
}
