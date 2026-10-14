// Copyright 2022-2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.common.config;

import build.bazel.remote.execution.v2.Platform;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;

@Data
public class DequeueMatchSettings {
  private boolean allowUnmatched = false;
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
