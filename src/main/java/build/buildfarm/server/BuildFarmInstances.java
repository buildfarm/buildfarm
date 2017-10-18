// Copyright 2017 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.server;

import build.buildfarm.instance.Instance;
import build.buildfarm.instance.memory.MemoryInstance;
import build.buildfarm.v1test.InstanceConfig;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BuildFarmInstances {
  private final Map<String, Instance> instances;
  private final Instance defaultInstance;

  public BuildFarmInstances(List<InstanceConfig> instanceConfigs, String defaultInstanceName) {
    instances = new HashMap<String, Instance>();
    createInstances(instanceConfigs);
    if (!defaultInstanceName.isEmpty()) {
      if (!instances.containsKey(defaultInstanceName)) {
        throw new IllegalArgumentException();
      }
      defaultInstance = instances.get(defaultInstanceName);
    } else {
      defaultInstance = null;
    }
  }

  public Instance getDefault() {
    return defaultInstance;
  }

  public Instance get(String name) throws InstanceNotFoundException {
    Instance instance;
    if (name == null || name.isEmpty()) {
      instance = getDefault();
    } else {
      instance = instances.get(name);
    }
    if (instance == null) {
      throw new InstanceNotFoundException(name);
    }
    return instance;
  }

  public Instance getFromOperationsCollectionName(
      String operationsCollectionName) throws InstanceNotFoundException {
    String instanceName = UrlPath.fromOperationsCollectionName(operationsCollectionName);
    return get(instanceName);
  }

  public Instance getFromOperationName(String operationName)
      throws InstanceNotFoundException {
    String instanceName = UrlPath.fromOperationName(operationName);
    return get(instanceName);
  }

  public Instance getFromOperationStream(String operationStream)
      throws InstanceNotFoundException {
    String instanceName = UrlPath.fromOperationStream(operationStream);
    return get(instanceName);
  }

  public Instance getFromBlob(String blobName)
      throws InstanceNotFoundException {
    String instanceName = UrlPath.fromBlobName(blobName);
    return get(instanceName);
  }

  public Instance getFromUploadBlob(String uploadBlobName)
      throws InstanceNotFoundException {
    String instanceName = UrlPath.fromUploadBlobName(uploadBlobName);
    return get(instanceName);
  }

  private void createInstances(List<InstanceConfig> instanceConfigs) {
    for (InstanceConfig instanceConfig : instanceConfigs) {
      String name = instanceConfig.getName();
      InstanceConfig.TypeCase typeCase = instanceConfig.getTypeCase();
      switch (typeCase) {
        default:
        case TYPE_NOT_SET:
          throw new IllegalArgumentException("Instance type not set in config");
        case MEMORY_INSTANCE_CONFIG:
          instances.put(name, new MemoryInstance(
              name,
              instanceConfig.getMemoryInstanceConfig()));
          break;
      }
    }
  }
}

