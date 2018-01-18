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

import build.buildfarm.common.DigestUtil;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.memory.MemoryInstance;
import build.buildfarm.v1test.InstanceConfig;
import io.grpc.Status;
import io.grpc.StatusException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.naming.ConfigurationException;

public class BuildFarmInstances {
  public static StatusException toStatusException(InstanceNotFoundException ex) {
    String errorMessage = String.format("Instance %s not known to Service", ex.instanceName);
    return new StatusException(Status.NOT_FOUND.withDescription(errorMessage));
  }

  private final Map<String, Instance> instances;
  private final Instance defaultInstance;

  public BuildFarmInstances(List<InstanceConfig> instanceConfigs, String defaultInstanceName) throws ConfigurationException {
    instances = new HashMap<String, Instance>();
    createInstances(instanceConfigs);
    if (!defaultInstanceName.isEmpty()) {
      if (!instances.containsKey(defaultInstanceName)) {
        throw new ConfigurationException(defaultInstanceName + " not specified in instance configs.");
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

  private static DigestUtil.HashFunction getValidHashFunction(InstanceConfig config) throws ConfigurationException {
    switch (config.getHashFunction()) {
      default:
      case UNRECOGNIZED:
        throw new ConfigurationException("HashFunction value unrecognized");
      case MD5: return DigestUtil.HashFunction.MD5;
      case SHA1: return DigestUtil.HashFunction.SHA1;
      case SHA256: return DigestUtil.HashFunction.SHA256;
    }
  }

  private void createInstances(List<InstanceConfig> instanceConfigs) throws ConfigurationException {
    for (InstanceConfig instanceConfig : instanceConfigs) {
      String name = instanceConfig.getName();
      DigestUtil.HashFunction hashFunction = getValidHashFunction(instanceConfig);
      InstanceConfig.TypeCase typeCase = instanceConfig.getTypeCase();
      switch (typeCase) {
        default:
        case TYPE_NOT_SET:
          throw new IllegalArgumentException("Instance type not set in config");
        case MEMORY_INSTANCE_CONFIG:
          instances.put(name, new MemoryInstance(
              name,
              instanceConfig.getMemoryInstanceConfig(),
              new DigestUtil(hashFunction)));
          break;
      }
    }
  }
}

