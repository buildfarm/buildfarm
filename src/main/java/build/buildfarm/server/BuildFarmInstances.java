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
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.memory.MemoryInstance;
import build.buildfarm.instance.shard.ShardInstance;
import build.buildfarm.v1test.InstanceConfig;
import io.grpc.Status;
import io.grpc.StatusException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.naming.ConfigurationException;

public class BuildFarmInstances implements Instances {
  public static StatusException toStatusException(InstanceNotFoundException e) {
    String errorMessage = String.format("Instance %s not known to Service", e.instanceName);
    return Status.NOT_FOUND.withDescription(errorMessage).asException();
  }

  private final Map<String, Instance> instances;
  private final Instance defaultInstance;

  public BuildFarmInstances(List<InstanceConfig> instanceConfigs, String defaultInstanceName)
      throws InterruptedException, ConfigurationException {
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

  private Instance getDefault() {
    return defaultInstance;
  }

  @Override
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

  @Override
  public Instance getFromOperationsCollectionName(
      String operationsCollectionName) throws InstanceNotFoundException {
    String instanceName = UrlPath.fromOperationsCollectionName(operationsCollectionName);
    return get(instanceName);
  }

  @Override
  public Instance getFromOperationName(String operationName)
      throws InstanceNotFoundException {
    String instanceName = UrlPath.fromOperationName(operationName);
    return get(instanceName);
  }

  @Override
  public Instance getFromOperationStream(String operationStream)
      throws InstanceNotFoundException {
    String instanceName = UrlPath.fromOperationStream(operationStream);
    return get(instanceName);
  }

  @Override
  public Instance getFromBlob(String blobName)
      throws InstanceNotFoundException {
    String instanceName = UrlPath.fromBlobName(blobName);
    return get(instanceName);
  }

  @Override
  public Instance getFromUploadBlob(String uploadBlobName)
      throws InstanceNotFoundException {
    String instanceName = UrlPath.fromUploadBlobName(uploadBlobName);
    return get(instanceName);
  }

  private static HashFunction getValidHashFunction(InstanceConfig config) throws ConfigurationException {
    try {
      return HashFunction.get(config.getHashFunction());
    } catch (IllegalArgumentException e) {
      throw new ConfigurationException("hash_function value unrecognized");
    }
  }

  private void createInstances(List<InstanceConfig> instanceConfigs)
      throws InterruptedException, ConfigurationException {
    for (InstanceConfig instanceConfig : instanceConfigs) {
      String name = instanceConfig.getName();
      HashFunction hashFunction = getValidHashFunction(instanceConfig);
      DigestUtil digestUtil = new DigestUtil(hashFunction);
      InstanceConfig.TypeCase typeCase = instanceConfig.getTypeCase();
      switch (typeCase) {
        default:
        case TYPE_NOT_SET:
          throw new IllegalArgumentException("Instance type not set in config");
        case MEMORY_INSTANCE_CONFIG:
          instances.put(name, new MemoryInstance(
              name,
              digestUtil,
              instanceConfig.getMemoryInstanceConfig()));
          break;
        case SHARD_INSTANCE_CONFIG:
          instances.put(name, new ShardInstance(
              name,
              digestUtil,
              instanceConfig.getShardInstanceConfig()));
          break;
      }
    }
  }

  @Override
  public void start() {
    for (Instance instance : instances.values()) {
      instance.start();
    }
  }

  @Override
  public void stop() {
    for (Instance instance : instances.values()) {
      instance.stop();
    }
  }
}
