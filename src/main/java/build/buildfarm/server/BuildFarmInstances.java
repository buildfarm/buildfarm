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
import com.google.common.collect.Iterables;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BuildFarmInstances {
  public static void respondFromException(StreamObserver<?> responseObserver, InstanceNotFoundException ex) {
    String errorMessage = String.format("Instance %s not known to Service", ex.instanceName);
    responseObserver.onError(new StatusException(Status.NOT_FOUND.withDescription(errorMessage)));
  }

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

  public Instance getDefaultInstance() {
    return defaultInstance;
  }

  public Instance getInstance(String name) throws InstanceNotFoundException {
    Instance instance;
    if (name == null || name.isEmpty()) {
      instance = getDefaultInstance();
    } else {
      instance = instances.get(name);
    }
    if (instance == null) {
      throw new InstanceNotFoundException(name);
    }
    return instance;
  }

  public Instance getInstanceFromOperationsCollectionName(
      String operationsCollectionName) throws InstanceNotFoundException {
    // {instance_name=**}/operations
    String[] components = operationsCollectionName.split("/");
    String instanceName = String.join(
        "/", Iterables.limit(
            Arrays.asList(components),
            components.length - 1));
    return getInstance(instanceName);
  }

  public Instance getInstanceFromOperationName(String operationName)
      throws InstanceNotFoundException {
    // {instance_name=**}/operations/{uuid}
    String[] components = operationName.split("/");
    String instanceName = String.join(
        "/", Iterables.limit(
            Arrays.asList(components),
            components.length - 2));
    return getInstance(instanceName);
  }

  public Instance getInstanceFromOperationStream(String operationStream)
      throws InstanceNotFoundException {
    // {instance_name=**}/operations/{uuid}/streams/{stream}
    String[] components = operationStream.split("/");
    String instanceName = String.join(
        "/", Iterables.limit(
            Arrays.asList(components),
            components.length - 4));
    return getInstance(instanceName);
  }

  public Instance getInstanceFromBlob(String blobName)
      throws InstanceNotFoundException {
    // {instance_name=**}/blobs/{hash}/{size}
    String[] components = blobName.split("/");
    String instanceName = String.join(
        "/", Iterables.limit(
            Arrays.asList(components),
            components.length - 3));
    return getInstance(instanceName);
  }

  public Instance getInstanceFromUploadBlob(String uploadBlobName)
      throws InstanceNotFoundException {
    // {instance_name=**}/uploads/{uuid}/blobs/{hash}/{size}
    String[] components = uploadBlobName.split("/");
    String instanceName = String.join(
        "/", Iterables.limit(
            Arrays.asList(components),
            components.length - 5));
    return getInstance(instanceName);
  }

  private void createInstances(List<InstanceConfig> instanceConfigs) {
    for (InstanceConfig instanceConfig : instanceConfigs) {
      String name = instanceConfig.getName();
      InstanceConfig.TypeCase typeCase = instanceConfig.getTypeCase();
      switch (instanceConfig.getTypeCase()) {
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

