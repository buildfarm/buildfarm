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
import build.buildfarm.v1test.BuildFarmServerConfig;
import build.buildfarm.v1test.InstanceConfig;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import com.google.devtools.common.options.OptionsParser;
import com.google.protobuf.TextFormat;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class BuildFarmServer {
  public static final Logger logger =
    Logger.getLogger(BuildFarmServer.class.getName());

  private final BuildFarmServerConfig config;
  private final Map<String, Instance> instances;
  private final Instance defaultInstance;
  private final Server server;

  public BuildFarmServer(BuildFarmServerConfig config) {
    this(ServerBuilder.forPort(config.getPort()), config);
  }

  public BuildFarmServer(ServerBuilder<?> serverBuilder, BuildFarmServerConfig config) {
    this.config = config;
    instances = new HashMap<String, Instance>();
    createInstances();
    String defaultInstanceName = config.getDefaultInstanceName();
    if (!defaultInstanceName.isEmpty()) {
      if (!instances.containsKey(defaultInstanceName)) {
        throw new IllegalArgumentException();
      }
      defaultInstance = instances.get(defaultInstanceName);
    } else {
      defaultInstance = null;
    }
    server = serverBuilder
        .addService(new ActionCacheService(this))
        .addService(new ContentAddressableStorageService(this))
        .addService(new ByteStreamService(this))
        .addService(new ExecutionService(this))
        .addService(new OperationQueueService(this))
        .addService(new WatcherService(this))
        .build();
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

  private void createInstances() {
    for (InstanceConfig instanceConfig : config.getInstancesList()) {
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

  /**
   * Decodes the given byte array assumed to be encoded with ISO-8859-1 encoding (isolatin1).
   */
  private static char[] convertFromLatin1(byte[] content) {
    char[] latin1 = new char[content.length];
    for (int i = 0; i < latin1.length; i++) { // yeah, latin1 is this easy! :-)
      latin1[i] = (char) (0xff & content[i]);
    }
    return latin1;
  }

  private static BuildFarmServerConfig toBuildFarmServerConfig(InputStream inputStream) throws IOException {
    BuildFarmServerConfig.Builder builder = BuildFarmServerConfig.newBuilder();
    String data = new String(convertFromLatin1(ByteStreams.toByteArray(inputStream)));
    TextFormat.merge(data, builder);
    return builder.build();
  }

  public void start() throws IOException {
    server.start();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        BuildFarmServer.this.stop();
        System.err.println("*** server shut down");
      }
    });
  }

  public void stop() {
    if (server != null) {
      server.shutdown();
    }
  }

  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  private static void printUsage(OptionsParser parser) {
    System.out.println("Usage: CONFIG_PATH");
    System.out.println(parser.describeOptions(Collections.<String, String>emptyMap(),
                                              OptionsParser.HelpVerbosity.LONG));
  }

  public static void main(String[] args) throws Exception {
    OptionsParser parser = OptionsParser.newOptionsParser(BuildFarmServerOptions.class);
    parser.parseAndExitUponError(args);
    List<java.lang.String> residue = parser.getResidue();
    if (residue.isEmpty()) {
      printUsage(parser);
      return;
    }
    Path configPath = Paths.get(residue.get(0));
    try (InputStream configInputStream = Files.newInputStream(configPath)) {
      BuildFarmServer server = new BuildFarmServer(toBuildFarmServerConfig(configInputStream));
      configInputStream.close();
      server.start();
      server.blockUntilShutdown();
    }
  }
}
