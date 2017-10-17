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
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class BuildFarmServer {
  public static final Logger logger =
    Logger.getLogger(BuildFarmServer.class.getName());

  private final BuildFarmServerConfig config;
  private final BuildFarmInstances instances;
  private final Server server;

  public BuildFarmServer(BuildFarmServerConfig config) {
    this(ServerBuilder.forPort(config.getPort()), config);
  }

  public BuildFarmServer(ServerBuilder<?> serverBuilder, BuildFarmServerConfig config) {
    this.config = config;
    String defaultInstanceName = config.getDefaultInstanceName();
    instances = new BuildFarmInstances(config.getInstancesList(), defaultInstanceName);
    server = serverBuilder
        .addService(new ActionCacheService(instances))
        .addService(new ContentAddressableStorageService(instances))
        .addService(new ByteStreamService(instances))
        .addService(new ExecutionService(instances))
        .addService(new OperationQueueService(instances))
        .addService(new WatcherService(instances))
        .build();
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

  private static BuildFarmServerConfig toBuildFarmServerConfig(InputStream inputStream, BuildFarmServerOptions options) throws IOException {
    BuildFarmServerConfig.Builder builder = BuildFarmServerConfig.newBuilder();
    String data = new String(convertFromLatin1(ByteStreams.toByteArray(inputStream)));
    TextFormat.merge(data, builder);
    if (options.port > 0) {
        builder.setPort(options.port);
    }
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
      throw new IllegalArgumentException("Missing CONFIG_PATH");
    }
    Path configPath = Paths.get(residue.get(0));
    try (InputStream configInputStream = Files.newInputStream(configPath)) {
      BuildFarmServer server = new BuildFarmServer(toBuildFarmServerConfig(configInputStream, parser.getOptions(BuildFarmServerOptions.class)));
      configInputStream.close();
      server.start();
      server.blockUntilShutdown();
    }
  }
}
