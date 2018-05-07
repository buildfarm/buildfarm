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

import build.buildfarm.v1test.BuildFarmServerConfig;
import com.google.common.io.ByteStreams;
import com.google.devtools.common.options.OptionsParser;
import com.google.protobuf.TextFormat;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.util.TransmitStatusRuntimeExceptionInterceptor;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.naming.ConfigurationException;

public class BuildFarmServer {
  // We need to keep references to the root and netty loggers to prevent them from being garbage
  // collected, which would cause us to loose their configuration.
  private static final Logger nettyLogger = Logger.getLogger("io.grpc.netty");
  public static final Logger logger =
    Logger.getLogger(BuildFarmServer.class.getName());

  private final BuildFarmServerConfig config;
  private final Instances instances;
  private final Server server;

  public BuildFarmServer(BuildFarmServerConfig config)
      throws InterruptedException, ConfigurationException {
    this(ServerBuilder.forPort(config.getPort()), config);
  }

  public BuildFarmServer(ServerBuilder<?> serverBuilder, BuildFarmServerConfig config)
      throws InterruptedException, ConfigurationException {
    this.config = config;
    String defaultInstanceName = config.getDefaultInstanceName();
    instances = new BuildFarmInstances(config.getInstancesList(), defaultInstanceName);
    server = serverBuilder
        .addService(new ActionCacheService(instances))
        .addService(new ContentAddressableStorageService(instances))
        .addService(new ByteStreamService(instances))
        .addService(new ExecutionService(instances))
        .addService(new OperationQueueService(instances))
        .addService(new OperationsService(instances))
        .addService(new WatcherService(instances))
        .intercept(TransmitStatusRuntimeExceptionInterceptor.instance())
        .build();
  }

  private static BuildFarmServerConfig toBuildFarmServerConfig(Readable input, BuildFarmServerOptions options) throws IOException {
    BuildFarmServerConfig.Builder builder = BuildFarmServerConfig.newBuilder();
    TextFormat.merge(input, builder);
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
    // Only log severe log messages from Netty. Otherwise it logs warnings that look like this:
    //
    // 170714 08:16:28.552:WT 18 [io.grpc.netty.NettyServerHandler.onStreamError] Stream Error
    // io.netty.handler.codec.http2.Http2Exception$StreamException: Received DATA frame for an
    // unknown stream 11369
    nettyLogger.setLevel(Level.SEVERE);

    OptionsParser parser = OptionsParser.newOptionsParser(BuildFarmServerOptions.class);
    parser.parseAndExitUponError(args);
    List<String> residue = parser.getResidue();
    if (residue.isEmpty()) {
      printUsage(parser);
      throw new IllegalArgumentException("Missing CONFIG_PATH");
    }
    Path configPath = Paths.get(residue.get(0));
    BuildFarmServer server;
    try (InputStream configInputStream = Files.newInputStream(configPath)) {
      server = new BuildFarmServer(toBuildFarmServerConfig(new InputStreamReader(configInputStream), parser.getOptions(BuildFarmServerOptions.class)));
    }
    server.start();
    server.blockUntilShutdown();
  }
}
