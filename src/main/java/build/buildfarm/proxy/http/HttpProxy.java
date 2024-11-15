// Copyright 2017 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.proxy.http;

import static java.util.concurrent.TimeUnit.SECONDS;

import build.buildfarm.common.LoggingMain;
import com.google.auth.Credentials;
import com.google.devtools.common.options.OptionsParser;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.util.TransmitStatusRuntimeExceptionInterceptor;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import javax.net.ssl.SSLException;
import lombok.extern.java.Log;

@Log
public class HttpProxy extends LoggingMain {
  // We need to keep references to the root and netty loggers to prevent them from being garbage
  // collected, which would cause us to loose their configuration.
  private static final Logger nettyLogger = Logger.getLogger("io.grpc.netty");

  private final Server server;

  private HealthStatusManager healthStatusManager;

  public HttpProxy(HttpProxyOptions options, @Nullable Credentials creds)
      throws URISyntaxException, SSLException {
    this(ServerBuilder.forPort(options.port), creds, options);
  }

  public HttpProxy(
      ServerBuilder<?> serverBuilder, @Nullable Credentials creds, HttpProxyOptions options)
      throws URISyntaxException, SSLException {
    super("HttpProxy");

    healthStatusManager = new HealthStatusManager();

    SimpleBlobStore simpleBlobStore =
        HttpBlobStore.create(
            URI.create(options.httpCache),
            /* remoteMaxConnections= */ 0,
            (int) SECONDS.toMillis(options.timeout),
            creds);
    server =
        serverBuilder
            .addService(healthStatusManager.getHealthService())
            .addService(new ActionCacheService(simpleBlobStore))
            .addService(
                new ContentAddressableStorageService(
                    simpleBlobStore, options.treeDefaultPageSize, options.treeMaxPageSize))
            .addService(new ByteStreamService(simpleBlobStore))
            .intercept(TransmitStatusRuntimeExceptionInterceptor.instance())
            .build();
  }

  public void start() throws IOException {
    healthStatusManager.setStatus(
        HealthStatusManager.SERVICE_NAME_ALL_SERVICES, ServingStatus.SERVING);
    server.start();
  }

  @Override
  protected void onShutdown() {
    System.err.println("*** shutting down gRPC server since JVM is shutting down");
    if (healthStatusManager != null) {
      healthStatusManager.setStatus(
          HealthStatusManager.SERVICE_NAME_ALL_SERVICES, ServingStatus.NOT_SERVING);
    }
    stop();
    System.err.println("*** server shut down");
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
    System.out.println("Usage: [OPTIONS]");
    System.out.println(
        parser.describeOptions(Collections.emptyMap(), OptionsParser.HelpVerbosity.LONG));
  }

  @SuppressWarnings("ConstantConditions")
  public static void main(String[] args) throws Exception {
    // Only log severe log messages from Netty. Otherwise it logs warnings that look like this:
    //
    // 170714 08:16:28.552:WT 18 [io.grpc.netty.NettyServerHandler.onStreamError] Stream Error
    // io.netty.handler.codec.http2.Http2Exception$StreamException: Received DATA frame for an
    // unknown stream 11369
    nettyLogger.setLevel(Level.SEVERE);

    OptionsParser parser =
        OptionsParser.newOptionsParser(HttpProxyOptions.class, AuthAndTLSOptions.class);
    parser.parseAndExitUponError(args);
    List<String> residue = parser.getResidue();
    if (!residue.isEmpty()) {
      printUsage(parser);
      throw new IllegalArgumentException("Unrecognized arguments: " + residue);
    }
    HttpProxyOptions options = parser.getOptions(HttpProxyOptions.class);
    if (options.port < 0) {
      printUsage(parser);
      throw new IllegalArgumentException("invalid port: " + options.port);
    }
    AuthAndTLSOptions authAndTlsOptions = parser.getOptions(AuthAndTLSOptions.class);
    HttpProxy server = new HttpProxy(options, GoogleAuthUtils.newCredentials(authAndTlsOptions));
    server.start();
    server.blockUntilShutdown();
  }
}
