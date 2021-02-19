// Copyright 2020 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker.shard;

import static java.util.logging.Level.WARNING;

import build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequest;
import build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequestResults;
import build.buildfarm.v1test.ShardWorkerConfig;
import build.buildfarm.v1test.ShutDownWorkerGrpc;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

public class ShutDownWorkerGracefully extends ShutDownWorkerGrpc.ShutDownWorkerImplBase {
  private static final Logger logger = Logger.getLogger(ShutDownWorkerGracefully.class.getName());
  private final Worker worker;
  private final ShardWorkerConfig config;

  public ShutDownWorkerGracefully(Worker worker, ShardWorkerConfig config) {
    this.worker = worker;
    this.config = config;
  }

  /**
   * Start point of worker graceful shutdown.
   *
   * @param request
   * @param responseObserver
   */
  @Override
  public void prepareWorkerForGracefulShutdown(
      PrepareWorkerForGracefulShutDownRequest request,
      StreamObserver<PrepareWorkerForGracefulShutDownRequestResults> responseObserver) {
    String clusterId = config.getAdminConfig().getClusterId();
    String clusterEndpoint = config.getAdminConfig().getClusterEndpoint();
    if (clusterId == null
        || clusterId.equals("")
        || clusterEndpoint == null
        || clusterEndpoint.equals("")) {
      String errorMessage =
          String.format(
              "Current AdminConfig doesn't have cluster_id or cluster_endpoint set, "
                  + "the worker %s won't be shut down.",
              config.getPublicName());
      logger.log(WARNING, errorMessage);
      responseObserver.onError(new RuntimeException(errorMessage));
      return;
    }

    if (!config.getAdminConfig().getEnableGracefulShutdown()) {
      String errorMessage =
          String.format(
              "Current AdminConfig doesn't support shut down worker gracefully, "
                  + "the worker %s won't be shut down.",
              config.getPublicName());
      logger.log(WARNING, errorMessage);
      responseObserver.onError(new RuntimeException(errorMessage));
      return;
    }

    try {
      CompletableFuture.runAsync(worker::prepareWorkerForGracefulShutdown);
      responseObserver.onNext(PrepareWorkerForGracefulShutDownRequestResults.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }
}
