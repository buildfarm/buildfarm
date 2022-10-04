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

import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequest;
import build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequestResults;
import build.buildfarm.v1test.ShutDownWorkerGrpc;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import lombok.extern.java.Log;

@Log
public class ShutDownWorkerGracefully extends ShutDownWorkerGrpc.ShutDownWorkerImplBase {

  private static BuildfarmConfigs configs = BuildfarmConfigs.getInstance();
  private final Worker worker;

  public ShutDownWorkerGracefully(Worker worker) {
    this.worker = worker;
  }

  /**
   * Start point of worker graceful shutdown.
   *
   * @param request
   * @param responseObserver
   */
  @SuppressWarnings({"JavaDoc", "ConstantConditions"})
  @Override
  public void prepareWorkerForGracefulShutdown(
      PrepareWorkerForGracefulShutDownRequest request,
      StreamObserver<PrepareWorkerForGracefulShutDownRequestResults> responseObserver) {
    String clusterId = configs.getServer().getClusterId();
    String clusterEndpoint = configs.getServer().getAdmin().getClusterEndpoint();
    if (clusterId == null
        || clusterId.equals("")
        || clusterEndpoint == null
        || clusterEndpoint.equals("")) {
      String errorMessage =
          String.format(
              "Current AdminConfig doesn't have cluster_id or cluster_endpoint set, "
                  + "the worker %s won't be shut down.",
              configs.getWorker().getPublicName());
      log.log(WARNING, errorMessage);
      responseObserver.onError(new RuntimeException(errorMessage));
      return;
    }

    if (!configs.getServer().getAdmin().isEnableGracefulShutdown()) {
      String errorMessage =
          String.format(
              "Current AdminConfig doesn't support shut down worker gracefully, "
                  + "the worker %s won't be shut down.",
              configs.getWorker().getPublicName());
      log.log(WARNING, errorMessage);
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
