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

package build.buildfarm.server;

import static java.util.logging.Level.INFO;

import build.buildfarm.admin.Admin;
import build.buildfarm.admin.aws.AwsAdmin;
import build.buildfarm.admin.gcp.GcpAdmin;
import build.buildfarm.common.CasIndexResults;
import build.buildfarm.instance.Instance;
import build.buildfarm.v1test.AdminConfig;
import build.buildfarm.v1test.AdminGrpc;
import build.buildfarm.v1test.DisableScaleInProtectionRequest;
import build.buildfarm.v1test.DisableScaleInProtectionRequestResults;
import build.buildfarm.v1test.GetClientStartTimeRequest;
import build.buildfarm.v1test.GetClientStartTimeResult;
import build.buildfarm.v1test.GetHostsRequest;
import build.buildfarm.v1test.GetHostsResult;
import build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequest;
import build.buildfarm.v1test.ReindexAllCasRequest;
import build.buildfarm.v1test.ReindexCasRequest;
import build.buildfarm.v1test.ReindexCasRequestResults;
import build.buildfarm.v1test.ScaleClusterRequest;
import build.buildfarm.v1test.ShutDownWorkerGracefullyRequest;
import build.buildfarm.v1test.ShutDownWorkerGracefullyRequestResults;
import build.buildfarm.v1test.ShutDownWorkerGrpc;
import build.buildfarm.v1test.StopContainerRequest;
import build.buildfarm.v1test.TerminateHostRequest;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AdminService extends AdminGrpc.AdminImplBase {
  private static final Logger logger = Logger.getLogger(AdminService.class.getName());

  private final Admin adminController;
  private final Instance instance;

  public AdminService(AdminConfig config, Instance instance) {
    this.adminController = getAdminController(config);
    this.instance = instance;
  }

  @Override
  public void terminateHost(TerminateHostRequest request, StreamObserver<Status> responseObserver) {
    try {
      if (adminController != null) {
        adminController.terminateHost(request.getHostId());
      }
      responseObserver.onNext(Status.newBuilder().setCode(Code.OK_VALUE).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Could not terminate host.", e);
      responseObserver.onError(io.grpc.Status.fromThrowable(e).asException());
    }
  }

  @Override
  public void stopContainer(StopContainerRequest request, StreamObserver<Status> responseObserver) {
    try {
      if (adminController != null) {
        adminController.stopContainer(request.getHostId(), request.getContainerName());
      }
      responseObserver.onNext(Status.newBuilder().setCode(Code.OK_VALUE).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Could not stop container.", e);
      responseObserver.onError(io.grpc.Status.fromThrowable(e).asException());
    }
  }

  @Override
  public void getHosts(GetHostsRequest request, StreamObserver<GetHostsResult> responseObserver) {
    try {
      GetHostsResult result = null;
      if (adminController != null) {
        result =
            adminController.getHosts(
                request.getFilter(), request.getAgeInMinutes(), request.getStatus());
      }
      responseObserver.onNext(result);
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Could not get hosts.", e);
      responseObserver.onError(io.grpc.Status.fromThrowable(e).asException());
    }
  }

  @Override
  public void getClientStartTime(
      GetClientStartTimeRequest request,
      StreamObserver<GetClientStartTimeResult> responseObserver) {
    try {
      GetClientStartTimeResult result = instance.getClientStartTime(request);
      responseObserver.onNext(result);
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.log(
          Level.SEVERE,
          String.format("Could not get client start time for %s.", request.getInstanceName()),
          e);
      responseObserver.onError(io.grpc.Status.fromThrowable(e).asException());
    }
  }

  @Override
  public void scaleCluster(ScaleClusterRequest request, StreamObserver<Status> responseObserver) {
    try {
      if (adminController != null) {
        adminController.scaleCluster(
            request.getScaleGroupName(),
            request.getMinHosts(),
            request.getMaxHosts(),
            request.getTargetHosts(),
            request.getTargetReservedHostsPercent());
      }
      responseObserver.onNext(Status.newBuilder().setCode(Code.OK_VALUE).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Could not scale cluster.", e);
      responseObserver.onError(io.grpc.Status.fromThrowable(e).asException());
    }
  }

  @Override
  public void reindexCas(
      ReindexCasRequest request, StreamObserver<ReindexCasRequestResults> responseObserver) {
    try {
      CasIndexResults results = instance.reindexCas(request.getHostId());
      logger.log(INFO, results.toMessage());
      responseObserver.onNext(
          ReindexCasRequestResults.newBuilder()
              .setRemovedHosts(results.removedHosts)
              .setRemovedKeys(results.removedKeys)
              .setTotalKeys(results.totalKeys)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Could not reindex CAS.", e);
      responseObserver.onError(io.grpc.Status.fromThrowable(e).asException());
    }
  }

  @Override
  public void reindexAllCas(
      ReindexAllCasRequest request, StreamObserver<ReindexCasRequestResults> responseObserver) {
    try {
      String arg = null;
      CasIndexResults results = instance.reindexCas(arg);
      logger.log(INFO, results.toMessage());
      responseObserver.onNext(
          ReindexCasRequestResults.newBuilder()
              .setRemovedHosts(results.removedHosts)
              .setRemovedKeys(results.removedKeys)
              .setTotalKeys(results.totalKeys)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Could not reindex CAS.", e);
      responseObserver.onError(io.grpc.Status.fromThrowable(e).asException());
    }
  }

  /**
   * Server-side implementation of ShutDownWorkerGracefully. This will reroute the request to target
   * worker.
   *
   * @param request ShutDownWorkerGracefullyRequest received through grpc
   * @param responseObserver grpc response observer
   */
  @Override
  public void shutDownWorkerGracefully(
      ShutDownWorkerGracefullyRequest request,
      StreamObserver<ShutDownWorkerGracefullyRequestResults> responseObserver) {
    try {
      informWorkerToPrepareForShutdown(request.getWorkerName());
      responseObserver.onNext(ShutDownWorkerGracefullyRequestResults.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      String errorMessage =
          String.format(
              "Could not inform the worker %s to prepare for graceful shutdown with error %s.",
              request.getWorkerName(), e.getMessage());
      logger.log(Level.SEVERE, errorMessage);
      responseObserver.onError(new Exception(errorMessage));
    }
  }

  /**
   * Inform a worker to prepare for graceful shutdown.
   *
   * @param host the host that should be prepared for shutdown.
   */
  @SuppressWarnings("ResultOfMethodCallIgnored")
  private void informWorkerToPrepareForShutdown(String host) {
    ManagedChannel channel = null;
    try {
      NettyChannelBuilder builder =
          NettyChannelBuilder.forTarget(host).negotiationType(NegotiationType.PLAINTEXT);
      channel = builder.build();
      ShutDownWorkerGrpc.ShutDownWorkerBlockingStub shutDownWorkerBlockingStub =
          ShutDownWorkerGrpc.newBlockingStub(channel);
      shutDownWorkerBlockingStub.prepareWorkerForGracefulShutdown(
          PrepareWorkerForGracefulShutDownRequest.newBuilder().build());
    } finally {
      if (channel != null) {
        channel.shutdown();
      }
    }
  }

  /**
   * Server-side implementation of disableScaleInProtection.
   *
   * @param request grpc request
   * @param responseObserver grpc response observer
   */
  @Override
  public void disableScaleInProtection(
      DisableScaleInProtectionRequest request,
      StreamObserver<DisableScaleInProtectionRequestResults> responseObserver) {
    try {
      String hostPrivateIp = trimHostPrivateDns(request.getInstanceName());
      adminController.disableHostScaleInProtection(hostPrivateIp);
      responseObserver.onNext(DisableScaleInProtectionRequestResults.newBuilder().build());
      responseObserver.onCompleted();
    } catch (RuntimeException e) {
      responseObserver.onError(e);
    }
  }

  /**
   * The private dns get from worker might be suffixed with ":portNumber", which should be trimmed.
   *
   * @param hostPrivateIp the private dns should be trimmed.
   * @return
   */
  @SuppressWarnings("JavaDoc")
  private String trimHostPrivateDns(String hostPrivateIp) {
    String portSeparator = ":";
    if (hostPrivateIp.contains(portSeparator)) {
      hostPrivateIp = hostPrivateIp.split(portSeparator)[0];
    }
    return hostPrivateIp;
  }

  private static Admin getAdminController(AdminConfig config) {
    switch (config.getDeploymentEnvironment()) {
      default:
        return null;
      case "aws":
        return new AwsAdmin(config.getAwsAdminConfig().getRegion());
      case "gcp":
        return new GcpAdmin();
    }
  }
}
