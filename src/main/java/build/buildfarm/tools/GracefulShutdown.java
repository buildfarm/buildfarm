// Copyright 2020 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.tools;

import static build.buildfarm.common.grpc.Channels.createChannel;

import build.buildfarm.v1test.AdminGrpc;
import build.buildfarm.v1test.DisableScaleInProtectionRequest;
import build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequest;
import build.buildfarm.v1test.ShutDownWorkerGracefullyRequest;
import build.buildfarm.v1test.WorkerControlGrpc;
import io.grpc.ManagedChannel;

class GracefulShutdown {
  /**
   * Example command: GracefulShutdown ShutDown workerIp buildfarm-endpoint
   *
   * @param args
   */
  @SuppressWarnings({"JavaDoc", "ResultOfMethodCallIgnored"})
  private static void shutDownGracefully(String[] args) {
    String workerName = args[1];
    String bfEndpoint = args[2];
    System.out.println(
        "Sending ShutDownWorkerGracefully request to bf "
            + bfEndpoint
            + " to shut down worker "
            + workerName
            + " gracefully");
    ManagedChannel channel = createChannel(bfEndpoint);
    AdminGrpc.AdminBlockingStub adminBlockingStub = AdminGrpc.newBlockingStub(channel);
    adminBlockingStub.shutDownWorkerGracefully(
        ShutDownWorkerGracefullyRequest.newBuilder().setWorkerName(workerName).build());

    System.out.println("Request is sent");
  }

  /**
   * Example command: GracefulShutdown PrepareWorker WorkerIp:port
   *
   * @param args
   */
  @SuppressWarnings({"JavaDoc", "ResultOfMethodCallIgnored"})
  private static void prepareWorkerForShutDown(String[] args) {
    String workerIpWithPort = args[1];
    System.out.println("Inform worker " + workerIpWithPort + " to prepare for shutdown!");
    ManagedChannel channel = createChannel(workerIpWithPort);
    WorkerControlGrpc.WorkerControlBlockingStub workerControlBlockingStub =
        WorkerControlGrpc.newBlockingStub(channel);
    workerControlBlockingStub.prepareWorkerForGracefulShutdown(
        PrepareWorkerForGracefulShutDownRequest.newBuilder().build());
    System.out.println("Worker " + workerIpWithPort + " informed!");
  }

  /**
   * Example command: GracefulShutdown DisableProtection WorkerIp buildfarm_endpoint
   *
   * @param args
   */
  @SuppressWarnings({"JavaDoc", "ResultOfMethodCallIgnored"})
  private static void disableScaleInProtection(String[] args) {
    String instancePrivateIp = args[1];
    String bfEndpoint = args[2];
    System.out.println("Ready to disable scale in protection of " + instancePrivateIp);
    ManagedChannel channel = createChannel(bfEndpoint);
    AdminGrpc.AdminBlockingStub adminBlockingStub = AdminGrpc.newBlockingStub(channel);
    adminBlockingStub.disableScaleInProtection(
        DisableScaleInProtectionRequest.newBuilder().setInstanceName(instancePrivateIp).build());
    System.out.println("Request for " + instancePrivateIp + " sent");
  }

  public static void main(String[] args) {
    switch (args[0]) {
      case "ShutDown":
        shutDownGracefully(args);
        break;
      case "PrepareWorker":
        prepareWorkerForShutDown(args);
        break;
      case "DisableProtection":
        disableScaleInProtection(args);
        break;
      default:
        System.out.println(
            "The action your choose is wrong. Please choose one from ShutDown, PrepareWorker, and"
                + " DisableProtection");
        break;
    }
  }
}
