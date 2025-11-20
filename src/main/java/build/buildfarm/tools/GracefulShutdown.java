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
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

/** Graceful shutdown operations for buildfarm workers. */
@Command(
    name = "gracefulshutdown",
    mixinStandardHelpOptions = true,
    description = "Graceful shutdown operations for buildfarm workers",
    subcommands = {
      GracefulShutdown.ShutDown.class,
      GracefulShutdown.PrepareWorker.class,
      GracefulShutdown.DisableProtection.class
    })
class GracefulShutdown implements Callable<Integer> {

  @Override
  public Integer call() {
    CommandLine.usage(this, System.out);
    return 0;
  }

  /** Shut down a worker gracefully. */
  @Command(name = "ShutDown", description = "Shut down a worker gracefully")
  static class ShutDown implements Callable<Integer> {
    @Parameters(index = "0", description = "Worker name or IP")
    private String workerName;

    @Parameters(index = "1", description = "Buildfarm endpoint [scheme://]host:port")
    private String bfEndpoint;

    @Override
    public Integer call() {
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
      return 0;
    }
  }

  /** Prepare a worker for shutdown. */
  @Command(name = "PrepareWorker", description = "Prepare a worker for shutdown")
  static class PrepareWorker implements Callable<Integer> {
    @Parameters(index = "0", description = "Worker IP with port (e.g., WorkerIp:port)")
    private String workerIpWithPort;

    @Override
    public Integer call() {
      System.out.println("Inform worker " + workerIpWithPort + " to prepare for shutdown!");
      ManagedChannel channel = createChannel(workerIpWithPort);
      WorkerControlGrpc.WorkerControlBlockingStub workerControlBlockingStub =
          WorkerControlGrpc.newBlockingStub(channel);
      workerControlBlockingStub.prepareWorkerForGracefulShutdown(
          PrepareWorkerForGracefulShutDownRequest.newBuilder().build());
      System.out.println("Worker " + workerIpWithPort + " informed!");
      return 0;
    }
  }

  /** Disable scale-in protection. */
  @Command(name = "DisableProtection", description = "Disable scale-in protection for an instance")
  static class DisableProtection implements Callable<Integer> {
    @Parameters(index = "0", description = "Instance private IP")
    private String instancePrivateIp;

    @Parameters(index = "1", description = "Buildfarm endpoint [scheme://]host:port")
    private String bfEndpoint;

    @Override
    public Integer call() {
      System.out.println("Ready to disable scale in protection of " + instancePrivateIp);
      ManagedChannel channel = createChannel(bfEndpoint);
      AdminGrpc.AdminBlockingStub adminBlockingStub = AdminGrpc.newBlockingStub(channel);
      adminBlockingStub.disableScaleInProtection(
          DisableScaleInProtectionRequest.newBuilder().setInstanceName(instancePrivateIp).build());
      System.out.println("Request for " + instancePrivateIp + " sent");
      return 0;
    }
  }

  public static void main(String[] args) {
    int exitCode = new CommandLine(new GracefulShutdown()).execute(args);
    System.exit(exitCode);
  }
}
