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

import build.buildfarm.common.admin.Admin;
import build.buildfarm.common.admin.aws.AwsAdmin;
import build.buildfarm.common.admin.gcp.GcpAdmin;
import build.buildfarm.v1test.AdminConfig;
import build.buildfarm.v1test.AdminGrpc;
import build.buildfarm.v1test.StopContainerRequest;
import build.buildfarm.v1test.TerminateHostRequest;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.logging.Logger;

public class AdminService extends AdminGrpc.AdminImplBase {
  private static final Logger logger = Logger.getLogger(AdminService.class.getName());

  private final Admin adminController;

  public AdminService(AdminConfig config) {
    this.adminController = getAdminController(config);
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
      logger.severe("Could not terminate host." + e);
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
      logger.severe("Could not stop container." + e);
      responseObserver.onError(io.grpc.Status.fromThrowable(e).asException());
    }
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
