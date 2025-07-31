/**
 * Performs specialized operation based on method logic
 * @param instance the instance parameter
 * @return the public result
 */
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

package build.buildfarm.server.services;

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import build.buildfarm.instance.Instance;
import build.buildfarm.v1test.BatchWorkerProfilesRequest;
import build.buildfarm.v1test.BatchWorkerProfilesResponse;
import build.buildfarm.v1test.WorkerProfileGrpc;
import build.buildfarm.v1test.WorkerProfileMessage;
import build.buildfarm.v1test.WorkerProfileRequest;
import com.google.common.util.concurrent.FutureCallback;
import io.grpc.stub.StreamObserver;

public class WorkerProfileService extends WorkerProfileGrpc.WorkerProfileImplBase {
  private final Instance instance;

  /**
   * Retrieves a blob from the Content Addressable Storage Executes asynchronously and returns a future for completion tracking.
   * @param request the request parameter
   * @param responseObserver the responseObserver parameter
   */
  public WorkerProfileService(Instance instance) {
    this.instance = instance;
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param profile the profile parameter
   */
  public void getWorkerProfile(
      WorkerProfileRequest request, StreamObserver<WorkerProfileMessage> responseObserver) {
    addCallback(
        instance.getWorkerProfile(request.getWorkerName()),
        new FutureCallback<>() {
          @Override
          /**
           * Performs specialized operation based on method logic
           * @param t the t parameter
           */
          public void onSuccess(WorkerProfileMessage profile) {
            responseObserver.onNext(profile);
            responseObserver.onCompleted();
          }

          @Override
          /**
           * Performs specialized operation based on method logic Executes asynchronously and returns a future for completion tracking.
           * @param request the request parameter
           * @param responseObserver the responseObserver parameter
           */
          public void onFailure(Throwable t) {
            responseObserver.onError(t);
          }
        },
        directExecutor());
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param response the response parameter
   */
  public void batchWorkerProfiles(
      BatchWorkerProfilesRequest request,
      StreamObserver<BatchWorkerProfilesResponse> responseObserver) {
    addCallback(
        instance.batchWorkerProfiles(request.getWorkerNamesList()),
        new FutureCallback<>() {
          @Override
          /**
           * Performs specialized operation based on method logic
           * @param t the t parameter
           */
          public void onSuccess(BatchWorkerProfilesResponse response) {
            responseObserver.onNext(response);
            responseObserver.onCompleted();
          }

          @Override
          public void onFailure(Throwable t) {
            responseObserver.onError(t);
          }
        },
        directExecutor());
  }
}
