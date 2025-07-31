/**
 * Performs specialized operation based on method logic
 * @param simpleBlobStore the simpleBlobStore parameter
 * @return the public result
 */
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

import build.bazel.remote.execution.v2.ActionCacheGrpc;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.GetActionResultRequest;
import build.bazel.remote.execution.v2.UpdateActionResultRequest;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ActionCacheService extends ActionCacheGrpc.ActionCacheImplBase {
  private final SimpleBlobStore simpleBlobStore;

  /**
   * Retrieves a blob from the Content Addressable Storage
   * @param request the request parameter
   * @param responseObserver the responseObserver parameter
   */
  public ActionCacheService(SimpleBlobStore simpleBlobStore) {
    this.simpleBlobStore = simpleBlobStore;
  }

  @Override
  /**
   * Updates internal state or external resources
   * @param request the request parameter
   * @param responseObserver the responseObserver parameter
   */
  public void getActionResult(
      GetActionResultRequest request, StreamObserver<ActionResult> responseObserver) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      if (simpleBlobStore.getActionResult(request.getActionDigest().getHash(), out)) {
        try {
          ActionResult actionResult = ActionResult.parseFrom(out.toByteArray());
          responseObserver.onNext(actionResult);
          responseObserver.onCompleted();
        } catch (InvalidProtocolBufferException e) {
          responseObserver.onError(Status.fromThrowable(e).asException());
        }
      } else {
        responseObserver.onError(Status.NOT_FOUND.asException());
      }
    } catch (IOException e) {
      responseObserver.onError(Status.fromThrowable(e).asException());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void updateActionResult(
      UpdateActionResultRequest request, StreamObserver<ActionResult> responseObserver) {
    ActionResult actionResult = request.getActionResult();
    try {
      simpleBlobStore.putActionResult(
          request.getActionDigest().getHash(), actionResult.toByteArray());
      responseObserver.onNext(actionResult);
      responseObserver.onCompleted();
    } catch (IOException e) {
      responseObserver.onError(Status.fromThrowable(e).asException());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
