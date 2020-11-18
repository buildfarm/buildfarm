// Copyright 2019 The Bazel Authors. All rights reserved.
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

package build.buildfarm.ac;

import static com.google.common.truth.Truth.assertThat;

import build.bazel.remote.execution.v2.ActionCacheGrpc.ActionCacheImplBase;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.GetActionResultRequest;
import build.bazel.remote.execution.v2.UpdateActionResultRequest;
import build.buildfarm.common.DigestUtil;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GrpcActionCacheTest {
  private static final DigestUtil DIGEST_UTIL = new DigestUtil(DigestUtil.HashFunction.SHA256);

  private static final String instanceName = "test-instance";

  private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();
  private final String fakeServerName = "fake server for " + getClass();

  private ActionCache ac;

  private Server fakeServer;

  @Before
  public void setUp() throws IOException {
    fakeServer =
        InProcessServerBuilder.forName(fakeServerName)
            .fallbackHandlerRegistry(serviceRegistry)
            .directExecutor()
            .build()
            .start();

    ac =
        new GrpcActionCache(
            instanceName, InProcessChannelBuilder.forName(fakeServerName).directExecutor().build());
  }

  @After
  public void tearDown() throws InterruptedException {
    fakeServer.shutdownNow();
    fakeServer.awaitTermination();
  }

  @Test
  public void getSuppliesResponse() throws Exception {
    ActionResult result =
        ActionResult.newBuilder().setStdoutRaw(ByteString.copyFromUtf8("out")).build();
    Digest actionDigest = DIGEST_UTIL.compute(ByteString.copyFromUtf8("in"));
    serviceRegistry.addService(
        new ActionCacheImplBase() {
          @Override
          public void getActionResult(
              GetActionResultRequest request, StreamObserver<ActionResult> responseObserver) {
            if (request.getInstanceName().equals(instanceName)
                && request.getActionDigest().equals(actionDigest)) {
              responseObserver.onNext(result);
              responseObserver.onCompleted();
            } else {
              responseObserver.onError(Status.NOT_FOUND.asException());
            }
          }
        });
    assertThat(ac.get(DigestUtil.asActionKey(actionDigest)).get()).isEqualTo(result);
  }

  @Test
  public void getNotFoundIsNull() throws Exception {
    Digest actionDigest = DIGEST_UTIL.compute(ByteString.copyFromUtf8("not-found"));
    serviceRegistry.addService(
        new ActionCacheImplBase() {
          @Override
          public void getActionResult(
              GetActionResultRequest request, StreamObserver<ActionResult> responseObserver) {
            responseObserver.onError(Status.NOT_FOUND.asException());
          }
        });
    assertThat(ac.get(DigestUtil.asActionKey(actionDigest)).get()).isNull();
  }

  @Test
  public void putUpdatesResult() throws InterruptedException {
    ActionResult result =
        ActionResult.newBuilder().setStdoutRaw(ByteString.copyFromUtf8("out")).build();
    Digest actionDigest = DIGEST_UTIL.compute(ByteString.copyFromUtf8("in"));
    serviceRegistry.addService(
        new ActionCacheImplBase() {
          @Override
          public void updateActionResult(
              UpdateActionResultRequest request, StreamObserver<ActionResult> responseObserver) {
            if (request.getInstanceName().equals(instanceName)
                && request.getActionDigest().equals(actionDigest)
                && request.getActionResult().equals(result)) {
              responseObserver.onNext(result);
              responseObserver.onCompleted();
            } else {
              responseObserver.onError(Status.UNAVAILABLE.asException());
            }
          }
        });
    ac.put(DigestUtil.asActionKey(actionDigest), result);
  }
}
