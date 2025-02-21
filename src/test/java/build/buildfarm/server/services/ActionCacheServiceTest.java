// Copyright 2022 The Buildfarm Authors. All rights reserved.
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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.UpdateActionResultRequest;
import build.buildfarm.instance.Instance;
import io.grpc.stub.StreamObserver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ActionCacheServiceTest {
  @Test
  public void writeFailsWhenActionCacheIsReadOnly() throws Exception {
    // If the ActionCache is configured to be read-only,
    // then attempting to write to it should result in an error.

    // ARRANGE
    Instance instance = mock(Instance.class);
    ActionCacheService service = new ActionCacheService(instance, /* isWritable= */ false);

    // ACT
    StreamObserver<ActionResult> response = mock(StreamObserver.class);
    UpdateActionResultRequest request = UpdateActionResultRequest.newBuilder().build();
    service.updateActionResult(request, response);

    // ASSERT
    verify(response, times(1)).onError(any(Throwable.class));
  }

  @Test
  public void writeSucceedsWhenActionCacheIsWritable() throws Exception {
    // If the ActionCache is configured to be read and write,
    // then attempting to write to it should succeed.

    // ARRANGE
    Instance instance = mock(Instance.class);
    ActionCacheService service = new ActionCacheService(instance, /* isWritable= */ true);

    // ACT
    StreamObserver<ActionResult> response = mock(StreamObserver.class);
    UpdateActionResultRequest request =
        UpdateActionResultRequest.newBuilder()
            .setDigestFunction(DigestFunction.Value.SHA256)
            .build();
    service.updateActionResult(request, response);

    // ASSERT
    verify(response, times(0)).onError(any(Throwable.class));
  }
}
