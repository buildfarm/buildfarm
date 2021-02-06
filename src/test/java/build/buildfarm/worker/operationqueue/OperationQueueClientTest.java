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

package build.buildfarm.worker.operationqueue;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.Platform.Property;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.MatchListener;
import build.buildfarm.v1test.ExecutionPolicy;
import build.buildfarm.v1test.QueueEntry;
import com.google.common.collect.ImmutableList;
import javax.annotation.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@RunWith(JUnit4.class)
public class OperationQueueClientTest {
  @Test
  public void matchPlatformContainsExecutionPolicies() throws InterruptedException {
    Instance instance = mock(Instance.class);
    doAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocation) throws InterruptedException {
                MatchListener listener = (MatchListener) invocation.getArguments()[1];
                listener.onEntry(null);
                return null;
              }
            })
        .when(instance)
        .match(any(Platform.class), any(MatchListener.class));
    OperationQueueClient client =
        new OperationQueueClient(
            instance,
            Platform.getDefaultInstance(),
            ImmutableList.of(ExecutionPolicy.newBuilder().setName("foo").build()));
    MatchListener listener =
        new MatchListener() {
          @Override
          public void onWaitStart() {}

          @Override
          public void onWaitEnd() {}

          @Override
          public boolean onEntry(@Nullable QueueEntry queueEntry) {
            return true;
          }

          @Override
          public void onError(Throwable t) {
            t.printStackTrace();
          }

          @Override
          public void setOnCancelHandler(Runnable onCancelHandler) {}
        };
    client.match(listener);
    Platform matchPlatform =
        Platform.newBuilder()
            .addProperties(
                Property.newBuilder().setName("execution-policy").setValue("foo").build())
            .build();
    verify(instance, times(1)).match(eq(matchPlatform), any(MatchListener.class));
  }
}
