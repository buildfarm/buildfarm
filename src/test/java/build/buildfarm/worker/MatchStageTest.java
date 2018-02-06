// Copyright 2018 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker;

import static com.google.common.truth.Truth.assertThat;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.v1test.QueuedOperationMetadata;
import build.buildfarm.v1test.WorkerConfig;
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.Command;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import javax.naming.ConfigurationException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MatchStageTest {
  class PipelineSink extends PipelineStage {
    Predicate<OperationContext> onPutShouldClose;

    PipelineSink(Predicate<OperationContext> onPutShouldClose) {
      super("PipelineSink", null, null, null);
      this.onPutShouldClose = onPutShouldClose;
    }

    @Override
    public void put(OperationContext operationContext) {
      if (onPutShouldClose.test(operationContext)) {
        close();
      }
    }

    @Override
    public OperationContext take() {
      throw new UnsupportedOperationException();
    }
  }

  @Test
  public void fetchFailureReentry() throws ConfigurationException {
    List<Operation> queue = new ArrayList<>();
    List<Boolean> results = new ArrayList<>();

    WorkerContext workerContext = new StubWorkerContext() {
      @Override
      public DigestUtil getDigestUtil() {
        return null;
      }

      @Override
      public void match(Predicate<Operation> onMatch) {
        assertThat(onMatch.test(queue.remove(0))).isEqualTo(results.remove(0));
      }

      @Override
      public ByteString getBlob(Digest digest) {
        if (digest.getHash().equals("action")) {
          return Action.newBuilder().build().toByteString();
        }
        return super.getBlob(digest);
      }

      @Override
      public Path getRoot() {
        return FileSystems.getDefault().getPath("tmp");
      }

      @Override
      public void requeue(Operation operation) {
        // ignore
      }
    };

    queue.add(Operation.newBuilder()
        .setName("bad")
        // inspire InvalidProtocolBufferException from operation metadata unpack
        .build());
    results.add(false);

    queue.add(Operation.newBuilder()
        .setName("good")
        .setMetadata(Any.pack(QueuedOperationMetadata.newBuilder()
            .setAction(Action.getDefaultInstance())
            .setCommand(Command.newBuilder()
                .addArguments("/bin/true")
                .build())
            .setExecuteOperationMetadata(ExecuteOperationMetadata.newBuilder()
                .setActionDigest(Digest.newBuilder().setHash("action").build())
                .build())
            .build()))
        .build());
    results.add(true);

    final PipelineStage output = new PipelineSink((operationContext) -> operationContext.operation.getName().equals("good"));

    PipelineStage matchStage = new MatchStage(workerContext, output, null);
    matchStage.run();
  }
}
