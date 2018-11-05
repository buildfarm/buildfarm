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

import build.buildfarm.common.function.InterruptingConsumer;
import build.buildfarm.v1test.WorkerConfig;
import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.logging.Logger;
import javax.naming.ConfigurationException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MatchStageTest {
  static class PipelineSink extends PipelineStage {
    private static final Logger logger = Logger.getLogger(PipelineSink.class.getName());

    private final Predicate<OperationContext> onPutShouldClose;

    PipelineSink(Predicate<OperationContext> onPutShouldClose) {
      super("Sink", null, null, null);
      this.onPutShouldClose = onPutShouldClose;
    }

    @Override
    public Logger getLogger() {
      return logger;
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

    WorkerContext workerContext = new StubWorkerContext() {
      @Override
      public void match(InterruptingConsumer<Operation> onMatch) throws InterruptedException {
        onMatch.accept(queue.remove(0));
      }

      @Override
      public void requeue(Operation operation) {
        assertThat(operation.getName()).isEqualTo("bad");
      }

      @Override
      public ByteString getBlob(Digest digest) {
        if (digest.getSizeBytes() == 0) {
          return ByteString.EMPTY;
        }
        if (digest.getHash().equals("action")) {
          return Action.newBuilder().build().toByteString();
        }
        return super.getBlob(digest);
      }

      @Override
      public Path getRoot() {
        return FileSystems.getDefault().getPath("tmp");
      }
    };

    queue.add(Operation.newBuilder()
        .setName("bad")
        // inspire InvalidProtocolBufferException from operation metadata unpack
        .build());

    queue.add(Operation.newBuilder()
        .setName("good")
        .setMetadata(Any.pack(ExecuteOperationMetadata.newBuilder()
            .setActionDigest(Digest.newBuilder().setHash("action").build())
            .build()))
        .build());

    final PipelineStage output = new PipelineSink((operationContext) -> operationContext.operation.getName().equals("good"));

    PipelineStage matchStage = new MatchStage(workerContext, output, null);
    matchStage.run();
  }
}
