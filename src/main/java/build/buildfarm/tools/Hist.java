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

package build.buildfarm.tools;

import static build.buildfarm.common.grpc.Channels.createChannel;

import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecutionStage;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.StubInstance;
import com.google.common.collect.ImmutableList;
import com.google.longrunning.Operation;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.ManagedChannel;

class Hist {
  @SuppressWarnings("ConstantConditions")
  private static void printHistogramValue(int executing) {
    StringBuilder s = new StringBuilder();
    int p = 0;
    int n = 5;
    while (p > 0) {
      p /= 10;
      n++;
    }
    int h = executing > 100 ? (100 - n) : executing;
    for (int i = 0; i < h; i++) {
      s.append('#');
    }
    if (executing > 100) {
      s.replace(0, n, "# (" + executing + ") ");
    }
    System.out.println(s.toString());
  }

  @SuppressWarnings("CatchMayIgnoreException")
  private static void printHistogram(Instance instance) {
    int executing = 0;

    String pageToken = "";
    do {
      ImmutableList.Builder<Operation> operations = new ImmutableList.Builder<>();
      for (; ; ) {
        try {
          pageToken = instance.listOperations(1024, pageToken, "", operations);
        } catch (io.grpc.StatusRuntimeException e) {
          continue;
        }
        break;
      }
      for (Operation operation : operations.build()) {
        try {
          ExecuteOperationMetadata metadata =
              operation.getMetadata().unpack(ExecuteOperationMetadata.class);
          if (metadata.getStage().equals(ExecutionStage.Value.EXECUTING)) {
            executing++;
          }
        } catch (InvalidProtocolBufferException e) {
        }
      }
    } while (!pageToken.equals(""));
    printHistogramValue(executing);
  }

  @SuppressWarnings("BusyWait")
  public static void main(String[] args) throws Exception {
    String instanceName = args[0];
    String host = args[1];
    DigestUtil digestUtil = DigestUtil.forHash(args[2]);
    ManagedChannel channel = createChannel(host);
    Instance instance = new StubInstance(instanceName, digestUtil, channel);
    try {
      //noinspection InfiniteLoopStatement
      for (; ; ) {
        printHistogram(instance);
        Thread.sleep(500);
      }
    } catch (InterruptedException e) {
      instance.stop();
    }
  }
}
