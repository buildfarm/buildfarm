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

import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.v1test.OperationTimesBetweenStages;
import build.buildfarm.v1test.StageInformation;
import build.buildfarm.v1test.WorkerProfileMessage;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.util.Durations;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import javax.naming.ConfigurationException;

class WorkerProfile {

  private static void analyzeMessage(String worker, WorkerProfileMessage response) {
    System.out.println("\nWorkerProfile: " + worker);
    String strIntFormat = "%-50s : %d";
    long entryCount = response.getCasEntryCount();
    long unreferencedEntryCount = response.getCasUnreferencedEntryCount();
    System.out.printf((strIntFormat) + "%n", "Current Total Entry Count", entryCount);
    System.out.printf(
        (strIntFormat) + "%n", "Current Unreferenced Entry Count", unreferencedEntryCount);
    if (entryCount != 0) {
      System.out.println(
          String.format(
              "%-50s : %2.1f%%",
              "Percentage of Unreferenced Entry", (100.0f * unreferencedEntryCount) / entryCount));
    }
    System.out.printf(
        (strIntFormat) + "%n",
        "Current DirectoryEntry Count",
        response.getCasDirectoryEntryCount());
    System.out.printf(
        (strIntFormat) + "%n", "Number of Evicted Entries", response.getCasEvictedEntryCount());
    System.out.printf(
        (strIntFormat) + "%n",
        "Total Evicted Entries size in Bytes",
        response.getCasEvictedEntrySize());

    List<StageInformation> stages = response.getStagesList();
    for (StageInformation stage : stages) {
      WorkerProfilePrinter.printStageInformation(stage);
    }

    List<OperationTimesBetweenStages> times = response.getTimesList();
    for (OperationTimesBetweenStages time : times) {
      WorkerProfilePrinter.printOperationTime(time);
    }
  }

  @SuppressWarnings("ConstantConditions")
  private static Set<String> getWorkers(Instance instance)
      throws ConfigurationException, IOException {
    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    builder.addAll(instance.getWorkerList().getWorkersList());
    return builder.build();
  }

  @SuppressWarnings("BusyWait")
  private static void workerProfile(String[] args) throws IOException {
    Set<String> workers = null;
    WorkerProfileMessage currentWorkerMessage;
    String serverUri = args[0];
    String instanceName = args[1];

    Instance serverInstance =
        new StubInstance(
            instanceName, "bf-workerprofile", createChannel(serverUri), Durations.fromMinutes(1));

    //noinspection InfiniteLoopStatement
    while (true) {
      // update worker list
      if (workers == null) {
        try {
          workers = getWorkers(serverInstance);
        } catch (ConfigurationException e) {
          e.printStackTrace();
        }
      }
      if (workers == null || workers.isEmpty()) {
        System.out.println("cannot find any workers");
      } else {
        // add new registered workers and profile
        for (String worker : workers) {
          try {
            currentWorkerMessage = serverInstance.getWorkerProfile(worker);
            analyzeMessage(worker, currentWorkerMessage);
          } catch (StatusRuntimeException e) {
            e.printStackTrace();
            System.out.println("==============TIMEOUT");
          }
        }
      }

      // sleep
      try {
        System.out.println("Waiting for 10 minutes:");
        Thread.sleep(10 * 60 * 1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  // how to run the binary:
  // bazel run //src/main/java/build/buildfarm/tools:bf-workerprofile grpc://localhost:8980 shard
  // SHA256
  public static void main(String[] args) throws Exception {
    workerProfile(args);
  }
}
