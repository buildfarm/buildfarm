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
import build.buildfarm.v1test.BatchWorkerProfilesResponse;
import build.buildfarm.v1test.OperationTimesBetweenStages;
import build.buildfarm.v1test.StageInformation;
import build.buildfarm.v1test.WorkerProfileMessage;
import com.google.protobuf.util.Durations;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.naming.ConfigurationException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(
    name = "worker-profile",
    mixinStandardHelpOptions = true,
    description = "Profile workers on the buildfarm server")
class WorkerProfile implements Callable<Integer> {
  @Parameters(index = "0", description = CliConstants.BUILDFARM_HOST)
  private String host;

  @Parameters(index = "1", description = CliConstants.INSTANCE_NAME)
  private String instanceName;

  @Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "Display this help message")
  private boolean helpRequested;

  private Instance instance;

  private void analyzeMessage(String worker, WorkerProfileMessage response) {
    System.out.println("\nWorkerProfile:");
    System.out.println(worker);
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
    System.out.printf(
        ("%-50s : %2.1f%%") + "%n",
        "Total size in Bytes",
        (100.0f * response.getCasSize() / response.getCasMaxSize()));

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
  private Set<String> getWorkers() throws ConfigurationException, IOException {
    return instance.backplaneStatus().getActiveExecuteWorkersList().stream()
        .collect(Collectors.toUnmodifiableSet());
  }

  @SuppressWarnings("BusyWait")
  private void workerProfile() throws IOException, ExecutionException, InterruptedException {
    Set<String> workers = null;

    //noinspection InfiniteLoopStatement
    while (true) {
      // update worker list
      if (workers == null) {
        try {
          workers = getWorkers();
        } catch (ConfigurationException e) {
          e.printStackTrace();
        }
      }
      if (workers == null || workers.isEmpty()) {
        System.out.println(
            "cannot find any workers, make sure there are workers in the" + " cluster");
      } else {
        BatchWorkerProfilesResponse responses = instance.batchWorkerProfiles(workers).get();
        // add new registered workers and profile
        for (BatchWorkerProfilesResponse.Response worker : responses.getResponsesList()) {
          try {
            analyzeMessage(worker.getWorkerName(), worker.getProfile());
          } catch (Exception e) {
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

  @Override
  public Integer call() throws Exception {
    instance =
        new StubInstance(
            instanceName, "bf-workerprofile", createChannel(host), Durations.fromMinutes(1));
    workerProfile();
    return 0;
  }

  public static void main(String[] args) {
    int exitCode = new CommandLine(new WorkerProfile()).execute(args);
    System.exit(exitCode);
  }
}
