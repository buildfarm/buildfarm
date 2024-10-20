// Copyright 2021 The Buildfarm Authors. All rights reserved.
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

import static java.lang.String.format;

import build.buildfarm.instance.Instance;
import build.buildfarm.v1test.OperationTimesBetweenStages;
import build.buildfarm.v1test.StageInformation;
import build.buildfarm.v1test.WorkerProfileMessage;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import java.util.List;

class WorkerProfilePrinter {
  public static void getWorkerProfile(Instance instance) {
    WorkerProfileMessage response = instance.getWorkerProfile();
    System.out.println("\nWorkerProfile:");
    String strIntFormat = "%-50s : %d";
    String strFloatFormat = "%-50s : %2.1f";
    long entryCount = response.getCasEntryCount();
    long unreferencedEntryCount = response.getCasUnreferencedEntryCount();
    System.out.printf((strIntFormat) + "%n", "Current Total Entry Count", entryCount);
    System.out.printf((strIntFormat) + "%n", "Current Total Size", response.getCasSize());
    System.out.printf((strIntFormat) + "%n", "Max Size", response.getCasMaxSize());
    System.out.printf((strIntFormat) + "%n", "Max Entry Size", response.getCasMaxEntrySize());
    System.out.printf(
        (strIntFormat) + "%n", "Current Unreferenced Entry Count", unreferencedEntryCount);
    if (entryCount != 0) {
      System.out.println(
          format(
                  strFloatFormat,
                  "Percentage of Unreferenced Entry",
                  100.0f * response.getCasEntryCount() / response.getCasUnreferencedEntryCount())
              + "%");
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
      printStageInformation(stage);
    }

    List<OperationTimesBetweenStages> times = response.getTimesList();
    for (OperationTimesBetweenStages time : times) {
      printOperationTime(time);
    }
  }

  public static void printStageInformation(StageInformation stage) {
    System.out.printf("%s slots configured: %d%n", stage.getName(), stage.getSlotsConfigured());
    System.out.printf("%s slots used %d%n", stage.getName(), stage.getSlotsUsed());
  }

  public static void printOperationTime(OperationTimesBetweenStages time) {
    String periodInfo = "\nIn last ";
    switch ((int) time.getPeriod().getSeconds()) {
      case 60:
        periodInfo += "1 minute";
        break;
      case 600:
        periodInfo += "10 minutes";
        break;
      case 3600:
        periodInfo += "1 hour";
        break;
      case 10800:
        periodInfo += "3 hours";
        break;
      case 86400:
        periodInfo += "24 hours";
        break;
      default:
        System.out.println("The period is UNKNOWN: " + time.getPeriod().getSeconds());
        periodInfo = periodInfo + time.getPeriod().getSeconds() + " seconds";
        break;
    }

    periodInfo += ":";
    System.out.println(periodInfo);
    System.out.println("Number of operations completed: " + time.getOperationCount());
    String strStrNumFormat = "%-28s -> %-28s : %12.2f ms";
    System.out.printf(
        (strStrNumFormat) + "%n",
        "Queued",
        "MatchStage",
        durationToMillis(time.getQueuedToMatch()));
    System.out.printf(
        (strStrNumFormat) + "%n",
        "MatchStage",
        "InputFetchStage start",
        durationToMillis(time.getMatchToInputFetchStart()));
    System.out.printf(
        (strStrNumFormat) + "%n",
        "InputFetchStage Start",
        "InputFetchStage Complete",
        durationToMillis(time.getInputFetchStartToComplete()));
    System.out.printf(
        (strStrNumFormat) + "%n",
        "InputFetchStage Complete",
        "ExecutionStage Start",
        durationToMillis(time.getInputFetchCompleteToExecutionStart()));
    System.out.printf(
        (strStrNumFormat) + "%n",
        "ExecutionStage Start",
        "ExecutionStage Complete",
        durationToMillis(time.getExecutionStartToComplete()));
    System.out.printf(
        (strStrNumFormat) + "%n",
        "ExecutionStage Complete",
        "ReportResultStage Start",
        durationToMillis(time.getExecutionCompleteToOutputUploadStart()));
    System.out.printf(
        (strStrNumFormat) + "%n",
        "OutputUploadStage Start",
        "OutputUploadStage Complete",
        durationToMillis(time.getOutputUploadStartToComplete()));
    System.out.println();
  }

  private static float durationToMillis(Duration d) {
    return Durations.toNanos(d) / (1000.0f * 1000.0f);
  }
}
