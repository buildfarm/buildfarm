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

package build.buildfarm.worker.shard;

import build.buildfarm.cas.cfc.CASFileCache;
import build.buildfarm.v1test.StageInformation;
import build.buildfarm.v1test.WorkerProfileGrpc;
import build.buildfarm.v1test.WorkerProfileMessage;
import build.buildfarm.v1test.WorkerProfileRequest;
import build.buildfarm.worker.PipelineStage;
import build.buildfarm.worker.PutOperationStage;
import build.buildfarm.worker.PutOperationStage.OperationStageDurations;
import build.buildfarm.worker.SuperscalarPipelineStage;
import com.google.common.base.Strings;
import io.grpc.stub.StreamObserver;
import javax.annotation.Nullable;

public class WorkerProfileService extends WorkerProfileGrpc.WorkerProfileImplBase {
  private final String name;
  private final String endpoint;
  private final @Nullable CASFileCache storage;
  private final @Nullable PipelineStage matchStage;
  private final @Nullable SuperscalarPipelineStage inputFetchStage;
  private final @Nullable SuperscalarPipelineStage executeActionStage;
  private final @Nullable SuperscalarPipelineStage reportResultStage;
  private final @Nullable PutOperationStage completeStage;

  public WorkerProfileService(
      String name,
      String endpoint,
      @Nullable CASFileCache storage,
      @Nullable PipelineStage matchStage,
      @Nullable SuperscalarPipelineStage inputFetchStage,
      @Nullable SuperscalarPipelineStage executeActionStage,
      @Nullable SuperscalarPipelineStage reportResultStage,
      @Nullable PutOperationStage completeStage) {
    this.name = name;
    this.endpoint = endpoint;
    this.storage = storage;
    this.matchStage = matchStage;
    this.inputFetchStage = inputFetchStage;
    this.executeActionStage = executeActionStage;
    this.reportResultStage = reportResultStage;
    this.completeStage = (PutOperationStage) completeStage;
  }

  private StageInformation unaryStageInformation(String name, @Nullable String operationName) {
    StageInformation.Builder builder =
        StageInformation.newBuilder().setName(name).setSlotsConfigured(1);
    if (!Strings.isNullOrEmpty(operationName)) {
      builder.setSlotsUsed(1).addOperationNames(operationName);
    }
    return builder.build();
  }

  private StageInformation superscalarStageInformation(SuperscalarPipelineStage stage) {
    return StageInformation.newBuilder()
        .setName(stage.getName())
        .setSlotsConfigured(stage.getWidth())
        .setSlotsUsed(stage.getSlotUsage())
        .addAllOperationNames(stage.getOperationNames())
        .build();
  }

  @Override
  public void getWorkerProfile(
      WorkerProfileRequest request, StreamObserver<WorkerProfileMessage> responseObserver) {
    // get usage of CASFileCache
    WorkerProfileMessage.Builder replyBuilder =
        WorkerProfileMessage.newBuilder().setName(name).setEndpoint(endpoint);

    // FIXME deliver full local storage chain
    if (storage != null) {
      replyBuilder
          .setCasSize(storage.size())
          .setCasEntryCount(storage.entryCount())
          .setCasMaxSize(storage.maxSize())
          .setCasMaxEntrySize(storage.maxEntrySize())
          .setCasUnreferencedEntryCount(storage.unreferencedEntryCount())
          .setCasDirectoryEntryCount(storage.directoryStorageCount())
          .setCasEvictedEntryCount(storage.getEvictedCount())
          .setCasEvictedEntrySize(storage.getEvictedSize())
          .setCasReadOnly(storage.isReadOnly());
    }

    // get slots configured and used of superscalar stages
    // prefer reverse order to avoid double counting if possible
    // these stats are not consistent across their sampling and will
    // produce: slots that are not consistent with operations, operations
    // in multiple stages even in reverse due to claim progress
    // in short: this is for monitoring, not for guaranteed consistency checks
    if (matchStage != null
        && inputFetchStage != null
        && executeActionStage != null
        && reportResultStage != null) {
      String matchOperation = matchStage.getOperationName();
      replyBuilder
          .addStages(superscalarStageInformation(reportResultStage))
          .addStages(superscalarStageInformation(executeActionStage))
          .addStages(superscalarStageInformation(inputFetchStage))
          .addStages(unaryStageInformation(matchStage.getName(), matchOperation));
    }

    // get average time costs on each stage
    if (completeStage != null) {
      OperationStageDurations[] durations = completeStage.getAverageTimeCostPerStage();
      for (OperationStageDurations duration : durations) {
        replyBuilder
            .addTimesBuilder()
            .setQueuedToMatch(duration.queuedToMatch)
            .setMatchToInputFetchStart(duration.matchToInputFetchStart)
            .setInputFetchStartToComplete(duration.inputFetchStartToComplete)
            .setInputFetchCompleteToExecutionStart(duration.inputFetchCompleteToExecutionStart)
            .setExecutionStartToComplete(duration.executionStartToComplete)
            .setExecutionCompleteToOutputUploadStart(duration.executionCompleteToOutputUploadStart)
            .setOutputUploadStartToComplete(duration.outputUploadStartToComplete)
            .setOperationCount(duration.operationCount)
            .setPeriod(duration.period);
      }
    }
    responseObserver.onNext(replyBuilder.build());
    responseObserver.onCompleted();
  }
}
