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

import build.buildfarm.backplane.Backplane;
import build.buildfarm.cas.cfc.CASFileCache;
import build.buildfarm.v1test.StageInformation;
import build.buildfarm.v1test.WorkerListMessage;
import build.buildfarm.v1test.WorkerListRequest;
import build.buildfarm.v1test.WorkerProfileGrpc;
import build.buildfarm.v1test.WorkerProfileMessage;
import build.buildfarm.v1test.WorkerProfileRequest;
import build.buildfarm.worker.PipelineStage;
import build.buildfarm.worker.PutOperationStage;
import build.buildfarm.worker.PutOperationStage.OperationStageDurations;
import build.buildfarm.worker.SuperscalarPipelineStage;
import com.google.common.base.Strings;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import javax.annotation.Nullable;

public class WorkerProfileService extends WorkerProfileGrpc.WorkerProfileImplBase {
  private final @Nullable CASFileCache storage;
  private final PipelineStage matchStage;
  private final SuperscalarPipelineStage inputFetchStage;
  private final SuperscalarPipelineStage executeActionStage;
  private final SuperscalarPipelineStage reportResultStage;
  private final PutOperationStage completeStage;
  private final Backplane backplane;

  public WorkerProfileService(
      @Nullable CASFileCache storage,
      PipelineStage matchStage,
      SuperscalarPipelineStage inputFetchStage,
      SuperscalarPipelineStage executeActionStage,
      SuperscalarPipelineStage reportResultStage,
      PutOperationStage completeStage,
      Backplane backplane) {
    super();
    this.storage = storage;
    this.matchStage = matchStage;
    this.inputFetchStage = inputFetchStage;
    this.executeActionStage = executeActionStage;
    this.reportResultStage = reportResultStage;
    this.completeStage = (PutOperationStage) completeStage;
    this.backplane = backplane;
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
    WorkerProfileMessage.Builder replyBuilder = WorkerProfileMessage.newBuilder();

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
          .setCasEvictedEntrySize(storage.getEvictedSize());
    }

    // get slots configured and used of superscalar stages
    // prefer reverse order to avoid double counting if possible
    // these stats are not consistent across their sampling and will
    // produce: slots that are not consistent with operations, operations
    // in multiple stages even in reverse due to claim progress
    // in short: this is for monitoring, not for guaranteed consistency checks
    String matchOperation = matchStage.getOperationName();
    replyBuilder
        .addStages(superscalarStageInformation(reportResultStage))
        .addStages(superscalarStageInformation(executeActionStage))
        .addStages(superscalarStageInformation(inputFetchStage))
        .addStages(unaryStageInformation(matchStage.getName(), matchOperation));

    // get average time costs on each stage
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
    responseObserver.onNext(replyBuilder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void getWorkerList(
      WorkerListRequest request, StreamObserver<WorkerListMessage> responseObserver) {
    WorkerListMessage.Builder replyBuilder = WorkerListMessage.newBuilder();
    try {
      replyBuilder.addAllWorkers(backplane.getStorageWorkers());
    } catch (IOException e) {
      responseObserver.onError(e);
    }
    responseObserver.onNext(replyBuilder.build());
    responseObserver.onCompleted();
  }
}
