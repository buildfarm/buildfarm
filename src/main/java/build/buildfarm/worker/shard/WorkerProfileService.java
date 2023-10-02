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

package build.buildfarm.worker.shard;

import build.buildfarm.backplane.Backplane;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.cfc.CASFileCache;
import build.buildfarm.v1test.OperationTimesBetweenStages;
import build.buildfarm.v1test.StageInformation;
import build.buildfarm.v1test.WorkerListMessage;
import build.buildfarm.v1test.WorkerListRequest;
import build.buildfarm.v1test.WorkerProfileGrpc;
import build.buildfarm.v1test.WorkerProfileMessage;
import build.buildfarm.v1test.WorkerProfileRequest;
import build.buildfarm.worker.ExecuteActionStage;
import build.buildfarm.worker.InputFetchStage;
import build.buildfarm.worker.PipelineStage;
import build.buildfarm.worker.PutOperationStage;
import build.buildfarm.worker.PutOperationStage.OperationStageDurations;
import build.buildfarm.worker.WorkerContext;
import io.grpc.stub.StreamObserver;
import java.io.IOException;

public class WorkerProfileService extends WorkerProfileGrpc.WorkerProfileImplBase {
  private final CASFileCache storage;
  private final InputFetchStage inputFetchStage;
  private final ExecuteActionStage executeActionStage;
  private final WorkerContext context;
  private final PutOperationStage completeStage;
  private final Backplane backplane;

  public WorkerProfileService(
      ContentAddressableStorage storage,
      PipelineStage inputFetchStage,
      PipelineStage executeActionStage,
      WorkerContext context,
      PipelineStage completeStage,
      Backplane backplane) {
    this.storage = (CASFileCache) storage;
    this.inputFetchStage = (InputFetchStage) inputFetchStage;
    this.executeActionStage = (ExecuteActionStage) executeActionStage;
    this.context = context;
    this.completeStage = (PutOperationStage) completeStage;
    this.backplane = backplane;
  }

  @Override
  public void getWorkerProfile(
      WorkerProfileRequest request, StreamObserver<WorkerProfileMessage> responseObserver) {
    // get usage of CASFileCache
    WorkerProfileMessage.Builder replyBuilder =
        WorkerProfileMessage.newBuilder()
            .setCasSize(storage.size())
            .setCasEntryCount(storage.entryCount())
            .setCasMaxSize(storage.maxSize())
            .setCasMaxEntrySize(storage.maxEntrySize())
            .setCasUnreferencedEntryCount(storage.unreferencedEntryCount())
            .setCasDirectoryEntryCount(storage.directoryStorageCount())
            .setCasEvictedEntryCount(storage.getEvictedCount())
            .setCasEvictedEntrySize(storage.getEvictedSize());

    // get slots configured and used of superscalar stages
    replyBuilder
        .addStages(
            StageInformation.newBuilder()
                .setName("InputFetchStage")
                .setSlotsConfigured(context.getInputFetchStageWidth())
                .setSlotsUsed(inputFetchStage.getSlotUsage())
                .build())
        .addStages(
            StageInformation.newBuilder()
                .setName("ExecuteActionStage")
                .setSlotsConfigured(context.getExecuteStageWidth())
                .setSlotsUsed(executeActionStage.getSlotUsage())
                .build());

    // get average time costs on each stage
    OperationStageDurations[] durations = completeStage.getAverageTimeCostPerStage();
    for (OperationStageDurations duration : durations) {
      OperationTimesBetweenStages.Builder timesBuilder = OperationTimesBetweenStages.newBuilder();
      timesBuilder
          .setQueuedToMatch(duration.queuedToMatch)
          .setMatchToInputFetchStart(duration.matchToInputFetchStart)
          .setInputFetchStartToComplete(duration.inputFetchStartToComplete)
          .setInputFetchCompleteToExecutionStart(duration.inputFetchCompleteToExecutionStart)
          .setExecutionStartToComplete(duration.executionStartToComplete)
          .setExecutionCompleteToOutputUploadStart(duration.executionCompleteToOutputUploadStart)
          .setOutputUploadStartToComplete(duration.outputUploadStartToComplete)
          .setOperationCount(duration.operationCount)
          .setPeriod(duration.period);
      replyBuilder.addTimes(timesBuilder.build());
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
