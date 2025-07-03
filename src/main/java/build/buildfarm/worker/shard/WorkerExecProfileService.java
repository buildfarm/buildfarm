package build.buildfarm.worker.shard;

import build.buildfarm.cas.cfc.CASFileCache;
import build.buildfarm.v1test.StageInformation;
import build.buildfarm.v1test.WorkerProfileMessage;
import build.buildfarm.v1test.WorkerProfileRequest;
import build.buildfarm.worker.PipelineStage;
import build.buildfarm.worker.PutOperationStage;
import build.buildfarm.worker.SuperscalarPipelineStage;
import com.google.common.base.Strings;
import io.grpc.stub.StreamObserver;
import javax.annotation.Nullable;

/**
 * A Worker profile service for a shard with execution functionality. <br>
 * It could be used with or without storage (null).
 *
 * @see WorkerProfileService for shards without execution functionality.
 */
public class WorkerExecProfileService extends WorkerProfileService {
  private final PipelineStage matchStage;
  private final SuperscalarPipelineStage inputFetchStage;
  private final SuperscalarPipelineStage executeActionStage;
  private final SuperscalarPipelineStage reportResultStage;
  private final PutOperationStage completeStage;

  public WorkerExecProfileService(
      @Nullable CASFileCache storage,
      PipelineStage matchStage,
      SuperscalarPipelineStage inputFetchStage,
      SuperscalarPipelineStage executeActionStage,
      SuperscalarPipelineStage reportResultStage,
      PutOperationStage completeStage) {
    super(storage);
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
    WorkerProfileMessage.Builder replyBuilder = WorkerProfileMessage.newBuilder();
    reportStorageUsage(replyBuilder);
    // get slots configured and used of superscalar stages
    // prefer reverse order to avoid double counting if possible
    // these stats are not consistent across their sampling and will
    // produce: slots that are not consistent with operations, operations
    // in multiple stages even in reverse due to claim progress
    // in short: this is for monitoring, not for guaranteed consistency checks
    replyBuilder.addStages(superscalarStageInformation(reportResultStage));
    replyBuilder.addStages(superscalarStageInformation(executeActionStage));
    replyBuilder.addStages(superscalarStageInformation(inputFetchStage));
    replyBuilder.addStages(
        unaryStageInformation(matchStage.getName(), matchStage.getOperationName()));
    // get average time costs on each stage
    PutOperationStage.OperationStageDurations[] durations =
        completeStage.getAverageTimeCostPerStage();
    for (PutOperationStage.OperationStageDurations duration : durations) {
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
}
