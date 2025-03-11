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

package build.buildfarm.metrics;

import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.ExecutedActionMetadata;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.Time;
import build.buildfarm.v1test.OperationRequestMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.longrunning.Operation;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.rpc.Code;
import com.google.rpc.PreconditionFailure;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import java.util.logging.Level;
import lombok.extern.java.Log;

@Log
public abstract class AbstractMetricsPublisher implements MetricsPublisher {
  private static final Counter actionsCounter =
      Counter.build().name("actions").help("Number of actions.").register();
  private static final Counter operationsInStage =
      Counter.build()
          .name("operations_stage_load")
          .labelNames("stage_name")
          .help("Operations in stage.")
          .register();
  private static final Counter operationStatus =
      Counter.build()
          .name("operation_status")
          .labelNames("code")
          .help("Operation execution status.")
          .register();
  private static final Counter operationsPerWorker =
      Counter.build()
          .name("operation_worker")
          .labelNames("worker_name")
          .help("Operations per worker.")
          .register();

  private static final Counter operationExitCode =
      Counter.build()
          .name("operation_exit_code")
          .labelNames("exit_code")
          .help("Operation execution exit code.")
          .register();
  private static final Histogram queuedTime =
      Histogram.build().name("queued_time_s").help("Queued time in seconds.").register();
  private static final Histogram outputUploadTime =
      Histogram.build()
          .name("output_upload_time_s")
          .help("Output upload time in seconds.")
          .register();

  private final String clusterId;

  public AbstractMetricsPublisher(String clusterId) {
    this.clusterId = clusterId;
  }

  public AbstractMetricsPublisher() {
    this(/* clusterId= */ null);
  }

  @Override
  public void publishRequestMetadata(Operation operation, RequestMetadata requestMetadata) {
    throw new UnsupportedOperationException("Not Implemented.");
  }

  @Override
  public abstract void publishMetric(String metricName, Object metricValue);

  @VisibleForTesting
  protected OperationRequestMetadata populateRequestMetadata(
      Operation operation, RequestMetadata requestMetadata) {
    try {
      actionsCounter.inc();
      OperationRequestMetadata operationRequestMetadata =
          OperationRequestMetadata.newBuilder()
              .setRequestMetadata(requestMetadata)
              .setOperationName(operation.getName())
              .setDone(operation.getDone())
              .setClusterId(clusterId)
              .build();
      if (operation.getDone() && operation.getResponse().is(ExecuteResponse.class)) {
        operationRequestMetadata =
            operationRequestMetadata.toBuilder()
                .setExecuteResponse(operation.getResponse().unpack(ExecuteResponse.class))
                .build();
        operationStatus
            .labels(
                Code.forNumber(operationRequestMetadata.getExecuteResponse().getStatus().getCode())
                    .name())
            .inc();
        operationExitCode
            .labels(
                Integer.toString(
                    operationRequestMetadata.getExecuteResponse().getResult().getExitCode()))
            .inc();
        if (operationRequestMetadata.getExecuteResponse().hasResult()
            && operationRequestMetadata.getExecuteResponse().getResult().hasExecutionMetadata()) {
          ExecutedActionMetadata executionMetadata =
              operationRequestMetadata.getExecuteResponse().getResult().getExecutionMetadata();
          operationsPerWorker.labels(executionMetadata.getWorker()).inc();
          queuedTime.observe(
              Time.toDurationSeconds(
                  executionMetadata.getQueuedTimestamp(),
                  executionMetadata.getExecutionStartTimestamp()));
          outputUploadTime.observe(
              Time.toDurationSeconds(
                  executionMetadata.getOutputUploadStartTimestamp(),
                  executionMetadata.getOutputUploadCompletedTimestamp()));
        }
      }
      if (operation.getMetadata().is(ExecuteOperationMetadata.class)) {
        operationRequestMetadata =
            operationRequestMetadata.toBuilder()
                .setExecuteOperationMetadata(
                    operation.getMetadata().unpack(ExecuteOperationMetadata.class))
                .build();
        operationsInStage
            .labels(operationRequestMetadata.getExecuteOperationMetadata().getStage().name())
            .inc();
      }
      return operationRequestMetadata;
    } catch (Exception e) {
      log.log(
          Level.WARNING,
          String.format("Could not populate request metadata for %s.", operation.getName()),
          e);
      return null;
    }
  }

  protected static String formatRequestMetadataToJson(
      OperationRequestMetadata operationRequestMetadata) throws InvalidProtocolBufferException {
    JsonFormat.TypeRegistry typeRegistry =
        JsonFormat.TypeRegistry.newBuilder()
            .add(ExecuteResponse.getDescriptor())
            .add(ExecuteOperationMetadata.getDescriptor())
            .add(PreconditionFailure.getDescriptor())
            .build();

    String formattedRequestMetadata =
        JsonFormat.printer()
            .usingTypeRegistry(typeRegistry)
            .omittingInsignificantWhitespace()
            .print(operationRequestMetadata);
    log.log(Level.FINER, "{}", formattedRequestMetadata);
    return formattedRequestMetadata;
  }
}
