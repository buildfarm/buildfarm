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

import static build.buildfarm.common.Errors.VIOLATION_TYPE_MISSING;
import static com.google.common.truth.Truth.assertThat;

import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.common.config.Metrics;
import build.buildfarm.metrics.log.LogMetricsPublisher;
import build.buildfarm.v1test.OperationRequestMetadata;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.rpc.PreconditionFailure;
import com.google.rpc.Status;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MetricsPublisherTest {
  private final ExecuteOperationMetadata defaultExecuteOperationMetadata =
      ExecuteOperationMetadata.getDefaultInstance();
  private final RequestMetadata defaultRequestMetadata =
      RequestMetadata.getDefaultInstance().toBuilder()
          .setCorrelatedInvocationsId(
              "http://user@host-name?uuid_source\\u003d%2Fproc%2Fsys%2Fkernel%2Frandom%2Fuuid\\u0026OSTYPE\\u003dlinux-gnu#c09a5efa-f015-4d7b-b889-8ee0d097dff7")
          .build();
  private final Operation defaultOperation =
      Operation.getDefaultInstance().toBuilder()
          .setDone(true)
          .setName("shard/operations/123")
          .build();
  private final ExecuteResponse defaultExecuteResponse = ExecuteResponse.getDefaultInstance();
  private final PreconditionFailure.Violation defaultViolation =
      PreconditionFailure.Violation.newBuilder()
          .setType(VIOLATION_TYPE_MISSING)
          .setSubject("TEST")
          .setDescription("TEST")
          .build();
  private final PreconditionFailure preconditionFailure =
      PreconditionFailure.getDefaultInstance().toBuilder().addViolations(defaultViolation).build();

  private BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  @Before
  public void setUp() throws IOException {
    configs.getServer().setCloudRegion("test");
    configs.getServer().setClusterId("buildfarm-test");
    configs.getServer().getMetrics().setPublisher(Metrics.PUBLISHER.LOG);
  }

  @Test
  public void publishCompleteMetricsTest() throws InvalidProtocolBufferException {
    Operation operation =
        defaultOperation.toBuilder()
            .setResponse(Any.pack(defaultExecuteResponse))
            .setMetadata(Any.pack(defaultExecuteOperationMetadata))
            .build();

    LogMetricsPublisher metricsPublisher = new LogMetricsPublisher();
    assertThat(
            AbstractMetricsPublisher.formatRequestMetadataToJson(
                metricsPublisher.populateRequestMetadata(operation, defaultRequestMetadata)))
        .isNotNull();

    OperationRequestMetadata operationRequestMetadata =
        OperationRequestMetadata.newBuilder()
            .setRequestMetadata(defaultRequestMetadata)
            .setOperationName(operation.getName())
            .setDone(operation.getDone())
            .setClusterId("buildfarm-test")
            .setExecuteResponse(operation.getResponse().unpack(ExecuteResponse.class))
            .setExecuteOperationMetadata(
                operation.getMetadata().unpack(ExecuteOperationMetadata.class))
            .build();

    assertThat(
            AbstractMetricsPublisher.formatRequestMetadataToJson(
                metricsPublisher.populateRequestMetadata(operation, defaultRequestMetadata)))
        .isEqualTo(
            JsonFormat.printer().omittingInsignificantWhitespace().print(operationRequestMetadata));
  }

  @Test
  public void publishMetricsWithNoExecuteResponseTest() {
    Operation operation =
        defaultOperation.toBuilder().setMetadata(Any.pack(defaultExecuteOperationMetadata)).build();

    assertThat(new LogMetricsPublisher().populateRequestMetadata(operation, defaultRequestMetadata))
        .isNotNull();
  }

  @Test
  public void publishMetricsWithNoExecuteOperationMetadataTest() {
    Operation operation =
        defaultOperation.toBuilder().setResponse(Any.pack(defaultExecuteResponse)).build();

    assertThat(new LogMetricsPublisher().populateRequestMetadata(operation, defaultRequestMetadata))
        .isNotNull();
  }

  @Test
  public void preconditionFailureTest() {
    Status status =
        Status.getDefaultInstance().toBuilder().addDetails(Any.pack(preconditionFailure)).build();

    Operation operation =
        defaultOperation.toBuilder()
            .setResponse(Any.pack(defaultExecuteResponse.toBuilder().setStatus(status).build()))
            .setMetadata(Any.pack(defaultExecuteOperationMetadata))
            .build();

    assertThat(new LogMetricsPublisher().populateRequestMetadata(operation, defaultRequestMetadata))
        .isNotNull();
  }
}
