// Copyright 2023 The Bazel Authors. All rights reserved.
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

package build.buildfarm.common;

import static build.buildfarm.common.Errors.VIOLATION_TYPE_MISSING;

import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.ExecutionStage;
import build.buildfarm.v1test.ExecuteEntry;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.rpc.PreconditionFailure;
import io.grpc.Status.Code;

/**
 * @class FailedOperationGetter
 * @brief Converts any operation into a failed operation.
 * @details Sets properties on the existing operation so that the new operation is considered
 *     finished and failed.
 */
public class FailedOperationGetter {
  public static Operation get(
      Operation operation,
      ExecuteEntry executeEntry,
      String failureMessage,
      String failureDetails) {
    return operation
        .toBuilder()
        .setName(executeEntry.getOperationName())
        .setDone(true)
        .setMetadata(
            Any.pack(executeOperationMetadata(executeEntry, ExecutionStage.Value.COMPLETED)))
        .setResponse(Any.pack(failResponse(executeEntry, failureMessage, failureDetails)))
        .build();
  }

  private static ExecuteOperationMetadata executeOperationMetadata(
      ExecuteEntry executeEntry, ExecutionStage.Value stage) {
    return ExecuteOperationMetadata.newBuilder()
        .setActionDigest(executeEntry.getActionDigest())
        .setStdoutStreamName(executeEntry.getStdoutStreamName())
        .setStderrStreamName(executeEntry.getStderrStreamName())
        .setStage(stage)
        .build();
  }

  private static ExecuteResponse failResponse(
      ExecuteEntry executeEntry, String failureMessage, String failureDetails) {
    PreconditionFailure.Builder preconditionFailureBuilder = PreconditionFailure.newBuilder();
    preconditionFailureBuilder
        .addViolationsBuilder()
        .setType(VIOLATION_TYPE_MISSING)
        .setSubject("blobs/" + DigestUtil.toString(executeEntry.getActionDigest()))
        .setDescription(failureDetails);
    PreconditionFailure preconditionFailure = preconditionFailureBuilder.build();

    return ExecuteResponse.newBuilder()
        .setStatus(
            com.google.rpc.Status.newBuilder()
                .setCode(Code.FAILED_PRECONDITION.value())
                .setMessage(failureMessage)
                .addDetails(Any.pack(preconditionFailure))
                .build())
        .build();
  }
}
