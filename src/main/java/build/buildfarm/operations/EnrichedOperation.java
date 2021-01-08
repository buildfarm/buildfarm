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

package build.buildfarm.operations;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.buildfarm.v1test.CompletedOperationMetadata;
import build.buildfarm.v1test.ExecutingOperationMetadata;
import build.buildfarm.v1test.QueuedOperationMetadata;
import com.google.longrunning.Operation;
import com.google.protobuf.util.JsonFormat;
import com.google.rpc.PreconditionFailure;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/**
 * @class EnrichedOperation
 * @brief Operations with resolved metadata.
 * @details Operations contain metadata which is not readily available when obtaining the operation.
 *     Additional calls must be made to retrieve parts of metadata based on digests. Often times an
 *     operation can't be evaluated without also evaluating the command of the operation. In these
 *     contexts, it would be more helpful to operate on an enriched operation that already has its
 *     important metadata resolved.
 */
public class EnrichedOperation {

  /**
   * @field operation
   * @brief The main operation object which contains digests to the remaining data members.
   * @details Its digests are used to resolve other data members.
   */
  public Operation operation;

  /**
   * @field action
   * @brief The resolved action of the operation.
   * @details Created from the digest in the operation.
   */
  public Action action;

  /**
   * @field command
   * @brief The resolved command of the action.
   * @details Created from the digest in the action.
   */
  public Command command;

  /**
   * @brief Convert the structure into a json string.
   * @details Uses proto to json serialization where appropriate.
   * @return The structure as a json string.
   * @note Suggested return identifier: json.
   */
  public String asJsonString() {
    JSONObject obj = new JSONObject();
    try {

      JsonFormat.Printer operationPrinter =
          JsonFormat.printer()
              .usingTypeRegistry(
                  JsonFormat.TypeRegistry.newBuilder()
                      .add(CompletedOperationMetadata.getDescriptor())
                      .add(ExecutingOperationMetadata.getDescriptor())
                      .add(ExecuteOperationMetadata.getDescriptor())
                      .add(QueuedOperationMetadata.getDescriptor())
                      .add(PreconditionFailure.getDescriptor())
                      .build());

      JSONParser j = new JSONParser();
      obj.put("operation", j.parse(operationPrinter.print(operation)));
      obj.put("action", j.parse(JsonFormat.printer().print(action)));
      obj.put("command", j.parse(JsonFormat.printer().print(command)));
    } catch (Exception e) {
    }
    return obj.toJSONString();
  }
}
