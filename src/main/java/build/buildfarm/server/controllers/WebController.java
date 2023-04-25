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

package build.buildfarm.server.controllers;

import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.instance.shard.ShardInstance;
import build.buildfarm.operations.EnrichedOperation;
import build.buildfarm.v1test.CompletedOperationMetadata;
import build.buildfarm.v1test.ExecutingOperationMetadata;
import build.buildfarm.v1test.QueuedOperationMetadata;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.PreconditionFailure;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import lombok.extern.java.Log;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

// An HTTP frontend for providing a web UI in buildfarm.  Its primarily used by developers so that
// they can introspect their build inovocations.  Standalone tools that communicate over GRPC are
// sometimes less accessible.  Nonetheless, the APIs should be similar.
// The web frontend can be deployed as part of the servers.  However, the controller may also be
// included in other standalone tools if you'd like the frontend decoupled from the servers.
// This controller can provide web content directly through spring boot via thymeleaf.
// The controller also provides raw REST APIs for those wishing to build out their own frontend.
@Log
@Controller
@ConditionalOnProperty("ui.frontend.enable")
public class WebController {
  private static ShardInstance instance;

  // Most of the routes require an instance to have their queries fulfilled.
  // I wanted to have the instance set in the controller's constructor, but I was having trouble
  // doing that with springboot's bean initialization and auto-wiring.
  // Particularly because the controller is constructed before the instance is actually created.
  // I settled for having the instance static and allowing the server startup to provide the
  // instance once ready.
  public static void setInstance(ShardInstance instanceIn) {
    instance = instanceIn;
  }

  // This is typically the starting for a developer looking to introspect their builds.
  // When running a bazel client with `--bes_results_url` they will shown this URL route.
  // The page is intented to give them a summary of their build invocation and allow them to drill
  // down into more details.
  @GetMapping("/invocation/{invocationId}")
  public String invocation(Model model, @PathVariable String invocationId) {
    // We need to look up the user's operations as fast as possible.  Scanning all of the stored
    // operations and filtering by invocatio ID (i.e. O(n)) does not scale.  Instead, the invocation
    // ID must be the primary key to their specific list of build operation IDs.  We then lookup
    // each
    // operation by ID (preferably batched).  This is technically two backend calls.  It could be
    // made faster
    // at the expense of duplicate information stored in the backend.
    Set<String> operationIDs = instance.findOperationsByInvocationId(invocationId);
    Iterable<Map.Entry<String, String>> foundOperations = instance.getOperations(operationIDs);

    // Populate the model for the page.
    buildInvocationModel(model, invocationId, foundOperations);

    // Render page.
    return "invocation";
  }

  private void buildInvocationModel(
      Model model, String invocationId, Iterable<Map.Entry<String, String>> foundOperations) {
    // Create an array representing table information about all the oprations involved in the
    // invocation.
    // This is the core content of the page.
    JSONArray operationResults = new JSONArray();
    for (Map.Entry<String, String> entry : foundOperations) {
      Operation operation = jsonToOperation(entry.getValue());
      JSONObject obj = new JSONObject();
      obj.put("target", extractTargetId(operation));
      obj.put("mnemonic", extractActionMnemonic(operation));
      obj.put("stage", extractStatus(operation));
      obj.put("duration", extractDuration(operation));
      obj.put("worker", extractWorker(operation));

      String operationId = entry.getKey();
      String id = operationId.substring(operationId.lastIndexOf('/')).substring(1);
      obj.put("operationId", id);

      operationResults.add(obj);
    }

    // Populate data to be provided to the frontend.
    model.addAttribute("operation", operationResults.toJSONString());
    model.addAttribute("invocationId", String.format("Invocation: %s", invocationId));
  }

  // An operation represents an executed action.
  // The operation has a target ID which corresponds to the bazel label what developers are
  // typically thinking of when wishing to evaluate their build.
  // This page shows them all of the information that we track related to the operation.
  @GetMapping("/operation/{operationId}")
  public String operation(Model model, @PathVariable String operationId) {
    EnrichedOperation result =
        instance.findEnrichedOperation(String.format("shard/operations/%s", operationId));
    model.addAttribute("fullOperation", result.asJsonString());
    return "operation";
  }

  // Information about the current deployment.  Useful for verifying what is running.
  @GetMapping("/info")
  public String info(Model model) {
    return "info";
  }

  /**
   * @brief Convert string json into operation type.
   * @details Parses json and returns null if invalid.
   * @param json The json to convert to Operation type.
   * @return The created operation.
   * @note Suggested return identifier: operation.
   */
  private static Operation jsonToOperation(String json) {
    // create a json parser
    JsonFormat.Parser operationParser =
        JsonFormat.parser()
            .usingTypeRegistry(
                JsonFormat.TypeRegistry.newBuilder()
                    .add(CompletedOperationMetadata.getDescriptor())
                    .add(ExecutingOperationMetadata.getDescriptor())
                    .add(ExecuteOperationMetadata.getDescriptor())
                    .add(QueuedOperationMetadata.getDescriptor())
                    .add(PreconditionFailure.getDescriptor())
                    .build())
            .ignoringUnknownFields();

    if (json == null) {
      log.log(Level.WARNING, "Operation Json is empty");
      return null;
    }
    try {
      Operation.Builder operationBuilder = Operation.newBuilder();
      operationParser.merge(json, operationBuilder);
      return operationBuilder.build();
    } catch (InvalidProtocolBufferException e) {
      log.log(Level.WARNING, "InvalidProtocolBufferException while building an operation.", e);
      return null;
    }
  }

  private String extractTargetId(Operation operation) {
    return expectRequestMetadata(operation).getTargetId();
  }

  private String extractActionMnemonic(Operation operation) {
    return expectRequestMetadata(operation).getActionMnemonic();
  }

  private String extractStatus(Operation operation) {
    return String.valueOf(expectExecuteOperationMetadata(operation).getStage());
  }

  private String extractDuration(Operation operation) {
    Any result = operation.getResponse();
    try {
      Timestamp start =
          result
              .unpack(ExecuteResponse.class)
              .getResult()
              .getExecutionMetadata()
              .getWorkerStartTimestamp();
      Timestamp end =
          result
              .unpack(ExecuteResponse.class)
              .getResult()
              .getExecutionMetadata()
              .getWorkerCompletedTimestamp();
      Duration duration = Timestamps.between(start, end);
      return Durations.toSecondsAsDouble(duration) + "s";
    } catch (InvalidProtocolBufferException e) {
      System.out.println(e.toString());
      return "Unknown";
    }
  }

  private String extractWorker(Operation operation) {
    Any result = operation.getResponse();
    try {
      return result.unpack(ExecuteResponse.class).getResult().getExecutionMetadata().getWorker();
    } catch (InvalidProtocolBufferException e) {
      System.out.println(e.toString());
      return "Unknown";
    }
  }

  private static ExecuteOperationMetadata expectExecuteOperationMetadata(Operation operation) {
    String name = operation.getName();
    Any metadata = operation.getMetadata();
    QueuedOperationMetadata queuedOperationMetadata = maybeQueuedOperationMetadata(name, metadata);
    if (queuedOperationMetadata != null) {
      return queuedOperationMetadata.getExecuteOperationMetadata();
    }
    ExecutingOperationMetadata executingOperationMetadata =
        maybeExecutingOperationMetadata(name, metadata);
    if (executingOperationMetadata != null) {
      return executingOperationMetadata.getExecuteOperationMetadata();
    }
    CompletedOperationMetadata completedOperationMetadata =
        maybeCompletedOperationMetadata(name, metadata);
    if (completedOperationMetadata != null) {
      return completedOperationMetadata.getExecuteOperationMetadata();
    }
    return ExecuteOperationMetadata.getDefaultInstance();
  }

  private static RequestMetadata expectRequestMetadata(Operation operation) {
    String name = operation.getName();
    Any metadata = operation.getMetadata();
    QueuedOperationMetadata queuedOperationMetadata = maybeQueuedOperationMetadata(name, metadata);
    if (queuedOperationMetadata != null) {
      return queuedOperationMetadata.getRequestMetadata();
    }
    ExecutingOperationMetadata executingOperationMetadata =
        maybeExecutingOperationMetadata(name, metadata);
    if (executingOperationMetadata != null) {
      return executingOperationMetadata.getRequestMetadata();
    }
    CompletedOperationMetadata completedOperationMetadata =
        maybeCompletedOperationMetadata(name, metadata);
    if (completedOperationMetadata != null) {
      return completedOperationMetadata.getRequestMetadata();
    }
    return RequestMetadata.getDefaultInstance();
  }

  private static QueuedOperationMetadata maybeQueuedOperationMetadata(String name, Any metadata) {
    if (metadata.is(QueuedOperationMetadata.class)) {
      try {
        return metadata.unpack(QueuedOperationMetadata.class);
      } catch (InvalidProtocolBufferException e) {
        log.log(Level.SEVERE, String.format("invalid executing operation metadata %s", name), e);
      }
    }
    return null;
  }

  private static ExecutingOperationMetadata maybeExecutingOperationMetadata(
      String name, Any metadata) {
    if (metadata.is(ExecutingOperationMetadata.class)) {
      try {
        return metadata.unpack(ExecutingOperationMetadata.class);
      } catch (InvalidProtocolBufferException e) {
        log.log(Level.SEVERE, String.format("invalid executing operation metadata %s", name), e);
      }
    }
    return null;
  }

  private static CompletedOperationMetadata maybeCompletedOperationMetadata(
      String name, Any metadata) {
    if (metadata.is(CompletedOperationMetadata.class)) {
      try {
        return metadata.unpack(CompletedOperationMetadata.class);
      } catch (InvalidProtocolBufferException e) {
        log.log(Level.SEVERE, String.format("invalid completed operation metadata %s", name), e);
      }
    }
    return null;
  }
}
