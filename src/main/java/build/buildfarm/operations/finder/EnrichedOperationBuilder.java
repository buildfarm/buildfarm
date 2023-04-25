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

package build.buildfarm.operations.finder;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.Utils;
import build.buildfarm.operations.EnrichedOperation;
import build.buildfarm.v1test.CompletedOperationMetadata;
import build.buildfarm.v1test.ExecutingOperationMetadata;
import build.buildfarm.v1test.QueuedOperationMetadata;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.rpc.PreconditionFailure;
import java.util.logging.Level;
import lombok.extern.java.Log;
import redis.clients.jedis.JedisCluster;

/**
 * @class EnrichedOperationBuilder
 * @brief Builds an operation from an operation key with the operation's important metadata
 *     pre-populated.
 * @details For performance reasons, only build these enriched operations when you intend to use the
 *     extra provided metadata.
 */
@Log
public class EnrichedOperationBuilder {
  /**
   * @brief Create an enriched operation based on an operation key.
   * @details This will make calls to get blobs, and resolve digests into the appropriate data
   *     structures.
   * @param cluster An established redis cluster.
   * @param instance An instance is used to get additional information about the operation.
   * @param operationKey Key to get operation from.
   * @return Operation with populated metadata.
   * @note Suggested return identifier: operation.
   */
  public static EnrichedOperation build(
      JedisCluster cluster, Instance instance, String operationKey) {
    EnrichedOperation operationWithMetadata = new EnrichedOperation();
    operationWithMetadata.operation = operationKeyToOperation(cluster, operationKey);
    // the operation could not be fetched so there is nothing further to derive
    if (operationWithMetadata.operation == null) {
      return operationWithMetadata;
    }
    operationWithMetadata.action =
        actionDigestToAction(instance, operationToActionDigest(operationWithMetadata.operation));

    // the action could not be fetched so there is nothing further to derive
    if (operationWithMetadata.action == null) {
      return operationWithMetadata;
    }

    operationWithMetadata.command =
        commandDigestToCommand(instance, operationWithMetadata.action.getCommandDigest());
    return operationWithMetadata;
  }

  /**
   * @brief Convert an operation key into the actual Operation type.
   * @details Extracts json from redis and parses it. Null if json was invalid.
   * @param cluster An established redis cluster.
   * @param operationKey The key to lookup and get back the operation of.
   * @return The looked up operation.
   * @note Suggested return identifier: operation.
   */
  public static Operation operationKeyToOperation(JedisCluster cluster, String operationKey) {
    String json = cluster.get(operationKey);
    return jsonToOperation(json);
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

  /**
   * @brief Get the action digest of the operation.
   * @details Extracted out of the relevant operation metadata.
   * @param operation The operation.
   * @return The extracted digest.
   * @note Suggested return identifier: digest.
   */
  @SuppressWarnings("ConstantConditions")
  private static Digest operationToActionDigest(Operation operation) {
    ExecuteOperationMetadata metadata;

    try {
      if (operation.getMetadata().is(QueuedOperationMetadata.class)) {
        QueuedOperationMetadata queuedOperationMetadata =
            operation.getMetadata().unpack(QueuedOperationMetadata.class);
        metadata = queuedOperationMetadata.getExecuteOperationMetadata();
      } else if (operation.getMetadata().is(ExecutingOperationMetadata.class)) {
        ExecutingOperationMetadata executingMetadata =
            operation.getMetadata().unpack(ExecutingOperationMetadata.class);
        metadata = executingMetadata.getExecuteOperationMetadata();
      } else if (operation.getMetadata().is(CompletedOperationMetadata.class)) {
        CompletedOperationMetadata completedMetadata =
            operation.getMetadata().unpack(CompletedOperationMetadata.class);
        metadata = completedMetadata.getExecuteOperationMetadata();
      } else {
        metadata = operation.getMetadata().unpack(ExecuteOperationMetadata.class);
      }

    } catch (InvalidProtocolBufferException e) {
      log.log(Level.WARNING, "InvalidProtocolBufferException while building an operation.", e);
      metadata = null;
    }

    return metadata.getActionDigest();
  }

  /**
   * @brief Get the action based on the action digest.
   * @details Instance used to fetch the blob.
   * @param instance An instance is used to get additional information about the operation.
   * @param digest The action digest.
   * @return The action from the provided digest.
   * @note Suggested return identifier: action.
   */
  private static Action actionDigestToAction(Instance instance, Digest digest) {
    try {
      ByteString blob =
          Utils.getBlob(
              instance, Compressor.Value.IDENTITY, digest, RequestMetadata.getDefaultInstance());
      Action action;
      try {
        action = Action.parseFrom(blob);
        return action;
      } catch (InvalidProtocolBufferException e) {
        log.log(Level.WARNING, "InvalidProtocolBufferException while building an operation.", e);
        return null;
      }
    } catch (Exception e) {
      log.log(Level.WARNING, e.getMessage());
      return null;
    }
  }

  /**
   * @brief Get the command based on the command digest.
   * @details Instance used to fetch the blob.
   * @param instance An instance is used to get additional information about the operation.
   * @param digest The command digest.
   * @return The Command from the provided digest.
   * @note Suggested return identifier: command.
   */
  private static Command commandDigestToCommand(Instance instance, Digest digest) {
    try {
      ByteString blob =
          Utils.getBlob(
              instance, Compressor.Value.IDENTITY, digest, RequestMetadata.getDefaultInstance());
      Command command;
      try {
        command = Command.parseFrom(blob);
        return command;
      } catch (InvalidProtocolBufferException e) {
        log.log(Level.WARNING, "InvalidProtocolBufferException while building an operation.", e);
        return null;
      }
    } catch (Exception e) {
      log.log(Level.WARNING, e.getMessage());
      return null;
    }
  }
}
