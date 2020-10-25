// Copyright 2019 The Bazel Authors. All rights reserved.
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

import static java.lang.String.format;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Platform;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import com.google.rpc.PreconditionFailure;
import com.google.rpc.PreconditionFailure.Violation;
import com.google.rpc.Status;
import io.grpc.StatusException;
import io.grpc.protobuf.StatusProto;

public final class Actions {
  private Actions() {}

  public static String invalidActionMessage(Digest actionDigest) {
    return format("Action %s is invalid", DigestUtil.toString(actionDigest));
  }

  public static String invalidActionVerboseMessage(
      Digest actionDigest, PreconditionFailure failure) {
    // if the list of violations get very long, display 3 at most
    int maxNumOfViolation = 3;
    String format =
        "Action %s is invalid: %s"
            + (failure.getViolationsList().size() > maxNumOfViolation ? " ..." : ".");
    String[] errorMessages =
        failure.getViolationsList().stream()
            .map(Violation::getDescription)
            .limit(maxNumOfViolation)
            .toArray(String[]::new);
    return format(format, DigestUtil.toString(actionDigest), String.join("; ", errorMessages));
  }

  public static void checkPreconditionFailure(
      Digest actionDigest, PreconditionFailure preconditionFailure) throws StatusException {
    if (preconditionFailure.getViolationsCount() != 0) {
      throw StatusProto.toStatusException(
          Status.newBuilder()
              .setCode(Code.FAILED_PRECONDITION.getNumber())
              .setMessage(invalidActionVerboseMessage(actionDigest, preconditionFailure))
              .addDetails(Any.pack(preconditionFailure))
              .build());
    }
  }

  public static Status asExecutionStatus(Throwable t) {
    Status.Builder status = Status.newBuilder();
    io.grpc.Status grpcStatus = io.grpc.Status.fromThrowable(t);
    switch (grpcStatus.getCode()) {
      case DEADLINE_EXCEEDED:
        // translate timeouts to retriable errors here, rather than
        // indications that the execution timed out
        status.setCode(Code.UNAVAILABLE.getNumber());
        break;
      default:
        status.setCode(grpcStatus.getCode().value());
        break;
    }
    return status.setMessage(t.getMessage()).build();
  }

  public static boolean isRetriable(Status status) {
    if (status == null
        || status.getCode() != Code.FAILED_PRECONDITION.getNumber()
        || status.getDetailsCount() == 0) {
      return false;
    }
    for (Any details : status.getDetailsList()) {
      try {
        PreconditionFailure f = details.unpack(PreconditionFailure.class);
        if (f.getViolationsCount() == 0) {
          return false; // Generally shouldn't happen
        }
        for (Violation v : f.getViolationsList()) {
          if (!v.getType().equals(Errors.VIOLATION_TYPE_MISSING)) {
            return false;
          }
        }
      } catch (InvalidProtocolBufferException protoEx) {
        return false;
      }
    }
    return true; // if *all* > 0 violations have type MISSING
  }

  private static boolean satisfiesRequirement(
      SetMultimap<String, String> provisions, String requirement, String value) {
    if (requirement.equals("min-cores")) {
      if (!provisions.containsKey("cores")) {
        return false;
      }
      int mincores = Integer.parseInt(value);
      int cores = Integer.parseInt(Iterables.getOnlyElement(provisions.get("cores")));
      return cores >= mincores;
    }
    if (requirement.equals("max-cores")) {
      return true;
    }
    return provisions.containsEntry(requirement, value)
        || provisions.containsEntry(requirement, "*");
  }

  public static boolean satisfiesRequirements(
      SetMultimap<String, String> provisions, Platform requirements) {
    for (Platform.Property property : requirements.getPropertiesList()) {
      if (!satisfiesRequirement(provisions, property.getName(), property.getValue())) {
        return false;
      }
    }
    return true;
  }
}
