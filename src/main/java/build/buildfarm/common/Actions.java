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
import static io.grpc.Status.Code.DEADLINE_EXCEEDED;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Platform;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.rpc.PreconditionFailure;
import com.google.rpc.PreconditionFailure.Violation;
import com.google.rpc.Code;
import com.google.rpc.Status;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.StatusException;
import io.grpc.protobuf.StatusProto;
import java.util.Map;

public final class Actions {
  private Actions() {
  }

  public static String invalidActionMessage(Digest actionDigest) {
    return format("Action %s is invalid", DigestUtil.toString(actionDigest));
  }

  public static void checkPreconditionFailure(
      Digest actionDigest,
      PreconditionFailure preconditionFailure)
      throws StatusException {
    if (preconditionFailure.getViolationsCount() != 0) {
      throw StatusProto.toStatusException(Status.newBuilder()
          .setCode(Code.FAILED_PRECONDITION.getNumber())
          .setMessage(invalidActionMessage(actionDigest))
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
    return status.build();
  }

  public static boolean isRetriable(Status status) {
    if (status.getCode() != Code.FAILED_PRECONDITION.getNumber()) {
      return false;
    }
    if (status == null || status.getDetailsCount() == 0) {
      return false;
    }
    for (Any details : status.getDetailsList()) {
      PreconditionFailure f;
      try {
        f = details.unpack(PreconditionFailure.class);
      } catch (InvalidProtocolBufferException protoEx) {
        return false;
      }
      if (f.getViolationsCount() == 0) {
        return false; // Generally shouldn't happen
      }
      for (Violation v : f.getViolationsList()) {
        if (!v.getType().equals(Errors.VIOLATION_TYPE_MISSING)) {
          return false;
        }
      }
    }
    return true; // if *all* > 0 violations have type MISSING
  }

  public static boolean satisfiesRequirements(Platform provider, Platform requirements) {
    // string compare only
    // no duplicate names
    ImmutableSetMultimap.Builder<String, String> provisionsBuilder =
        new ImmutableSetMultimap.Builder<>();
    for (Platform.Property property : provider.getPropertiesList()) {
      provisionsBuilder.put(property.getName(), property.getValue());
    }
    ImmutableSetMultimap<String, String> provisions = provisionsBuilder.build();
    for (Platform.Property property : requirements.getPropertiesList()) {
      if (!provisions.containsKey(property.getName()) ||
          !provisions.get(property.getName()).contains(property.getValue())) {
        return false;
      }
    }
    return true;
  }
}
