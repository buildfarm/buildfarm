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

package build.buildfarm.common.grpc;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import build.buildfarm.common.Time;
import io.grpc.Context;
import io.grpc.Status;
import java.util.concurrent.ScheduledExecutorService;

///
/// @class   GrpcEndpointHandler
/// @brief   Utilities for acting on grpc endpoints.
/// @details Can enforce deadlines and ensure correct errors are sent back to
///          the client.
///
public class GrpcEndpointHandler {

  ///
  /// @brief   Handle timeouts specified by the grpc endpoint.
  /// @details A grpc endpoint may not enforce any specific timeouts.
  /// @param   endpoint Information about the endpoint including how to handle timeouts.
  /// @param   context  The grpc context from hitting the endpoint.
  /// @param   executor The executor to use with the context.
  ///
  public static <T> void handleTimeout(
      GrpcEndpoint<T> endpoint, Context context, ScheduledExecutorService executor) {
    if (endpoint.enforceDeadline) {
      enforceDeadline(endpoint, context, executor);
    }
  }
  ///
  /// @brief   Enforce a deadline on the endpoint.
  /// @details Cancel the context and operation after a certain amount of time.
  /// @param   endpoint Information about the endpoint including how to handle timeouts.
  /// @param   context  The grpc context from hitting the endpoint.
  /// @param   executor The executor to use with the context.
  ///
  private static <T> void enforceDeadline(
      GrpcEndpoint<T> endpoint, Context context, ScheduledExecutorService executor) {
    // enforce a grpc timeout using a deadline.
    // when the timeout occurs, it will cause a context cancellation.
    // we also cancel the operation, and return an error to the client.
    // the operation is canceled so that ongoing work does not continue after the grpc timeout.
    Context.CancellableContext c =
        context.withDeadline(Time.toDeadline(endpoint.duration), executor);
    Context.CancellationListener listener =
        new Context.CancellationListener() {
          @Override
          public void cancelled(Context ctx) {
            endpoint.operation.cancel(true);
            String error = deadlineExceededError(endpoint);
            endpoint.streamObserver.onError(
                Status.DEADLINE_EXCEEDED.withDescription(error).asException());
          }
        };

    c.addListener(listener, directExecutor());
  }
  ///
  /// @brief   Get error message for deadline exceeded.
  /// @details This message will be sent back to the client.
  /// @param   endpoint Information about the endpoint including how to handle timeouts.
  /// @return  The error message.
  /// @note    Suggested return identifier: error.
  ///
  private static <T> String deadlineExceededError(GrpcEndpoint<T> endpoint) {
    return String.format(
        "The grpc endpoint '%s' has timed out because buildfarm enforces a deadline of %ds",
        endpoint.name, endpoint.duration.getSeconds());
  }
}
