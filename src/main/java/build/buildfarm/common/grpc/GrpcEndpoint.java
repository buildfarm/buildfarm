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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Duration;
import io.grpc.stub.ServerCallStreamObserver;

///
/// @class   GrpcEndpoint
/// @brief   Information about specific grpc endpoints.
/// @details Used to control deadlines on specific endpoints and provide
///          useful error messages.
///
public class GrpcEndpoint<StreamType> {

  ///
  /// @field   streamObserver
  /// @brief   The stream observer of the client.
  /// @details Used to communicate to the caller of the endpoint.
  ///
  public ServerCallStreamObserver<StreamType> streamObserver;

  ///
  /// @field   operation
  /// @brief   The ongoing operation from a client hitting the endpoint.
  /// @details Can be canceled upon grpc timeout.
  ///
  public ListenableFuture<Void> operation;

  ///
  /// @field   name
  /// @brief   The name of the endpoint.
  /// @details This is for debugging messages and for sending information to
  ///          the client. It should closely resemble the grpc service method.
  ///
  public String name;

  ///
  /// @field   enforceDeadline
  /// @brief   Whether or not to enforce a grpc deadline on the endpoint.
  /// @details A deadline can be used to terminate the ongoing operation and
  ///          respond to the user via the stream observer.
  ///
  public boolean enforceDeadline;

  ///
  /// @field   duration
  /// @brief   The amount of time the operation is allowed to run.
  /// @details This can be used to enforce a global timeout on the grpc
  ///          endpoint.
  ///
  public Duration duration;
}
