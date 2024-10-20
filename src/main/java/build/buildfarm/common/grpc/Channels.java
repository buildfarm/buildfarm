// Copyright 2023 The Buildfarm Authors. All rights reserved.
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

import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;

public final class Channels {
  private static final String GRPCS_URL_PREFIX = "grpcs://";
  private static final String GRPC_URL_PREFIX = "grpc://";

  private Channels() {}

  public static ManagedChannel createChannel(String target) {
    NegotiationType negotiationType = NegotiationType.PLAINTEXT;
    if (target.startsWith(GRPCS_URL_PREFIX)) {
      target = target.substring(GRPCS_URL_PREFIX.length());
      negotiationType = NegotiationType.TLS;
    } else if (target.startsWith(GRPC_URL_PREFIX)) {
      target = target.substring(GRPC_URL_PREFIX.length());
      negotiationType = NegotiationType.PLAINTEXT;
    }
    NettyChannelBuilder builder =
        NettyChannelBuilder.forTarget(target).negotiationType(negotiationType);
    return builder.build();
  }
}
