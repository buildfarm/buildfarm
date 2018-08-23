// Copyright 2018 The Bazel Authors. All rights reserved.
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

package build.buildfarm.cas;

import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.Retrier;
import build.buildfarm.v1test.ContentAddressableStorageConfig;
import build.buildfarm.v1test.GrpcCASConfig;
import io.grpc.Channel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;

public final class ContentAddressableStorages {
  private static Channel createChannel(String target) {
    NettyChannelBuilder builder =
        NettyChannelBuilder.forTarget(target)
            .negotiationType(NegotiationType.PLAINTEXT);
    return builder.build();
  }

  private static ContentAddressableStorage createGrpcCAS(GrpcCASConfig config) {
    Channel channel = createChannel(config.getTarget());
    ByteStreamUploader byteStreamUploader
        = new ByteStreamUploader("", channel, null, 300, Retrier.NO_RETRIES, null);

    return new GrpcCAS(config.getInstanceName(), channel, byteStreamUploader);
  }

  public static ContentAddressableStorage create(ContentAddressableStorageConfig config) {
    switch (config.getTypeCase()) {
      default:
      case TYPE_NOT_SET:
        throw new IllegalArgumentException("CAS config not set in config");
      case GRPC:
        return createGrpcCAS(config.getGrpc());
      case MEMORY:
        return new MemoryCAS(config.getMemory().getMaxSizeBytes());
    }
  }
}
