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

import static build.buildfarm.common.grpc.Channels.createChannel;
import static build.buildfarm.common.grpc.Retrier.NO_RETRIES;
import static com.google.common.collect.Multimaps.synchronizedListMultimap;

import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.common.config.Cas;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.v1test.Digest;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import io.grpc.Channel;

public final class ContentAddressableStorages {
  private static BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  public static ContentAddressableStorage createGrpcCAS(Cas cas) {
    Channel channel = createChannel(cas.getTarget());
    ByteStreamUploader byteStreamUploader =
        new ByteStreamUploader("", channel, null, 300, NO_RETRIES);
    ListMultimap<Digest, Runnable> onExpirations =
        synchronizedListMultimap(MultimapBuilder.hashKeys().arrayListValues().build());

    return new GrpcCAS(
        configs.getServer().getName(),
        cas.isReadonly(),
        channel,
        byteStreamUploader,
        onExpirations);
  }
}
