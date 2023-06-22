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

import static build.buildfarm.common.grpc.Retrier.NO_RETRIES;
import static com.google.common.collect.Multimaps.synchronizedListMultimap;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.cas.cfc.CASFileCache;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.common.config.Cas;
import build.buildfarm.instance.stub.ByteStreamUploader;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import io.grpc.Channel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import javax.naming.ConfigurationException;

public final class ContentAddressableStorages {
  private static BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  private static Channel createChannel(String target) {
    NettyChannelBuilder builder =
        NettyChannelBuilder.forTarget(target).negotiationType(NegotiationType.PLAINTEXT);
    return builder.build();
  }

  public static ContentAddressableStorage createGrpcCAS(Cas cas) {
    Channel channel = createChannel(cas.getTarget());
    ByteStreamUploader byteStreamUploader =
        new ByteStreamUploader("", channel, null, 300, NO_RETRIES);
    ListMultimap<Digest, Runnable> onExpirations =
        synchronizedListMultimap(MultimapBuilder.hashKeys().arrayListValues().build());

    return new GrpcCAS(configs.getServer().getName(), channel, byteStreamUploader, onExpirations);
  }

  public static ContentAddressableStorage createFilesystemCAS(Cas config)
      throws ConfigurationException {
    String path = config.getPath();
    if (path.isEmpty()) {
      throw new ConfigurationException("filesystem cas path is empty");
    }
    if (config.getMaxSizeBytes() <= 0) {
      throw new ConfigurationException("filesystem cas max_size_bytes <= 0");
    }
    if (configs.getMaxEntrySizeBytes() <= 0) {
      throw new ConfigurationException("filesystem cas max_entry_size_bytes <= 0");
    }
    if (configs.getMaxEntrySizeBytes() > config.getMaxSizeBytes()) {
      throw new ConfigurationException("filesystem cas max_entry_size_bytes > maxSizeBytes");
    }
    if (config.getHexBucketLevels() < 0) {
      throw new ConfigurationException("filesystem cas hex_bucket_levels <= 0");
    }
    CASFileCache cas =
        new CASFileCache(
            Paths.get(path),
            config,
            configs.getMaxEntrySizeBytes(),
            DigestUtil.forHash("SHA256"),
            /* expireService=*/ newDirectExecutorService(),
            /* accessRecorder=*/ directExecutor()) {
          @Override
          protected InputStream newExternalInput(
              Compressor.Value compressor, Digest digest, long offset) throws IOException {
            throw new NoSuchFileException(digest.getHash());
          }
        };
    try {
      cas.start(false);
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException("error starting filesystem cas", e);
    }
    return cas;
  }

  public static ContentAddressableStorage create(Cas cas) throws ConfigurationException {
    switch (cas.getType()) {
      default:
        throw new IllegalArgumentException("CAS config not set in config");
      case FILESYSTEM:
        return createFilesystemCAS(cas);
      case GRPC:
        return createGrpcCAS(cas);
      case MEMORY:
        return new MemoryCAS(cas.getMaxSizeBytes());
    }
  }
}
