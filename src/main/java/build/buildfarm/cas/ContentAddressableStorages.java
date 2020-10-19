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
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

import build.bazel.remote.execution.v2.BatchReadBlobsResponse.Response;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.Write;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.v1test.ContentAddressableStorageConfig;
import build.buildfarm.v1test.FilesystemCASConfig;
import build.buildfarm.v1test.GrpcCASConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;
import javax.naming.ConfigurationException;

public final class ContentAddressableStorages {
  private static Channel createChannel(String target) {
    NettyChannelBuilder builder =
        NettyChannelBuilder.forTarget(target).negotiationType(NegotiationType.PLAINTEXT);
    return builder.build();
  }

  public static ContentAddressableStorage createGrpcCAS(GrpcCASConfig config) {
    Channel channel = createChannel(config.getTarget());
    ByteStreamUploader byteStreamUploader =
        new ByteStreamUploader("", channel, null, 300, NO_RETRIES);
    ListMultimap<Digest, Runnable> onExpirations =
        synchronizedListMultimap(MultimapBuilder.hashKeys().arrayListValues().build());

    return new GrpcCAS(config.getInstanceName(), channel, byteStreamUploader, onExpirations);
  }

  public static ContentAddressableStorage createFilesystemCAS(FilesystemCASConfig config)
      throws ConfigurationException {
    String path = config.getPath();
    if (path.isEmpty()) {
      throw new ConfigurationException("filesystem cas path is empty");
    }
    long maxSizeBytes = config.getMaxSizeBytes();
    long maxEntrySizeBytes = config.getMaxEntrySizeBytes();
    boolean storeFileDirsIndexInMemory = config.getFileDirectoriesIndexInMemory();
    if (maxSizeBytes <= 0) {
      throw new ConfigurationException("filesystem cas max_size_bytes <= 0");
    }
    if (maxEntrySizeBytes <= 0) {
      throw new ConfigurationException("filesystem cas max_entry_size_bytes <= 0");
    }
    if (maxEntrySizeBytes > maxSizeBytes) {
      throw new ConfigurationException("filesystem cas max_entry_size_bytes > maxSizeBytes");
    }
    CASFileCache cas =
        new CASFileCache(
            Paths.get(path),
            maxSizeBytes,
            maxEntrySizeBytes,
            storeFileDirsIndexInMemory,
            DigestUtil.forHash("SHA256"),
            /* expireService=*/ newDirectExecutorService(),
            /* accessRecorder=*/ directExecutor()) {
          @Override
          protected InputStream newExternalInput(Digest digest, long offset) throws IOException {
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

  public static ContentAddressableStorage create(ContentAddressableStorageConfig config)
      throws ConfigurationException {
    switch (config.getTypeCase()) {
      default:
      case FILESYSTEM:
        return createFilesystemCAS(config.getFilesystem());
      case TYPE_NOT_SET:
        throw new IllegalArgumentException("CAS config not set in config");
      case GRPC:
        return createGrpcCAS(config.getGrpc());
      case MEMORY:
        return new MemoryCAS(config.getMemory().getMaxSizeBytes());
    }
  }

  /** decorates a map with a CAS interface, does not react to removals with expirations */
  public static ContentAddressableStorage casMapDecorator(Map<Digest, ByteString> map) {
    return new ContentAddressableStorage() {
      final Writes writes = new Writes(this);

      @Override
      public boolean contains(Digest digest) {
        return map.containsKey(digest);
      }

      @Override
      public Iterable<Digest> findMissingBlobs(Iterable<Digest> digests) {
        ImmutableList.Builder<Digest> missing = ImmutableList.builder();
        for (Digest digest : digests) {
          if (digest.getSizeBytes() != 0 && !map.containsKey(digest)) {
            missing.add(digest);
          }
        }
        return missing.build();
      }

      @Override
      public Write getWrite(Digest digest, UUID uuid, RequestMetadata requestMetadata) {
        return writes.get(digest, uuid);
      }

      @Override
      public InputStream newInput(Digest digest, long offset) throws IOException {
        ByteString data = map.get(digest);
        if (data == null) {
          throw new NoSuchFileException(digest.getHash());
        }
        InputStream in = data.newInput();
        in.skip(offset);
        return in;
      }

      @Override
      public void get(
          Digest digest,
          long offset,
          long count,
          ServerCallStreamObserver<ByteString> responseObserver,
          RequestMetadata requestMetadata) {
        ByteString data = map.get(digest);
        if (data == null) {
          responseObserver.onError(Status.NOT_FOUND.asException());
        } else {
          responseObserver.onNext(map.get(digest));
          responseObserver.onCompleted();
        }
      }

      @Override
      public ListenableFuture<Iterable<Response>> getAllFuture(Iterable<Digest> digests) {
        return immediateFuture(MemoryCAS.getAll(digests, map::get));
      }

      @Override
      public Blob get(Digest digest) {
        ByteString data = map.get(digest);
        if (data == null) {
          return null;
        }
        return new Blob(data, digest);
      }

      @Override
      public void put(Blob blob) {
        map.put(blob.getDigest(), blob.getData());

        writes.getFuture(blob.getDigest()).set(blob.getData());
      }

      @Override
      public void put(Blob blob, Runnable onExpiration) {
        put(blob);
      }

      @Override
      public long maxEntrySize() {
        return UNLIMITED_ENTRY_SIZE_MAX;
      }
    };
  }
}
