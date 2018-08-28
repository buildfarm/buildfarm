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

import build.buildfarm.common.DigestUtil;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.ByteStringIteratorInputStream;
import build.buildfarm.instance.stub.Chunker;
import build.buildfarm.instance.stub.RetryException;
import build.buildfarm.v1test.GrpcCASConfig;
import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamBlockingStub;
import com.google.bytestream.ByteStreamGrpc.ByteStreamStub;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusResponse;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Iterators;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc.ContentAddressableStorageBlockingStub;
import build.bazel.remote.execution.v2.Digest;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

class GrpcCAS implements ContentAddressableStorage {
  private final String instanceName;
  private final Channel channel;
  private final ByteStreamUploader uploader;
  private final ConcurrentMap<Digest, List<Runnable>> digestsOnExpirations = new ConcurrentHashMap<>();

  GrpcCAS(String instanceName, Channel channel, ByteStreamUploader uploader) {
    this.instanceName = instanceName;
    this.channel = channel;
    this.uploader = uploader;
  }

  private final Supplier<ContentAddressableStorageBlockingStub> contentAddressableStorageBlockingStub =
      Suppliers.memoize(
          new Supplier<ContentAddressableStorageBlockingStub>() {
            @Override
            public ContentAddressableStorageBlockingStub get() {
              return ContentAddressableStorageGrpc.newBlockingStub(channel);
            }
          });

  private final Supplier<ByteStreamBlockingStub> bsBlockingStub =
      Suppliers.memoize(
          new Supplier<ByteStreamBlockingStub>() {
            @Override
            public ByteStreamBlockingStub get() {
              return ByteStreamGrpc.newBlockingStub(channel);
            }
          });

  public InputStream newStreamInput(String name) {
    Iterator<ReadResponse> replies = bsBlockingStub
        .get()
        .read(ReadRequest.newBuilder().setResourceName(name).build());
    return new ByteStringIteratorInputStream(Iterators.transform(replies, (reply) -> reply.getData()));
  }

  private String getBlobName(Digest digest) {
    return String.format(
        "%sblobs/%s",
        instanceName.isEmpty() ? "" : (instanceName + "/"),
        DigestUtil.toString(digest));
  }

  @Override
  public boolean contains(Digest digest) {
    QueryWriteStatusResponse response = bsBlockingStub.get()
        .queryWriteStatus(QueryWriteStatusRequest.newBuilder()
            .setResourceName(getBlobName(digest))
            .build());
    boolean contains = response.getComplete()
        && response.getCommittedSize() == digest.getSizeBytes();
    if (!contains) {
      expire(digest);
    }
    return contains;
  }

  private synchronized void addOnExpiration(Digest digest, Runnable onExpiration) {
    List<Runnable> onExpirations = digestsOnExpirations.get(digest);
    if (onExpirations != null) {
      onExpirations = new ArrayList<>(1);
      digestsOnExpirations.put(digest, onExpirations);
    }
    onExpirations.add(onExpiration);
  }

  private void expire(Digest digest) {
    List<Runnable> onExpirations = digestsOnExpirations.remove(digest);
    if (onExpirations != null) {
      for (Runnable r : onExpirations) {
        r.run();
      }
    }
  }

  @Override
  public Blob get(Digest digest) {
    try (InputStream in = newStreamInput(getBlobName(digest))) {
      ByteString content = ByteString.readFrom(in);
      if (content.size() != digest.getSizeBytes()) {
        throw new IOException(String.format(
            "size/data mismatch: was %d, expected %d",
            content.size(),
            digest.getSizeBytes()));
      }
      return new Blob(content, digest);
    } catch (IOException ex) {
      expire(digest);
      return null;
    }
  }

  @Override
  public void put(Blob blob) throws InterruptedException {
    Chunker chunker = new Chunker(blob.getData(), blob.getDigest());
    try {
      uploader.uploadBlobs(Collections.singleton(chunker));
    } catch (RetryException e) {
      if (e.getCause() instanceof StatusRuntimeException) {
        throw (StatusRuntimeException) e.getCause();
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  public void put(Blob blob, Runnable onExpiration) throws InterruptedException {
    put(blob);
    addOnExpiration(blob.getDigest(), onExpiration);
  }
}
