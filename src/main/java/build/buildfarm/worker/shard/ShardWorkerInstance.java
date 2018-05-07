// Copyright 2017 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker.shard;

import build.buildfarm.common.ContentAddressableStorage;
import build.buildfarm.common.ContentAddressableStorage.Blob;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.ShardBackplane;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.TokenizableIterator;
import build.buildfarm.instance.TreeIterator;
import build.buildfarm.v1test.ShardWorkerInstanceConfig;
import build.buildfarm.worker.Fetcher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.Platform;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata.Stage;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.Consumer;
import java.util.function.Predicate;
import javax.naming.ConfigurationException;

public class ShardWorkerInstance implements Instance {
  private final String name;
  private final DigestUtil digestUtil;
  private final ShardWorkerInstanceConfig config;
  private final ShardBackplane backplane;
  private final Fetcher fetcher;
  private final ContentAddressableStorage contentAddressableStorage;

  public ShardWorkerInstance(
      String name,
      DigestUtil digestUtil,
      ShardBackplane backplane,
      Fetcher fetcher,
      ContentAddressableStorage contentAddressableStorage,
      ShardWorkerInstanceConfig config) throws ConfigurationException {
    this.name = name;
    this.digestUtil = digestUtil;
    this.config = config;
    this.backplane = backplane;
    this.fetcher = fetcher;
    this.contentAddressableStorage = contentAddressableStorage;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public DigestUtil getDigestUtil() {
    return digestUtil;
  }

  @Override
  public ActionResult getActionResult(ActionKey actionKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putActionResult(ActionKey actionKey, ActionResult actionResult) {
    backplane.putActionResult(actionKey, actionResult);
  }

  @Override
  public Iterable<Digest> findMissingBlobs(Iterable<Digest> digests) {
    return Iterables.filter(digests, (digest) -> !contentAddressableStorage.contains(digest));
  }

  @Override
  public Iterable<Digest> putAllBlobs(Iterable<ByteString> blobs)
      throws IOException, IllegalArgumentException, InterruptedException {
    ImmutableList.Builder<Digest> digests = new ImmutableList.Builder<>();
    for (ByteString content : blobs) {
      if (content.size() == 0) {
        digests.add(digestUtil.empty());
        continue;
      }

      Blob blob = new Blob(content, digestUtil);
      Digest blobDigest = blob.getDigest();
      contentAddressableStorage.put(blob);
      backplane.addBlobLocation(blobDigest, getName());
      digests.add(blobDigest);
    }
    return digests.build();
  }

  @Override
  public String getBlobName(Digest blobDigest) {
    throw new UnsupportedOperationException();
  }

  private ByteString getBlobImpl(Digest blobDigest) {
    Blob blob = contentAddressableStorage.get(blobDigest);
    if (blob == null) {
      return null;
    }
    return blob.getData();
  }

  @Override
  public ByteString getBlob(Digest blobDigest) {
    return getBlobImpl(blobDigest);
  }

  @Override
  public ByteString getBlob(Digest blobDigest, long offset, long limit) {
    ByteString content = getBlobImpl(blobDigest);
    if (content == null) {
      return null;
    }
    if (offset != 0 || limit != 0) {
      content = content.substring((int) offset, (int) (limit == 0 ? (content.size() - offset) : (limit + offset)));
    }
    return content;
  }

  // write through fetch with local lookup
  public ByteString fetchBlob(Digest blobDigest) {
    synchronized (contentAddressableStorage.acquire(blobDigest)) {
      ByteString content = getBlob(blobDigest);

      if (content == null) {
        content = fetcher.fetchBlob(blobDigest);
        if (content != null) {
          Blob blob = new Blob(content, digestUtil);
          putBlobSynchronized(blob);
        }
      }
      contentAddressableStorage.release(blobDigest);

      return content;
    }
  }

  private Digest putRawBlob(Blob blob) {
    contentAddressableStorage.put(blob);
    backplane.addBlobLocation(blob.getDigest(), getName());

    return blob.getDigest();
  }

  private void putBlobSynchronized(Blob blob) {
    contentAddressableStorage.put(blob);
    backplane.addBlobLocation(blob.getDigest(), getName());
    contentAddressableStorage.release(blob.getDigest());
  }

  @Override
  public Digest putBlob(ByteString content) {
    if (content.size() == 0) {
      return digestUtil.empty();
    }

    return putRawBlob(new Blob(content, digestUtil));
  }

  private TokenizableIterator<Directory> createTreeIterator(
      Digest rootDigest, String pageToken) {
    return new TreeIterator((digest) -> fetchBlob(digest), rootDigest, pageToken);
  }

  @Override
  public String getTree(
      Digest rootDigest,
      int pageSize,
      String pageToken,
      ImmutableList.Builder<Directory> directories) {
    if (pageSize == 0) {
      pageSize = 1024; // getTreeDefaultPageSize();
    }
    if (pageSize >= 0 && pageSize > 1024 /* getTreeMaxPageSize() */) {
      pageSize = 1024; // getTreeMaxPageSize();
    }

    TokenizableIterator<Directory> iter =
        createTreeIterator(rootDigest, pageToken);

    while (iter.hasNext() && pageSize != 0) {
      Directory directory = iter.next();
      // If part of the tree is missing from the CAS, the server will return the
      // portion present and omit the rest.
      if (directory != null) {
        directories.add(directory);
        if (pageSize > 0) {
          pageSize--;
        }
      }
    }
    return iter.toNextPageToken();
  }

  @Override
  public OutputStream getStreamOutput(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream newStreamInput(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void execute(
      Action action,
      boolean skipCacheLookup,
      int totalInputFileCount,
      long totalInputFileBytes,
      Consumer<Operation> onOperation) {
    throw new UnsupportedOperationException();
  }

  private void matchInterruptible(Platform platform, boolean requeueOnFailure, Predicate<Operation> onMatch) throws InterruptedException {
    String operationName = null;
    do {
      operationName = backplane.dispatchOperation();
    } while (operationName == null);

    // FIXME platform match

    Operation operation = backplane.getOperation(operationName);
    if (!onMatch.test(operation) && requeueOnFailure) {
      backplane.putOperation(operation);
    }
  }

  @Override
  public void match(Platform platform, boolean requeueOnFailure, Predicate<Operation> onMatch) {
    try {
      matchInterruptible(platform, requeueOnFailure, onMatch);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public boolean putOperation(Operation operation) {
    return backplane.putOperation(operation);
  }

  @Override
  public boolean pollOperation(String operationName, Stage stage) {
    throw new UnsupportedOperationException();
  }

  // returns nextPageToken suitable for list restart
  @Override
  public String listOperations(
      int pageSize,
      String pageToken,
      String filter,
      ImmutableList.Builder<Operation> operations) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Operation getOperation(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void cancelOperation(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteOperation(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean watchOperation(
      String operationName,
      boolean watchInitialState,
      Predicate<Operation> watcher) {
    throw new UnsupportedOperationException();
  }
}
