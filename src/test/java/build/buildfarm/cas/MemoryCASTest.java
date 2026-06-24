// Copyright 2018 The Buildfarm Authors. All rights reserved.
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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.BatchReadBlobsResponse.Response;
import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.common.DigestUtil;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import io.grpc.stub.ServerCallStreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MemoryCASTest {
  @Test
  public void expireShouldCallOnExpiration() throws IOException, InterruptedException {
    ContentAddressableStorage storage = new MemoryCAS(10);

    DigestUtil digestUtil = new DigestUtil(DigestUtil.HashFunction.SHA256);
    Runnable mockOnExpiration = mock(Runnable.class);
    storage.put(new Blob(ByteString.copyFromUtf8("stdout"), digestUtil), mockOnExpiration);
    verify(mockOnExpiration, never()).run();
    storage.put(new Blob(ByteString.copyFromUtf8("stderr"), digestUtil));
    verify(mockOnExpiration, times(1)).run();
  }

  @Test
  public void expireShouldOccurAtLimitExactly() throws IOException, InterruptedException {
    ContentAddressableStorage storage = new MemoryCAS(11);

    DigestUtil digestUtil = new DigestUtil(DigestUtil.HashFunction.SHA256);
    Runnable mockOnExpiration = mock(Runnable.class);
    storage.put(new Blob(ByteString.copyFromUtf8("stdin"), digestUtil), mockOnExpiration);
    storage.put(new Blob(ByteString.copyFromUtf8("stdout"), digestUtil), mockOnExpiration);
    verify(mockOnExpiration, never()).run();
    storage.put(new Blob(ByteString.copyFromUtf8("a"), digestUtil));
    verify(mockOnExpiration, times(1)).run();
  }

  @Test
  public void duplicateEntryRegistersMultipleOnExpiration()
      throws IOException, InterruptedException {
    ContentAddressableStorage storage = new MemoryCAS(10);

    DigestUtil digestUtil = new DigestUtil(DigestUtil.HashFunction.SHA256);
    Runnable mockOnExpiration = mock(Runnable.class);
    storage.put(new Blob(ByteString.copyFromUtf8("stdout"), digestUtil), mockOnExpiration);
    storage.put(new Blob(ByteString.copyFromUtf8("stdout"), digestUtil), mockOnExpiration);
    verify(mockOnExpiration, never()).run();
    storage.put(new Blob(ByteString.copyFromUtf8("stderr"), digestUtil));
    verify(mockOnExpiration, times(2)).run();
  }

  @Test(expected = IllegalArgumentException.class)
  public void emptyPutThrowsIllegalArgumentException() throws IOException, InterruptedException {
    ContentAddressableStorage storage = new MemoryCAS(10);

    DigestUtil digestUtil = new DigestUtil(DigestUtil.HashFunction.SHA256);
    storage.put(new Blob(ByteString.EMPTY, digestUtil));
  }

  @Test(expected = IllegalArgumentException.class)
  public void emptyGetThrowsIllegalArgumentException() {
    ContentAddressableStorage storage = new MemoryCAS(10);

    DigestUtil digestUtil = new DigestUtil(DigestUtil.HashFunction.SHA256);
    storage.get(digestUtil.compute(ByteString.EMPTY));
  }

  private static final DigestUtil DIGEST_UTIL = new DigestUtil(DigestUtil.HashFunction.SHA256);

  private static Blob blobOf(String content) {
    return new Blob(ByteString.copyFromUtf8(content), DIGEST_UTIL);
  }

  @Test
  public void containsReportsStoredBlobWithSize() throws Exception {
    MemoryCAS storage = new MemoryCAS(1024);
    Blob blob = blobOf("present");
    storage.put(blob);

    build.bazel.remote.execution.v2.Digest.Builder result =
        build.bazel.remote.execution.v2.Digest.newBuilder();
    assertThat(storage.contains(blob.getDigest(), result)).isTrue();
    assertThat(result.getHash()).isEqualTo(blob.getDigest().getHash());
    assertThat(result.getSizeBytes()).isEqualTo(blob.getDigest().getSize());
  }

  @Test
  public void containsReportsFalseForAbsentBlob() {
    MemoryCAS storage = new MemoryCAS(1024);

    assertThat(
            storage.contains(
                blobOf("absent").getDigest(), build.bazel.remote.execution.v2.Digest.newBuilder()))
        .isFalse();
  }

  @Test
  public void findMissingBlobsReturnsOnlyAbsentNonEmptyDigests() throws Exception {
    MemoryCAS storage = new MemoryCAS(1024);
    Blob present = blobOf("here");
    storage.put(present);
    Blob absent = blobOf("gone");

    Iterable<build.bazel.remote.execution.v2.Digest> missing =
        storage.findMissingBlobs(
            ImmutableList.of(
                DigestUtil.toDigest(present.getDigest()), DigestUtil.toDigest(absent.getDigest())),
            DigestFunction.Value.SHA256);

    assertThat(missing).containsExactly(DigestUtil.toDigest(absent.getDigest()));
  }

  @Test
  public void findMissingBlobsIgnoresEmptyDigest() throws Exception {
    MemoryCAS storage = new MemoryCAS(1024);

    Iterable<build.bazel.remote.execution.v2.Digest> missing =
        storage.findMissingBlobs(
            ImmutableList.of(build.bazel.remote.execution.v2.Digest.getDefaultInstance()),
            DigestFunction.Value.SHA256);

    assertThat(missing).isEmpty();
  }

  @Test
  public void newInputReturnsDataFromOffset() throws Exception {
    MemoryCAS storage = new MemoryCAS(1024);
    Blob blob = blobOf("0123456789");
    storage.put(blob);

    try (InputStream in = storage.newInput(Compressor.Value.IDENTITY, blob.getDigest(), 4)) {
      assertThat(new String(in.readAllBytes())).isEqualTo("456789");
    }
  }

  @Test
  public void newInputRejectsOutOfBoundsOffset() {
    MemoryCAS storage = new MemoryCAS(1024);
    Blob blob = blobOf("data");
    storage.put(blob);

    assertThrows(
        IndexOutOfBoundsException.class,
        () -> storage.newInput(Compressor.Value.IDENTITY, blob.getDigest(), blob.size() + 1));
  }

  @Test
  public void newInputThrowsNoSuchFileForAbsentBlob() {
    MemoryCAS storage = new MemoryCAS(1024);

    assertThrows(
        NoSuchFileException.class,
        () -> storage.newInput(Compressor.Value.IDENTITY, blobOf("absent").getDigest(), 0));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void getStreamsStoredBlobToObserver() {
    MemoryCAS storage = new MemoryCAS(1024);
    Blob blob = blobOf("streamed");
    storage.put(blob);

    ServerCallStreamObserver<ByteString> observer = mock(ServerCallStreamObserver.class);
    storage.get(
        Compressor.Value.IDENTITY,
        blob.getDigest(),
        /* offset= */ 0,
        /* limit= */ 0,
        observer,
        RequestMetadata.getDefaultInstance());

    verify(observer, times(1)).onNext(blob.getData());
    verify(observer, times(1)).onCompleted();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void getErrorsObserverForAbsentBlob() {
    MemoryCAS storage = new MemoryCAS(1024);

    ServerCallStreamObserver<ByteString> observer = mock(ServerCallStreamObserver.class);
    storage.get(
        Compressor.Value.IDENTITY,
        blobOf("absent").getDigest(),
        /* offset= */ 0,
        /* limit= */ 0,
        observer,
        RequestMetadata.getDefaultInstance());

    verify(observer, times(1)).onError(org.mockito.ArgumentMatchers.any());
  }

  @Test
  public void getResponseReturnsOkWhenBlobPresent() {
    build.bazel.remote.execution.v2.Digest digest =
        DigestUtil.toDigest(blobOf("payload").getDigest());

    Response response =
        MemoryCAS.getResponse(
            digest, DigestFunction.Value.SHA256, d -> ByteString.copyFromUtf8("payload"));

    assertThat(response.getStatus()).isEqualTo(ContentAddressableStorage.OK);
    assertThat(response.getData()).isEqualTo(ByteString.copyFromUtf8("payload"));
  }

  @Test
  public void getResponseReturnsNotFoundWhenBlobMissing() {
    build.bazel.remote.execution.v2.Digest digest =
        DigestUtil.toDigest(blobOf("payload").getDigest());

    Response response = MemoryCAS.getResponse(digest, DigestFunction.Value.SHA256, d -> null);

    assertThat(response.getStatus()).isEqualTo(ContentAddressableStorage.NOT_FOUND);
  }

  @Test
  public void getResponseCapturesGetterFailureStatus() {
    build.bazel.remote.execution.v2.Digest digest =
        DigestUtil.toDigest(blobOf("payload").getDigest());

    Response response =
        MemoryCAS.getResponse(
            digest,
            DigestFunction.Value.SHA256,
            d -> {
              throw new RuntimeException("boom");
            });

    // A failed getter is reported as a non-OK status rather than propagating.
    assertThat(response.getStatus()).isNotEqualTo(ContentAddressableStorage.OK);
  }

  @Test
  public void getAllReturnsResponsePerDigestInOrder() {
    Blob present = blobOf("yes");
    Blob absent = blobOf("no");

    java.util.List<Response> responses =
        MemoryCAS.getAll(
            ImmutableList.of(
                DigestUtil.toDigest(present.getDigest()), DigestUtil.toDigest(absent.getDigest())),
            DigestFunction.Value.SHA256,
            d -> {
              if (d.equals(DigestUtil.toDigest(present.getDigest()))) {
                return present.getData();
              }
              return null;
            });

    assertThat(responses).hasSize(2);
    assertThat(responses.get(0).getStatus()).isEqualTo(ContentAddressableStorage.OK);
    assertThat(responses.get(1).getStatus()).isEqualTo(ContentAddressableStorage.NOT_FOUND);
  }

  @Test
  public void getAllFutureResolvesImmediately() throws Exception {
    MemoryCAS storage = new MemoryCAS(1024);
    Blob blob = blobOf("future");
    storage.put(blob);

    java.util.List<Response> responses =
        storage
            .getAllFuture(
                ImmutableList.of(DigestUtil.toDigest(blob.getDigest())),
                DigestFunction.Value.SHA256)
            .get();

    assertThat(responses).hasSize(1);
    assertThat(responses.get(0).getStatus()).isEqualTo(ContentAddressableStorage.OK);
  }

  @Test
  public void delegateServesBlobWhenAbsentLocally() throws Exception {
    ContentAddressableStorage delegate = mock(ContentAddressableStorage.class);
    Blob blob = blobOf("delegated");
    when(delegate.get(blob.getDigest())).thenReturn(blob);
    MemoryCAS storage = new MemoryCAS(1024, d -> {}, delegate);

    assertThat(storage.get(blob.getDigest())).isEqualTo(blob);
    verify(delegate, times(1)).get(blob.getDigest());
  }

  @Test
  public void maxEntrySizeIsUnlimited() {
    MemoryCAS storage = new MemoryCAS(1024);

    assertThat(storage.maxEntrySize())
        .isEqualTo(ContentAddressableStorage.UNLIMITED_ENTRY_SIZE_MAX);
  }

  @Test
  public void isReadOnlyIsFalse() {
    assertThat(new MemoryCAS(1024).isReadOnly()).isFalse();
  }
}
