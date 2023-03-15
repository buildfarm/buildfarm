// Copyright 2023 The Bazel Authors. All rights reserved.
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

package build.buildfarm.server.services;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import build.bazel.remote.asset.v1.FetchBlobRequest;
import build.bazel.remote.asset.v1.FetchBlobResponse;
import build.bazel.remote.asset.v1.Qualifier;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.instance.Instance;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.stubbing.Answer;

@RunWith(JUnit4.class)
public class FetchServiceTest {
  private static final DigestUtil DIGEST_UTIL = new DigestUtil(HashFunction.SHA256);

  @Test
  public void existingEntryShouldCompleteWithoutFetch() throws InterruptedException {
    Instance instance = mock(Instance.class);
    FetchService service = new FetchService(instance);

    ByteString content = ByteString.copyFromUtf8("Fetch Blob Content");
    Digest contentDigest = DIGEST_UTIL.compute(content);
    Digest containsDigest = contentDigest.toBuilder().setSizeBytes(-1).build();

    FetchBlobRequest request =
        FetchBlobRequest.newBuilder()
            .addQualifiers(
                Qualifier.newBuilder()
                    .setName("checksum.sri")
                    .setValue(hashChecksumSRI(contentDigest.getHash()))
                    .build())
            .build();

    doAnswer(
            (Answer<Boolean>)
                invocation -> {
                  Digest.Builder result = (Digest.Builder) invocation.getArguments()[1];
                  result.mergeFrom(contentDigest);
                  return true;
                })
        .when(instance)
        .containsBlob(eq(containsDigest), any(Digest.Builder.class), any(RequestMetadata.class));
    StreamObserver<FetchBlobResponse> response = mock(StreamObserver.class);
    service.fetchBlob(request, response);
    verify(instance, never())
        .fetchBlob(any(Iterable.class), any(Digest.class), any(RequestMetadata.class));
    verify(response, times(1)).onCompleted();
    verify(response, times(1))
        .onNext(FetchBlobResponse.newBuilder().setBlobDigest(contentDigest).build());
  }

  private String hashChecksumSRI(String hash) {
    return "sha256-" + BaseEncoding.base64().encode(BaseEncoding.base16().lowerCase().decode(hash));
  }
}
