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

package build.buildfarm;

import build.buildfarm.common.Digests;
import build.buildfarm.server.BuildFarmInstances;
import build.buildfarm.server.ByteStreamService;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.util.Random;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ByteStreamServiceTest {

  private static byte[] generateFileData(Random random) {
    byte[] data = new byte[1024 * 1024];
    random.nextBytes(data);
    return data;
  }

  @Test
  public void testValidWrite() {
    BuildFarmInstances instances = mock(BuildFarmInstances.class);

    ByteStreamService service = new ByteStreamService(instances);

    StreamObserver<WriteResponse> responseObserver = mock(StreamObserver.class);
    verify(responseObserver, never()).onError(any());

    StreamObserver<WriteRequest> requestObserver = service.write(responseObserver);

    String instance = "foo_instance";
    String uuid = "1d4e65cd-7c0d-44ca-b49b-b912988c8967"; // dummy value

    Random random = new Random(49);
    byte[] data = generateFileData(random);
    ByteString bytes = ByteString.copyFrom(data);
    Digest digest = Digests.computeDigest(bytes);
    String hash = digest.toString();
    int size = data.length;

    WriteRequest request = WriteRequest.newBuilder()
      .setResourceName(String.format("%s/uploads/%s/blobs/%s/%d", instance, uuid, hash, size))
      .setWriteOffset(0)
      .setData(bytes)
      .build();
    requestObserver.onNext(request);
    requestObserver.onCompleted();

    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }
}

