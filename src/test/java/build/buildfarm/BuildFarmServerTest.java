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

import static com.google.common.truth.Truth.assertThat;

import build.buildfarm.common.Digests;
import build.buildfarm.server.BuildFarmServer;
import build.buildfarm.v1test.BuildFarmServerConfig;
import build.buildfarm.v1test.MemoryInstanceConfig;
import com.google.devtools.remoteexecution.v1test.BatchUpdateBlobsRequest;
import com.google.devtools.remoteexecution.v1test.BatchUpdateBlobsResponse;
import com.google.devtools.remoteexecution.v1test.ContentAddressableStorageGrpc;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.FindMissingBlobsRequest;
import com.google.devtools.remoteexecution.v1test.FindMissingBlobsResponse;
import com.google.devtools.remoteexecution.v1test.UpdateBlobRequest;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import java.util.Collections;
import java.util.Iterator;

@RunWith(JUnit4.class)
public class BuildFarmServerTest {
  private BuildFarmServer server;
  private ManagedChannel inProcessChannel;
  private MemoryInstanceConfig memoryInstanceConfig;

  @Before
  public void setUp() throws Exception {
    String uniqueServerName = "in-process server for " + getClass();

    memoryInstanceConfig = MemoryInstanceConfig.newBuilder()
        .setListOperationsDefaultPageSize(1024)
        .setListOperationsMaxPageSize(16384)
        .setTreeDefaultPageSize(1024)
        .setTreeMaxPageSize(16384)
        .setOperationExecutingTimeout(Duration.newBuilder()
            .setSeconds(30)
            .setNanos(0))
        .setOperationCompletedDelay(Duration.newBuilder()
            .setSeconds(10)
            .setNanos(0))
        .build();

    BuildFarmServerConfig.Builder configBuilder =
        BuildFarmServerConfig.newBuilder().setPort(0);
    configBuilder.addInstancesBuilder()
        .setName("memory")
        .setMemoryInstanceConfig(memoryInstanceConfig);

    server = new BuildFarmServer(
        InProcessServerBuilder.forName(uniqueServerName).directExecutor(),
        configBuilder.build());
    server.start();
    inProcessChannel = InProcessChannelBuilder.forName(uniqueServerName)
        .directExecutor().build();
  }

  @After
  public void tearDown() throws Exception {
    inProcessChannel.shutdownNow();
    server.stop();
  }

  @Test
  public void findMissingBlobs() {
    ByteString content = ByteString.copyFromUtf8("Hello, World!");
    Iterable<Digest> digests =
        Collections.singleton(Digests.computeDigest(content));
    FindMissingBlobsRequest request = FindMissingBlobsRequest.newBuilder()
        .setInstanceName("memory")
        .addAllBlobDigests(digests)
        .build();
    ContentAddressableStorageGrpc.ContentAddressableStorageBlockingStub stub =
        ContentAddressableStorageGrpc.newBlockingStub(inProcessChannel);

    FindMissingBlobsResponse response = stub.findMissingBlobs(request);

    assertThat(response.getMissingBlobDigestsList())
        .containsExactlyElementsIn(digests);
  }

  @Test
  public void batchUpdateBlobs() {
    ByteString content = ByteString.copyFromUtf8("Hello, World!");
    Digest digest = Digests.computeDigest(content);
    BatchUpdateBlobsRequest request = BatchUpdateBlobsRequest.newBuilder()
        .setInstanceName("memory")
        .addRequests(UpdateBlobRequest.newBuilder()
            .setContentDigest(digest)
            .setData(content)
            .build())
        .build();
    ContentAddressableStorageGrpc.ContentAddressableStorageBlockingStub stub =
        ContentAddressableStorageGrpc.newBlockingStub(inProcessChannel);

    BatchUpdateBlobsResponse response = stub.batchUpdateBlobs(request);

    BatchUpdateBlobsResponse.Response expected = BatchUpdateBlobsResponse.Response.newBuilder()
        .setBlobDigest(digest)
        .setStatus(com.google.rpc.Status.newBuilder()
            .setCode(com.google.rpc.Code.OK.getNumber())
            .build())
        .build();
    assertThat(response.getResponsesList())
        .containsExactlyElementsIn(Collections.singleton(expected));
  }
}
