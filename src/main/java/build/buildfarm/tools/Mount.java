// Copyright 2019 The Bazel Authors. All rights reserved.
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

package build.buildfarm.tools;

import static build.buildfarm.common.grpc.Channels.createChannel;
import static build.buildfarm.instance.Utils.getBlob;
import static com.google.common.base.Preconditions.checkArgument;

import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.worker.FuseCAS;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

class Mount {
  @SuppressWarnings("BusyWait")
  public static void main(String[] args) throws Exception {
    String host = args[0];
    String instanceName = args[1];
    DigestUtil digestUtil = DigestUtil.forHash(args[2]);
    ManagedChannel channel = createChannel(host);
    Instance instance = new StubInstance(instanceName, digestUtil, channel);

    Path cwd = Paths.get(".");

    FuseCAS fuse =
        new FuseCAS(
            cwd.resolve(args[3]),
            new InputStreamFactory() {
              final Map<Digest, ByteString> cache = new HashMap<>();

              public synchronized InputStream newInput(
                  Compressor.Value compressor, Digest blobDigest, long offset) {
                checkArgument(compressor == Compressor.Value.IDENTITY);
                if (cache.containsKey(blobDigest)) {
                  return cache.get(blobDigest).substring((int) offset).newInput();
                }
                try {
                  ByteString value =
                      getBlob(
                          instance, compressor, blobDigest, RequestMetadata.getDefaultInstance());
                  if (offset == 0) {
                    cache.put(blobDigest, value);
                  }
                  return value.newInput();
                } catch (IOException e) {
                  return null;
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  return null;
                }
              }
            });

    // FIXME make bettar
    fuse.createInputRoot(args[5], DigestUtil.parseDigest(args[4]));

    try {
      //noinspection InfiniteLoopStatement
      for (; ; ) {
        Thread.sleep(1000);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      fuse.stop();
    }
  }
}
