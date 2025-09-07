// Copyright 2019 The Buildfarm Authors. All rights reserved.
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
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.v1test.Digest;
import build.buildfarm.worker.FuseCAS;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

class Mount {
  @SuppressWarnings("BusyWait")
  public static void mount(
      String host, String instanceName, String root, Digest inputRoot, String name)
      throws IOException, InterruptedException {
    ManagedChannel channel = createChannel(host);
    Instance instance = new StubInstance(instanceName, channel);

    Path cwd = Path.of(".");

    FuseCAS fuse =
        new FuseCAS(
            cwd.resolve(root),
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

    fuse.createInputRoot(name, inputRoot);

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

  public static void main(String[] args) throws Exception {
    if (args.length != 5) {
      System.err.println("Usage: bf-mount <endpoint> <instance-name> <root> <digest> <name>");
      System.err.println("\nMount an REAPI directory specified by 'digest' at 'name' under 'root'");
      System.exit(1);
    }

    String host = args[0];
    String instanceName = args[1];
    String root = args[2];
    Digest inputRoot = DigestUtil.parseDigest(args[3]);
    String name = args[4];
    mount(host, instanceName, root, inputRoot, name);
  }
}
