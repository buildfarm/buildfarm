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
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

/** Mount an REAPI directory via FUSE. */
@Command(
    name = "mount",
    mixinStandardHelpOptions = true,
    description = "Mount an REAPI directory specified by digest at a mount point")
class Mount implements Callable<Integer> {

  @Parameters(
      index = "0",
      description =
          "The [scheme://]host:port of the buildfarm server. Scheme should be 'grpc://',\""
              + " 'grpcs://', or omitted (default 'grpc://')")
  private String host;

  @Parameters(index = "1", description = "The instance name")
  private String instanceName;

  @Parameters(index = "2", description = "Root directory path where the mount will be created")
  private String root;

  @Parameters(index = "3", description = "Digest of the directory to mount")
  private String digestStr;

  @Parameters(index = "4", description = "Name of the mount point (subdirectory under root)")
  private String name;

  @Override
  public Integer call() throws Exception {
    Digest inputRoot = DigestUtil.parseDigest(digestStr);
    mountDirectory(host, instanceName, root, inputRoot, name);
    return 0;
  }

  @SuppressWarnings("BusyWait")
  private static void mountDirectory(
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

  public static void main(String[] args) {
    int exitCode = new CommandLine(new Mount()).execute(args);
    System.exit(exitCode);
  }
}
