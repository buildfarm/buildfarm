// Copyright 2022 The Buildfarm Authors. All rights reserved.
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

import build.bazel.remote.execution.v2.ActionResult;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.StubInstance;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

/** Interact directly with the Action Cache API. */
@Command(
    name = "ac",
    mixinStandardHelpOptions = true,
    description = "Interact directly with the Action Cache API")
class Ac implements Callable<Integer> {

  @Parameters(
      index = "0",
      description =
          "The [scheme://]host:port of the buildfarm server. Scheme should be 'grpc://',\""
              + " 'grpcs://', or omitted (default 'grpc://')")
  private String host;

  @Parameters(index = "1", description = "The instance name")
  private String instanceName;

  @Parameters(index = "2", description = "The digest hash function (e.g., SHA256)")
  private String hashFunction;

  @Override
  public Integer call() throws Exception {
    DigestUtil digestUtil = DigestUtil.forHash(hashFunction);

    // create instance
    ManagedChannel channel = createChannel(host);
    Instance instance = new StubInstance(instanceName, channel);

    try {
      // upload fake data to the Action Cache.
      DigestUtil.ActionKey key =
          DigestUtil.asActionKey(digestUtil.compute(ByteString.copyFromUtf8("Hello, World")));
      ActionResult.Builder result = ActionResult.newBuilder();
      instance.putActionResult(key, result.build());
    } finally {
      instance.stop();
    }
    return 0;
  }

  public static void main(String[] args) {
    int exitCode = new CommandLine(new Ac()).execute(args);
    System.exit(exitCode);
  }
}
