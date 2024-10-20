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

// This tool can be used to interact directly with the Action Cache API.
// ./tool <URL> shard SHA256 <command>
class Ac {
  public static void main(String[] args) throws Exception {
    // get arguments for establishing an instance
    String host = args[0];
    String instanceName = args[1];
    DigestUtil digestUtil = DigestUtil.forHash(args[2]);

    // create instance
    ManagedChannel channel = createChannel(host);
    Instance instance = new StubInstance(instanceName, channel);

    // upload fake data to the Action Cache.
    DigestUtil.ActionKey key =
        DigestUtil.asActionKey(digestUtil.compute(ByteString.copyFromUtf8("Hello, World")));
    ActionResult.Builder result = ActionResult.newBuilder();
    instance.putActionResult(key, result.build());

    instance.stop();
  }
}
