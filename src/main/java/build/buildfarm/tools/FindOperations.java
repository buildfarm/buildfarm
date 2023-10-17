// Copyright 2020 The Bazel Authors. All rights reserved.
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

import build.buildfarm.common.DigestUtil;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.StubInstance;
import com.google.common.collect.ImmutableList;
import com.google.longrunning.Operation;
import io.grpc.ManagedChannel;

// This tool can be used to find Operations based on their particular properties.
// For example, it could find all of the operations executed by a particular user or particular
// program.
// ./tool <URL> shard SHA256 <user>
// The operations that match the query will be printed.
class FindOperations {
  public static void main(String[] args) throws Exception {
    // get arguments for establishing an instance
    String host = args[0];
    String instanceName = args[1];
    DigestUtil digestUtil = DigestUtil.forHash(args[2]);

    // decide filter predicate
    String filterPredicate = "*";
    if (args.length >= 4) {
      filterPredicate = args[3];
    }

    // create instance
    ManagedChannel channel = createChannel(host);
    Instance instance = new StubInstance(instanceName, digestUtil, channel);

    // get operations and print them
    ImmutableList.Builder<Operation> operations = new ImmutableList.Builder<>();
    instance.listOperations(100, "/operations", filterPredicate, operations);
    for (Operation operation : operations.build()) {
      System.out.println(operation.getName());
    }

    instance.stop();
  }
}
