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

import build.buildfarm.common.CasIndexResults;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.StubInstance;
import io.grpc.ManagedChannel;

// This tool can be used to remove worker entries from the CAS.
// This is usually done via the admin service when a worker is departing from the cluster.
// ./tool <URL> shard SHA256 <worker instance name>
// The results of the removal are printed after the CAS entries have been removed.
class IndexWorker {
  public static void main(String[] args) throws Exception {
    String host = args[0];
    String instanceName = args[1];
    ManagedChannel channel = createChannel(host);
    Instance instance = new StubInstance(instanceName, channel);
    CasIndexResults results = instance.reindexCas();
    System.out.println(results.toMessage());
    instance.stop();
  }
}
