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
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.common.config.ShardWorkerOptions;
import build.buildfarm.common.redis.RedisClient;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.shard.JedisClusterFactory;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.v1test.OperationTimesBetweenStages;
import build.buildfarm.v1test.ShardWorker;
import build.buildfarm.v1test.StageInformation;
import build.buildfarm.v1test.WorkerProfileMessage;
import com.google.common.collect.Sets;
import com.google.devtools.common.options.OptionsParser;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.JsonFormat;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.naming.ConfigurationException;
import redis.clients.jedis.JedisCluster;

class WorkerProfile {
  private static BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  /**
   * Transform worker string from "ip-10-135-31-210.ec2:8981" to "10.135.31.210".
   *
   * @param worker
   * @return
   */
  @SuppressWarnings("JavaDoc")
  private static String workerStringTransformation(String worker) {
    return worker.split("\\.")[0].substring("ip-".length()).replaceAll("-", ".")
        + ':'
        + worker.split(":")[1];
  }

  private static void analyzeMessage(String worker, WorkerProfileMessage response) {
    System.out.println("\nWorkerProfile:");
    System.out.println(worker);
    String strIntFormat = "%-50s : %d";
    String strFloatFormat = "%-50s : %2.1f";
    long entryCount = response.getCasEntryCount();
    long unreferencedEntryCount = response.getCasUnreferencedEntryCount();
    System.out.printf((strIntFormat) + "%n", "Current Total Entry Count", entryCount);
    System.out.printf(
        (strIntFormat) + "%n", "Current Unreferenced Entry Count", unreferencedEntryCount);
    if (entryCount != 0) {
      System.out.printf(
          (strFloatFormat) + "%n",
          "Percentage of Unreferenced Entry",
          1.0 * response.getCasEntryCount() / response.getCasUnreferencedEntryCount());
    }
    System.out.printf(
        (strIntFormat) + "%n",
        "Current DirectoryEntry Count",
        response.getCasDirectoryEntryCount());
    System.out.printf(
        (strIntFormat) + "%n", "Number of Evicted Entries", response.getCasEvictedEntryCount());
    System.out.printf(
        (strIntFormat) + "%n",
        "Total Evicted Entries size in Bytes",
        response.getCasEvictedEntrySize());

    List<StageInformation> stages = response.getStagesList();
    for (StageInformation stage : stages) {
      WorkerProfilePrinter.printStageInformation(stage);
    }

    List<OperationTimesBetweenStages> times = response.getTimesList();
    for (OperationTimesBetweenStages time : times) {
      WorkerProfilePrinter.printOperationTime(time);
    }
  }

  @SuppressWarnings("ConstantConditions")
  private static Set<String> getWorkers(String[] args) throws ConfigurationException, IOException {
    OptionsParser parser = OptionsParser.newOptionsParser(ShardWorkerOptions.class);
    parser.parseAndExitUponError(args);
    List<String> residue = parser.getResidue();
    if (residue.isEmpty()) {
      throw new IllegalArgumentException("Missing Config_PATH");
    }
    try {
      configs.loadConfigs(Paths.get(residue.get(3)));
    } catch (IOException e) {
      System.out.println("Could not parse yml configuration file." + e);
    }
    RedisClient client = new RedisClient(JedisClusterFactory.create("worker-profile").get());
    return client.call(jedis -> fetchWorkers(jedis, System.currentTimeMillis()));
  }

  private static Set<String> fetchWorkers(JedisCluster jedis, long now) {
    Set<String> workers = Sets.newConcurrentHashSet();
    for (Map.Entry<String, String> entry :
        jedis.hgetAll(configs.getBackplane().getWorkersHashName() + "_storage").entrySet()) {
      String json = entry.getValue();
      try {
        if (json != null) {
          ShardWorker.Builder builder = ShardWorker.newBuilder();
          JsonFormat.parser().merge(json, builder);
          ShardWorker worker = builder.build();
          if (worker.getExpireAt() > now) {
            workers.add(worker.getEndpoint());
          }
        }
      } catch (InvalidProtocolBufferException e) {
        e.printStackTrace();
      }
    }
    return workers;
  }

  @SuppressWarnings("BusyWait")
  private static void workerProfile(String[] args) throws IOException {
    Set<String> workers = null;
    DigestUtil digestUtil = DigestUtil.forHash(args[2]);
    WorkerProfileMessage currentWorkerMessage;
    HashMap<String, Instance> workersToChannels = new HashMap<>();
    String type = args[1];

    //noinspection InfiniteLoopStatement
    while (true) {
      // update worker list
      if (workers == null) {
        try {
          workers = getWorkers(args);
        } catch (ConfigurationException e) {
          e.printStackTrace();
        }
      }
      if (workers == null || workers.isEmpty()) {
        System.out.println(
            "cannot find any workers, check the redis url and make sure there are workers in the cluster");
      } else {
        // remove the unregistered workers
        for (String existingWorker : workersToChannels.keySet()) {
          if (!workers.contains(existingWorker)) {
            workersToChannels.remove(existingWorker);
          }
        }

        // add new registered workers and profile
        for (String worker : workers) {
          if (!workersToChannels.containsKey(worker)) {
            workersToChannels.put(
                worker,
                new StubInstance(
                    type,
                    "bf-workerprofile",
                    digestUtil,
                    createChannel(workerStringTransformation(worker)),
                    Durations.fromMinutes(1)));
          }
          try {
            currentWorkerMessage = workersToChannels.get(worker).getWorkerProfile();
            System.out.println(worker);
            analyzeMessage(worker, currentWorkerMessage);
          } catch (StatusRuntimeException e) {
            e.printStackTrace();
            System.out.println("==============TIMEOUT");
          }
        }
      }

      // sleep
      try {
        System.out.println("Waiting for 10 minutes:");
        Thread.sleep(10 * 60 * 1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  // how to run the binary: bf-workerprofile WorkerProfile shard SHA256
  // examples/config.yml
  public static void main(String[] args) throws Exception {
    workerProfile(args);
  }
}
