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

package build.buildfarm;

import static java.lang.String.format;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.redis.RedisClient;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.shard.JedisClusterFactory;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.v1test.OperationTimesBetweenStages;
import build.buildfarm.v1test.RedisShardBackplaneConfig;
import build.buildfarm.v1test.ShardWorker;
import build.buildfarm.v1test.ShardWorkerConfig;
import build.buildfarm.v1test.StageInformation;
import build.buildfarm.v1test.WorkerProfileMessage;
import build.buildfarm.worker.shard.WorkerOptions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.devtools.common.options.OptionsParser;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.naming.ConfigurationException;
import redis.clients.jedis.JedisCluster;

class WorkerProfile {
  private static ManagedChannel createChannel(String target) {
    NettyChannelBuilder builder =
        NettyChannelBuilder.forTarget(target).negotiationType(NegotiationType.PLAINTEXT);
    return builder.build();
  }

  /**
   * Transform worker string from "ip-10-135-31-210.ec2:8981" to "10.135.31.210".
   *
   * @param worker
   * @return
   */
  private static String workerStringTransformation(String worker) {
    StringBuilder sb = new StringBuilder();
    sb.append(worker.split("\\.")[0].substring("ip-".length()).replaceAll("-", "."))
        .append(':')
        .append(worker.split(":")[1]);
    return sb.toString();
  }

  private static void analyzeMessage(String worker, WorkerProfileMessage response) {
    System.out.println("\nWorkerProfile:");
    System.out.println(worker);
    String strIntFormat = "%-50s : %d";
    String strFloatFormat = "%-50s : %2.1f";
    long entryCount = response.getCasEntryCount();
    long unreferencedEntryCount = response.getCasUnreferencedEntryCount();
    System.out.println(String.format(strIntFormat, "Current Total Entry Count", entryCount));
    System.out.println(
        String.format(strIntFormat, "Current Unreferenced Entry Count", unreferencedEntryCount));
    if (entryCount != 0) {
      System.out.println(
          String.format(
              strFloatFormat,
              "Percentage of Unreferenced Entry",
              1.0 * response.getCasEntryCount() / response.getCasUnreferencedEntryCount()));
    }
    System.out.println(
        String.format(
            strIntFormat, "Current DirectoryEntry Count", response.getCasDirectoryEntryCount()));
    System.out.println(
        String.format(
            strIntFormat, "Number of Evicted Entries", response.getCasEvictedEntryCount()));
    System.out.println(
        String.format(
            strIntFormat,
            "Total Evicted Entries size in Bytes",
            response.getCasEvictedEntrySize()));

    List<StageInformation> stages = response.getStagesList();
    for (StageInformation stage : stages) {
      printStageInformation(stage);
    }

    List<OperationTimesBetweenStages> times = response.getTimesList();
    for (OperationTimesBetweenStages time : times) {
      printOperationTime(time);
    }
  }

  private static RedisShardBackplaneConfig toRedisShardBackplaneConfig(
      Readable input, WorkerOptions options) throws IOException {
    ShardWorkerConfig.Builder builder = ShardWorkerConfig.newBuilder();
    TextFormat.merge(input, builder);
    if (!Strings.isNullOrEmpty(options.root)) {
      builder.setRoot(options.root);
    }
    if (!Strings.isNullOrEmpty(options.publicName)) {
      builder.setPublicName(options.publicName);
    }
    return builder.build().getRedisShardBackplaneConfig();
  }

  private static Set<String> getWorkers(String[] args) throws ConfigurationException, IOException {
    OptionsParser parser = OptionsParser.newOptionsParser(WorkerOptions.class);
    parser.parseAndExitUponError(args);
    List<String> residue = parser.getResidue();
    if (residue.isEmpty()) {
      throw new IllegalArgumentException("Missing Config_PATH");
    }
    Path configPath = Paths.get(residue.get(3));
    RedisShardBackplaneConfig config = null;
    try (InputStream configInputStream = Files.newInputStream(configPath)) {
      config =
          toRedisShardBackplaneConfig(
              new InputStreamReader(configInputStream), parser.getOptions(WorkerOptions.class));
    } catch (Exception e) {
      e.printStackTrace();
    }

    RedisClient client = new RedisClient(JedisClusterFactory.create(config).get());
    RedisShardBackplaneConfig finalConfig = config;
    return client.call(jedis -> fetchWorkers(jedis, finalConfig, System.currentTimeMillis()));
  }

  private static Set<String> fetchWorkers(
      JedisCluster jedis, RedisShardBackplaneConfig config, long now) {
    Set<String> workers = Sets.newConcurrentHashSet();
    for (Map.Entry<String, String> entry : jedis.hgetAll(config.getWorkersHashName()).entrySet()) {
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

  private static void workerProfile(String[] args) throws IOException {
    Set<String> workers = null;
    DigestUtil digestUtil = DigestUtil.forHash(args[2]);
    WorkerProfileMessage currentWorkerMessage;
    HashMap<String, Instance> workersToChannels = new HashMap<>();
    String type = args[1];

    while (true) {
      // update worker list
      if (workers == null) {
        try {
          workers = getWorkers(args);
        } catch (ConfigurationException e) {
          e.printStackTrace();
        }
      }
      if (workers == null || workers.size() == 0) {
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

  private static void getWorkerProfile(Instance instance) {
    // List<String> worker = instance.
    WorkerProfileMessage response = instance.getWorkerProfile();
    System.out.println("\nWorkerProfile:");
    String strIntFormat = "%-50s : %d";
    String strFloatFormat = "%-50s : %2.1f";
    long entryCount = response.getCasEntryCount();
    long unreferencedEntryCount = response.getCasUnreferencedEntryCount();
    System.out.println(format(strIntFormat, "Current Total Entry Count", entryCount));
    System.out.println(format(strIntFormat, "Current Total Size", response.getCasSize()));
    System.out.println(format(strIntFormat, "Max Size", response.getCasMaxSize()));
    System.out.println(format(strIntFormat, "Max Entry Size", response.getCasMaxEntrySize()));
    System.out.println(
        format(strIntFormat, "Current Unreferenced Entry Count", unreferencedEntryCount));
    if (entryCount != 0) {
      System.out.println(
          format(
                  strFloatFormat,
                  "Percentage of Unreferenced Entry",
                  100.0f * response.getCasEntryCount() / response.getCasUnreferencedEntryCount())
              + "%");
    }
    System.out.println(
        format(strIntFormat, "Current DirectoryEntry Count", response.getCasDirectoryEntryCount()));
    System.out.println(
        format(strIntFormat, "Number of Evicted Entries", response.getCasEvictedEntryCount()));
    System.out.println(
        format(
            strIntFormat,
            "Total Evicted Entries size in Bytes",
            response.getCasEvictedEntrySize()));

    List<StageInformation> stages = response.getStagesList();
    for (StageInformation stage : stages) {
      printStageInformation(stage);
    }

    List<OperationTimesBetweenStages> times = response.getTimesList();
    for (OperationTimesBetweenStages time : times) {
      printOperationTime(time);
    }
  }

  private static void printStageInformation(StageInformation stage) {
    System.out.println(
        format("%s slots configured: %d", stage.getName(), stage.getSlotsConfigured()));
    System.out.println(format("%s slots used %d", stage.getName(), stage.getSlotsUsed()));
  }

  private static void printOperationTime(OperationTimesBetweenStages time) {
    String periodInfo = "\nIn last ";
    switch ((int) time.getPeriod().getSeconds()) {
      case 60:
        periodInfo += "1 minute";
        break;
      case 600:
        periodInfo += "10 minutes";
        break;
      case 3600:
        periodInfo += "1 hour";
        break;
      case 10800:
        periodInfo += "3 hours";
        break;
      case 86400:
        periodInfo += "24 hours";
        break;
      default:
        System.out.println("The period is UNKNOWN: " + time.getPeriod().getSeconds());
        periodInfo = periodInfo + time.getPeriod().getSeconds() + " seconds";
        break;
    }

    periodInfo += ":";
    System.out.println(periodInfo);
    System.out.println("Number of operations completed: " + time.getOperationCount());
    String strStrNumFormat = "%-28s -> %-28s : %12.2f ms";
    System.out.println(
        format(strStrNumFormat, "Queued", "MatchStage", durationToMillis(time.getQueuedToMatch())));
    System.out.println(
        format(
            strStrNumFormat,
            "MatchStage",
            "InputFetchStage start",
            durationToMillis(time.getMatchToInputFetchStart())));
    System.out.println(
        format(
            strStrNumFormat,
            "InputFetchStage Start",
            "InputFetchStage Complete",
            durationToMillis(time.getInputFetchStartToComplete())));
    System.out.println(
        format(
            strStrNumFormat,
            "InputFetchStage Complete",
            "ExecutionStage Start",
            durationToMillis(time.getInputFetchCompleteToExecutionStart())));
    System.out.println(
        format(
            strStrNumFormat,
            "ExecutionStage Start",
            "ExecutionStage Complete",
            durationToMillis(time.getExecutionStartToComplete())));
    System.out.println(
        format(
            strStrNumFormat,
            "ExecutionStage Complete",
            "ReportResultStage Start",
            durationToMillis(time.getExecutionCompleteToOutputUploadStart())));
    System.out.println(
        format(
            strStrNumFormat,
            "OutputUploadStage Start",
            "OutputUploadStage Complete",
            durationToMillis(time.getOutputUploadStartToComplete())));
    System.out.println();
  }

  private static float durationToMillis(Duration d) {
    return Durations.toNanos(d) / (1000.0f * 1000.0f);
  }

  // how to run the binary: bf-workerprofile WorkerProfile shard SHA256
  // examples/shard-worker.config.example
  public static void main(String[] args) throws Exception {
    workerProfile(args);
  }
}
