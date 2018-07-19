package build.buildfarm;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.instance.shard.ShardInstance;
import build.buildfarm.v1test.ShardInstanceConfig;
import build.buildfarm.v1test.RedisShardBackplaneConfig;
import com.google.common.collect.ImmutableList;
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.Command;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.GetTreeResponse;
import com.google.devtools.remoteexecution.v1test.Platform;
import com.google.protobuf.util.JsonFormat;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

class Bench {
  public static void main(String[] args) throws Exception {
    DigestUtil digestUtil = new DigestUtil(HashFunction.SHA1);
    /*
    String[] cmdArgs = new String[]{
      "bazel-out/host/bin/python/hub/hidlc2/hidlc2_py",
      "--output",
      "bazel-out/k8-fastbuild_re/genfiles/experimental/adupuis/messages",
      "-g",
      "boostpython",
      "--split-python-module",
      "--gen-python-module",
      "--python-module-name",
      "_odtac_overhead_ladar_message",
      "-g",
      "cpp",
      "-g",
      "index",
      "-I",
      "bazel-out/k8-fastbuild_re/genfiles",
      "experimental/adupuis/messages/overhead_ladar.hidl",
    };
    System.out.println(DigestUtil.toString(digestUtil.compute(Command.newBuilder()
        .addAllArguments(Arrays.asList(cmdArgs))
        .build())));
    */
    ShardInstance instance = new ShardInstance(
        "kraid",
        digestUtil,
        ShardInstanceConfig.newBuilder()
            .setRunDispatchedMonitor(false)
            .setRedisShardBackplaneConfig(RedisShardBackplaneConfig.newBuilder()
                .setRedisUri("redis://buildfarm.pit-irn-1.uberatc.net:6379")
                .setWorkersSetName("Workers")
                .setActionCachePrefix("ActionCache")
                .setOperationPrefix("Operation")
                .setQueuedOperationsListName("QueuedOperations")
                .setDispatchedOperationsHashName("DispatchedOperations")
                .setOperationChannelPrefix("OperationChannel")
                .setCasPrefix("ContentAddressableStorage")
                .setTreePrefix("Tree")
                .build())
            .build(), () -> {});
    /*
    String[] outputFiles = new String[]{
      "bazel-out/k8-fastbuild_re/genfiles/experimental/adupuis/messages/_odtac_overhead_ladar_message_python_module.cc", 
      "bazel-out/k8-fastbuild_re/genfiles/experimental/adupuis/messages/overhead_ladar.cc",
      "bazel-out/k8-fastbuild_re/genfiles/experimental/adupuis/messages/overhead_ladar.hh",
      "bazel-out/k8-fastbuild_re/genfiles/experimental/adupuis/messages/overhead_ladar.hidl.d",
      "bazel-out/k8-fastbuild_re/genfiles/experimental/adupuis/messages/overhead_ladar.hidlindex",
      "bazel-out/k8-fastbuild_re/genfiles/experimental/adupuis/messages/overhead_ladar_python.cc",
      "bazel-out/k8-fastbuild_re/genfiles/experimental/adupuis/messages/overhead_ladar_python.hh",
    };
    Action action = Action.newBuilder()
      .setInputRootDigest(DigestUtil.parseDigest("1a97e8e3ba65ad91bf90bf942827daa2d3e30506/177"))
      .setCommandDigest(DigestUtil.parseDigest("016c3fa174c7b5e90199c2e8579bc325e29d0196/349"))
      .addAllOutputFiles(Arrays.asList(outputFiles))
      .setPlatform(Platform.getDefaultInstance())
      .setDoNotCache(true)
      .build();
    System.out.println(DigestUtil.toString(digestUtil.compute(action)));
    */

    // instance.start(); // not really required
    // System.out.println("Caching input tree");
    // instance.cacheOperationActionInputTree(args[0]);
    System.out.println("Starting Validation");
    // boolean validation = instance.validateOperation(args[0], false);
    instance.checkMissingBlob(DigestUtil.parseDigest(args[0]), args.length > 0 && args[1].equals("true"));
    // System.out.println("Validation: " + (validation ? "successful" : "failed"));
    instance.stop();
  }
}
