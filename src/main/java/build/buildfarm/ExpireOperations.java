package build.buildfarm;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.Retrier;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.v1test.RedisShardBackplaneConfig;
import build.buildfarm.instance.shard.RedisShardBackplane;
import com.google.common.collect.ImmutableList;
import com.google.devtools.remoteexecution.v1test.ExecuteResponse;
import com.google.longrunning.Operation;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.util.concurrent.TimeUnit;

class ExpireOperations {
  private static ManagedChannel createChannel(String target) {
    NettyChannelBuilder builder =
        NettyChannelBuilder.forTarget(target)
            .negotiationType(NegotiationType.PLAINTEXT);
    return builder.build();
  }

  public static void main(String[] args) throws Exception {
    String host = args[0];
    String instanceName = args[1];
    DigestUtil digestUtil = DigestUtil.forHash(args[2]);
    ManagedChannel channel = createChannel(host);
    Instance instance = new StubInstance(
        instanceName,
        digestUtil,
        channel,
        10, TimeUnit.SECONDS,
        Retrier.NO_RETRIES,
        new ByteStreamUploader("", channel, null, 300, Retrier.NO_RETRIES, null));
    RedisShardBackplaneConfig config = RedisShardBackplaneConfig.newBuilder()
        .setRedisUri("redis://buildfarm.pit-irn-1.uberatc.net:6379")
        .setWorkersSetName("Workers")
        .setActionCachePrefix("ActionCache")
        .setOperationsHashName("Operations")
        .setQueuedOperationsListName("QueuedOperations")
        .setDispatchedOperationsHashName("DispatchedOperations")
        .setCompletedOperationsListName("CompletedOperations")
        .setOperationChannelName("OperationChannel")
        .setCasPrefix("ContentAddressableStorage")
        .setTreePrefix("Tree")
        .build();
    RedisShardBackplane backplane = new RedisShardBackplane(config, null, null, null);
    ImmutableList.Builder<String> legacyCompleted = new ImmutableList.Builder<>();
    int legacyCount = 0;
    for (String operationName : backplane.getCompletedOperations()) {
      Operation operation = backplane.getOperation(operationName);
      switch (operation.getResultCase()) {
      case ERROR:
        System.out.println("Ignoring " + operationName + " which has error case, probably need to add a cancelled at...");
        break;
      case RESPONSE:
        if (operation.getResponse().is(ExecuteResponse.class)) {
          System.out.println("Operation " + operationName + " has legacy response");
          legacyCompleted.add(operationName);
          legacyCount++;
        }
        if (legacyCount == 10000) {
          System.out.println("Deleting 10000 legacy operations...");
          backplane.deleteAllCompletedOperation(legacyCompleted.build());
          legacyCompleted = new ImmutableList.Builder<>();
          legacyCount = 0;
        }
        break;
      case RESULT_NOT_SET:
        System.out.println("Result not set on " + operationName);
        break;
      }
    }
    backplane.deleteAllCompletedOperation(legacyCompleted.build());
  }
};
