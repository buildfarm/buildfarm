package build.buildfarm;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.Retrier;
import build.buildfarm.instance.stub.StubInstance;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.util.concurrent.TimeUnit;

class Cancel {
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
    for( int i = 3; i < args.length; i++ ) {
      instance.cancelOperation(args[i]);
    }
  }
};
