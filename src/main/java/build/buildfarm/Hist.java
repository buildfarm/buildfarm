package build.buildfarm;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.Retrier;
import build.buildfarm.instance.stub.StubInstance;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Command;
import com.google.devtools.remoteexecution.v1test.Command.EnvironmentVariable;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.DirectoryNode;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata;
import com.google.devtools.remoteexecution.v1test.FileNode;
import com.google.devtools.remoteexecution.v1test.OutputFile;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.util.concurrent.TimeUnit;

class Hist {
  private static ManagedChannel createChannel(String target) {
    NettyChannelBuilder builder =
        NettyChannelBuilder.forTarget(target)
            .negotiationType(NegotiationType.PLAINTEXT);
    return builder.build();
  }

  private static void printHistogramValue(int executing, int queued) {
    StringBuilder s = new StringBuilder();
    int p = 0;
    int n = 5;
    while (p > 0) {
      p /= 10;
      n++;
    }
    int h = executing > 100 ? (100 - n) : executing;
    for( int i = 0; i < h; i++ ) {
      s.append('#');
    }
    if (executing > 100) {
      s.replace(0, n, "# (" + executing + ") ");
    }
    System.out.println(s.toString());
  }

  private static void printHistogram(Instance instance) {
    int executing = 0;
    int queued = 0;

    String pageToken = "";
    do {
      ImmutableList.Builder<Operation> operations = new ImmutableList.Builder<>();
      for(;;) {
        try {
          pageToken = instance.listOperations(1024, pageToken, "", operations);
        } catch (io.grpc.StatusRuntimeException e) {
          continue;
        }
        break;
      }
      for (Operation operation : operations.build()) {
        try {
          ExecuteOperationMetadata metadata = operation.getMetadata().unpack(ExecuteOperationMetadata.class);
          switch (metadata.getStage()) {
            case QUEUED:
              queued++;
              break;
            case EXECUTING:
              executing++;
              break;
            default:
              break;
          }
        } catch (InvalidProtocolBufferException e) {
        }
      }
    } while(!pageToken.equals(""));
    printHistogramValue(executing, queued);
  }

  public static void main(String[] args) throws java.io.IOException {
    String instanceName = args[0];
    String host = args[1];
    DigestUtil digestUtil = DigestUtil.forHash(args[2]);
    ManagedChannel channel = createChannel(host);
    Instance instance = new StubInstance(
        instanceName,
        digestUtil,
        channel,
        10, TimeUnit.SECONDS,
        Retrier.NO_RETRIES,
        new ByteStreamUploader("", channel, null, 300, Retrier.NO_RETRIES, null));
    try {
      for(;;) {
        printHistogram(instance);
        Thread.sleep(500);
      }
    } catch (InterruptedException e) {
    }
  }
};
