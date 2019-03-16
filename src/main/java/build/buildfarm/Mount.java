package build.buildfarm;

import static build.buildfarm.instance.Utils.getBlob;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.Retrier;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.worker.FuseCAS;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class Mount {
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

    Path cwd = Paths.get(".");

    FuseCAS fuse = new FuseCAS(cwd.resolve(args[3]), new InputStreamFactory() {
      Map<Digest, ByteString> cache = new HashMap<>();

      public synchronized InputStream newInput(Digest blobDigest, long offset) {
        if (cache.containsKey(blobDigest)) {
          return cache.get(blobDigest).substring((int) offset).newInput();
        }
        try {
          ByteString value = getBlob(instance, blobDigest);
          if (offset == 0) {
            cache.put(blobDigest, value);
          }
          return value.newInput();
        } catch (IOException e) {
          return null;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return null;
        }
      }
    });
    
    // FIXME make bettar
    fuse.createInputRoot(args[5], DigestUtil.parseDigest(args[4]));

    try {
      for (;;) {
        Thread.currentThread().sleep(1000);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      fuse.stop();
    }
  }
};
