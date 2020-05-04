package build.buildfarm;

import build.buildfarm.v1test.*;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CASMemoryProfileClient {
  private static final Logger logger = Logger.getLogger(CASMemoryProfileClient.class.getName());

  private final CASMemoryProfileGrpc.CASMemoryProfileBlockingStub blockingStub;

  public CASMemoryProfileClient(Channel channel) {
    blockingStub = CASMemoryProfileGrpc.newBlockingStub(channel);
  }

  public void getCASMemoryProfile(int interval) {
    MemoryUsageRequest request = MemoryUsageRequest.newBuilder().build();
    MemoryUsageMessage response;

    while (true) {
      try {
        response = blockingStub.getCASMemoryUsage(request);
      } catch (StatusRuntimeException e) {
        logger.log(Level.WARNING, e.getMessage());
        return;
      }

      MemoryUsage u0 = response.getEntry(0);
      MemoryUsage u1 = response.getEntry(1);

      logger.log(Level.INFO, u0.getMemoryUsedFor() + " " + u0.getMemoryUsage());
      logger.log(Level.INFO, u1.getMemoryUsedFor() + " " + u1.getMemoryUsage());

      try {
        TimeUnit.SECONDS.sleep(interval);
      } catch (InterruptedException e) {
        logger.log(Level.WARNING, e.getMessage());
        return;
      }
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      logger.log(
          Level.SEVERE,
          "running pariodic profile needs two arguments: \n"
              + "1) the ip address of worker you want to profile and \n"
              + "2) number of seconds you want to wait between two profile. \n"
              + "example: CAS_profile_client 0.0.0.0 20");

      return;
    }

    String target = args[0] + ":8981";

    logger.log(Level.INFO, "Tying to profile memory usage of CASFileCache or worker: " + target);
    int profileInterval = Integer.parseInt(args[1]);
    ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();

    try {
      CASMemoryProfileClient client = new CASMemoryProfileClient(channel);
      client.getCASMemoryProfile(profileInterval);
    } finally {
      channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
  }
}
