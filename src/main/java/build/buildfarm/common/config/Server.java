package build.buildfarm.common.config;

import com.google.common.base.Strings;
import java.net.InetAddress;
import java.util.UUID;
import lombok.Data;
import lombok.extern.java.Log;

@Data
@Log
public class Server {
  public enum INSTANCE_TYPE {
    SHARD
  }

  private static UUID sessionGuid = UUID.randomUUID();

  private INSTANCE_TYPE instanceType = INSTANCE_TYPE.SHARD;
  private String name = "shard";
  private boolean actionCacheReadOnly = false;
  private int port = 8980;
  private GrpcMetrics grpcMetrics = new GrpcMetrics();
  private int casWriteTimeout = 3600;
  private int bytestreamTimeout = 3600;
  private String sslCertificatePath = null;
  private String sslPrivateKeyPath = null;
  private boolean runDispatchedMonitor = true;
  private int dispatchedMonitorIntervalSeconds = 1;
  private boolean runOperationQueuer = true;
  private boolean ensureOutputsPresent = false;
  private int maxRequeueAttempts = 5;
  private boolean useDenyList = true;
  private long grpcTimeout = 3600;
  private long executeKeepaliveAfterSeconds = 60;
  private boolean recordBesEvents = false;
  private Admin admin = new Admin();
  private Metrics metrics = new Metrics();
  private int maxCpu;
  private String clusterId = "";
  private String cloudRegion;
  private String publicName;

  private ServerCacheConfigs caches = new ServerCacheConfigs();

  public String getPublicName() {
    // use environment override (useful for containerized deployment)
    if (!Strings.isNullOrEmpty(System.getenv("INSTANCE_NAME"))) {
      return System.getenv("INSTANCE_NAME");
    }

    // use configured value
    if (!Strings.isNullOrEmpty(publicName)) {
      return publicName;
    }

    // derive a value
    try {
      return InetAddress.getLocalHost().getHostAddress() + ":" + port;
    } catch (Exception e) {
      log.severe("publicName could not be derived:" + e);
      return publicName;
    }
  }

  public String getSession() {
    return String.format("buildfarm-server-%s-%s", getPublicName(), sessionGuid);
  }
}
