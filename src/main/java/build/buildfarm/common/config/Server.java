package build.buildfarm.common.config;

import com.google.common.base.Strings;
import lombok.Data;

@Data
public class Server {
  public enum INSTANCE_TYPE {
    SHARD,
    MEMORY
  }

  private INSTANCE_TYPE instanceType = INSTANCE_TYPE.SHARD;
  private String name = "shard";
  private boolean actionCacheReadOnly = false;
  private int port = 8980;
  private int prometheusPort = 9090;
  private GrpcMetrics grpcMetrics = new GrpcMetrics();
  private int casWriteTimeout = 3600;
  private int bytestreamTimeout = 3600;
  private String sslCertificatePath = null;
  private boolean runDispatchedMonitor = true;
  private int dispatchedMonitorIntervalSeconds = 1;
  private boolean runOperationQueuer = true;
  private boolean ensureOutputsPresent = false;
  private long maxEntrySizeBytes = 2147483648L; // 2 * 1024 * 1024 * 1024
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

  public String getPublicName() {
    if (!Strings.isNullOrEmpty(publicName)) {
      return publicName;
    } else {
      return System.getenv("INSTANCE_NAME");
    }
  }
}
