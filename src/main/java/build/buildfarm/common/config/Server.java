package build.buildfarm.common.config;

import com.google.common.base.Strings;
import lombok.Data;

@Data
public class Server {
  public enum INSTANCE_TYPE {
    SHARD,
    MEMORY
  }

  private INSTANCE_TYPE instanceType;
  private String name;
  private boolean actionCacheReadOnly;
  private int port;
  private int prometheusPort;
  private GrpcMetrics grpcMetrics = new GrpcMetrics();
  private int casWriteTimeout;
  private int bytestreamTimeout;
  private String sslCertificatePath;
  private boolean runDispatchedMonitor;
  private int dispatchedMonitorIntervalSeconds;
  private boolean runOperationQueuer;
  private boolean ensureOutputsPresent;
  private long maxEntrySizeBytes;
  private int maxRequeueAttempts;
  private boolean useDenyList;
  private long grpcTimeout;
  private long executeKeepaliveAfterSeconds;
  private boolean recordBesEvents;
  private Admin admin;
  private Metrics metrics;
  private int maxCpu;
  private String clusterId;
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
