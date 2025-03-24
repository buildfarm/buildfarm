package build.buildfarm.common.config;

import java.util.Set;
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

  private INSTANCE_TYPE instanceType;
  private String name;
  private boolean actionCacheReadOnly;
  private String bindAddress;
  private int port;
  private GrpcMetrics grpcMetrics = new GrpcMetrics();
  private int casWriteTimeout;
  private int bytestreamTimeout;
  private String sslCertificatePath;
  private String sslPrivateKeyPath;
  private boolean runDispatchedMonitor;
  private int dispatchedMonitorIntervalSeconds;
  private boolean runFailsafeOperation;
  private boolean runOperationQueuer;
  private boolean ensureOutputsPresent;
  private boolean mergeExecutions;
  private int maxRequeueAttempts;
  private boolean useDenyList;
  private long grpcTimeout;
  private long executeKeepaliveAfterSeconds;
  private boolean recordBesEvents;
  private Admin admin = new Admin();
  private Metrics metrics = new Metrics();
  private int maxCpu;
  private String clusterId;
  private String cloudRegion;
  private String publicName;
  private int maxInboundMessageSizeBytes;
  private int maxInboundMetadataSize;
  private ServerCacheConfigs caches = new ServerCacheConfigs();
  private boolean findMissingBlobsViaBackplane;
  private int gracefulShutdownSeconds;
  private Set<String> correlatedInvocationsIndexScopes;

  public String getSession() {
    return String.format("buildfarm-server-%s-%s", getPublicName(), sessionGuid);
  }
}
