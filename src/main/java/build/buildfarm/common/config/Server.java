package build.buildfarm.common.config;

import com.google.common.collect.ImmutableSet;
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

  private INSTANCE_TYPE instanceType = INSTANCE_TYPE.SHARD;
  private String name = "shard";
  private boolean actionCacheReadOnly = false;
  private String bindAddress = "";
  private int port = 8980;
  private GrpcMetrics grpcMetrics = new GrpcMetrics();
  private int casWriteTimeout = 3600;
  private int bytestreamTimeout = 3600;
  private String sslCertificatePath = null;
  private String sslPrivateKeyPath = null;
  private boolean runDispatchedMonitor = true;
  private int dispatchedMonitorIntervalSeconds = 1;
  private boolean runFailsafeOperation = true;
  private boolean runOperationQueuer = true;
  private boolean ensureOutputsPresent = true;
  private boolean mergeExecutions = true;
  private int maxRequeueAttempts = 3;
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
  private int maxInboundMessageSizeBytes = 0;
  private int maxInboundMetadataSize = 0;
  private ServerCacheConfigs caches = new ServerCacheConfigs();
  private boolean findMissingBlobsViaBackplane = false;
  private int gracefulShutdownSeconds = 0;
  private Set<String> correlatedInvocationsIndexScopes = ImmutableSet.of("host", "username");

  public String getSession() {
    return String.format("buildfarm-server-%s-%s", getPublicName(), sessionGuid);
  }
}
