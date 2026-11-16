// Copyright 2022-2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
  private int casReadTimeout = 60;
  private int expectTimeout = 60;
  private int cacheCheckTimeout = 60;
  private int queueTimeout = 60;
  private int requeueTimeout = 60;
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
  private boolean grpcChannelz = false;
  private int gracefulShutdownSeconds = 0;
  private Set<String> correlatedInvocationsIndexScopes = ImmutableSet.of("host", "username");
  private int fastCdc2020AvgChunkSizeBytes = 512 * 1024;
  private int fastCdc2020Seed = 0;
  private int fastCdc2020Normalization = 2;

  public String getSession() {
    return String.format("buildfarm-server-%s-%s", getPublicName(), sessionGuid);
  }
}
