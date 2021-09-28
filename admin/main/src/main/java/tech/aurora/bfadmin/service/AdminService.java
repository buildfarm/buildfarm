package tech.aurora.bfadmin.service;

import tech.aurora.bfadmin.model.ClusterDetails;
import tech.aurora.bfadmin.model.ClusterInfo;
import tech.aurora.bfadmin.model.Instance;
import java.util.List;

public interface AdminService {

  ClusterInfo getClusterInfo();

  ClusterDetails getClusterDetails();

  String scaleGroup(String asgName, Integer desiredInstances);

  int terminateInstance(String instanceId, String grpcEndpoint, int grpcPort);

  String getInstanceIdByPrivateDnsName(String dnsName);

  int stopDockerContainer(String instanceId, String containerType, String grpcEndpoint, int grpcPort);
}
