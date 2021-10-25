package tech.aurora.bfadmin.service;

import build.buildfarm.v1test.ReindexCasRequestResults;
import tech.aurora.bfadmin.model.ClusterDetails;
import tech.aurora.bfadmin.model.ClusterInfo;

import java.util.List;

public interface AdminService {

  List<ClusterInfo> getAllClustersWithDetails();

  List<String> getAllClusters();

  ClusterInfo getClusterInfo();

  ClusterInfo getClusterInfo(String clusterId);

  ClusterDetails getClusterDetails();

  void reindexAllCas();

  String scaleGroup(String asgName, Integer desiredInstances);

  int terminateInstance(String instanceId, String grpcEndpoint, int grpcPort);

  String getInstanceIdByPrivateDnsName(String dnsName);

  int stopDockerContainer(String instanceId, String containerType, String grpcEndpoint, int grpcPort);
}
