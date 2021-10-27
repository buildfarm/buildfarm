package tech.aurora.bfadmin.rest;

import build.buildfarm.v1test.ReindexCasRequestResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import tech.aurora.bfadmin.model.ClusterDetails;
import tech.aurora.bfadmin.model.ClusterInfo;
import tech.aurora.bfadmin.service.AdminService;


import java.util.List;

@RestController
@RequestMapping(value = "admin", method = RequestMethod.POST)
public class AdminApi {
  private static final Logger logger = LoggerFactory.getLogger(AdminApi.class);
  
  @Autowired
  AdminService adminService;

  @Value("${deployment.domain}")
  private String deploymentDomain;

  @Value("${buildfarm.public.port}")
  private int deploymentPort;

  @Value("${buildfarm.docker.name.regex}")
  private String containerRegex;

  @RequestMapping("/clusters/info")
  public List<String> getAllClusters() {
    return adminService.getAllClusters();
  }

  @RequestMapping("/clusters/details")
  public List<ClusterInfo> getAllClustersWithDetails() {
    return adminService.getAllClustersWithDetails();
  }

  @RequestMapping("/cluster/info")
  public ClusterInfo getClusterInfo() {
    return adminService.getClusterInfo();
  }

  @RequestMapping("/cluster/details")
  public ClusterDetails getClusterDetails() {
    return adminService.getClusterDetails();
  }

  @RequestMapping("/reindexallcas")
  public void reindexAllCas() {
    adminService.reindexAllCas();
  }

  @RequestMapping("/restart/{instanceId}")
  public int restartContainer(@PathVariable String instanceId) {
    if (instanceId.contains("ip")) {
      instanceId = adminService.getInstanceIdByPrivateDnsName(instanceId);
    }
    return adminService.stopDockerContainer(instanceId, containerRegex, deploymentDomain, deploymentPort);
  }

  @RequestMapping("/terminate/{instanceId}")
  public int terminateInstance(@PathVariable String instanceId) {
    if (instanceId.contains("ip")) {
      instanceId = adminService.getInstanceIdByPrivateDnsName(instanceId);
    }
    return adminService.terminateInstance(instanceId, deploymentDomain, deploymentPort);
  }

  @RequestMapping("/scale/{asgName}/{numInstances}")
  public String scaleGroup(@PathVariable String asgName,
      @PathVariable Integer numInstances) {
    return adminService.scaleGroup(asgName, numInstances);
  }
}
