package tech.aurora.bfadmin.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import tech.aurora.bfadmin.service.AdminService;

@RestController
@RequestMapping("admin")
public class AdminApi {
  private static final Logger logger = LoggerFactory.getLogger(AdminApi.class);
  
  @Autowired
  AdminService adminService;

  @Value("${deployment.domain}")
  private String deploymentDomain;

  @Value("${deployment.port}")
  private int deploymentPort;

  @Value("${buildfarm.run.docker.regex}")
  private String containerRegex;

  @RequestMapping("/restart/worker/{instanceId}")
  public int restartWorker(@PathVariable String instanceId) {
    if (instanceId.contains("ip")) {
      instanceId = adminService.getInstanceIdByPrivateDnsName(instanceId);
      logger.info("Retrieved instance id {}", instanceId);
    }
    return adminService.stopDockerContainer(instanceId, containerRegex, deploymentDomain, deploymentPort);
  }

  @RequestMapping("/restart/server/{instanceId}")
  public int restartScheduler(@PathVariable String instanceId) {
    if (instanceId.contains("ip")) {
      instanceId = adminService.getInstanceIdByPrivateDnsName(instanceId);
      logger.info("Retrieved instance id {}", instanceId);
    }
    return adminService.stopDockerContainer(instanceId, containerRegex, deploymentDomain, deploymentPort);
  }

  @RequestMapping("/terminate/{instanceId}")
  public int terminateInstance(@PathVariable String instanceId) {
    if (instanceId.contains("ip")) {
      instanceId = adminService.getInstanceIdByPrivateDnsName(instanceId);
      logger.info("Retrieved instance id {}", instanceId);
    }
    return adminService.terminateInstance(instanceId, deploymentDomain, deploymentPort);
  }

  @RequestMapping("/scale/{autoScaleGroup}/{numInstances}")
  public String scaleWorkers(@PathVariable String autoScaleGroup,
      @PathVariable Integer numInstances) {
    return adminService.scaleGroup(autoScaleGroup, numInstances);
  }
}
