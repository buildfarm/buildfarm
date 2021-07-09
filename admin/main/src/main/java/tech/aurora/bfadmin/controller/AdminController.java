package tech.aurora.bfadmin.controller;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import tech.aurora.bfadmin.service.AdminService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.annotation.PostConstruct;
import java.util.List;

@Controller
public class AdminController {
  private static final Logger logger = LoggerFactory.getLogger(AdminController.class);
  
  @Autowired
  AdminService adminService;

  @Value("${cluster.id}")
  private String clusterId;

  @Value("${deployment.domain}")
  private String deploymentDomain;

  @Value("${deployment.port}")
  private int deploymentPort;

  @Value("${deployment.tag.instance.type.server}")
  private String serverTagValue;

  @Value("${deployment.tag.instance.type.cpuworker}")
  private String cpuWorkerTagValue;

  @Value("${deployment.tag.instance.type.gpuworker}")
  private String gpuWorkerTagValue;

  @Value("${deployment.tag.cluster.name}")
  private String clusterTagName;

  @Value("${aws.region}")
  private String region;

  private AmazonEC2 ec2;
  private String workerAsg;
  private String gpuWorkerAsg;
  private String serverAsg;

  @RequestMapping("/")
  public String getMainApp() {
    return "redirect:/dashboard";
  }
  
  @RequestMapping("/dashboard")
  public String getDashboard(Model model) {
    model.addAttribute("workers", adminService.getInstances(workerAsg, 0, deploymentDomain, deploymentPort));
    model.addAttribute("gpuWorkers", adminService.getInstances(gpuWorkerAsg, 0, deploymentDomain, deploymentPort));
    model.addAttribute("servers", adminService.getInstances(serverAsg, 0, deploymentDomain, deploymentPort));
    model.addAttribute("serversAutoScalingGroup", serverAsg);
    model.addAttribute("workersAutoScalingGroup", workerAsg);
    model.addAttribute("gpuWorkersAutoScalingGroup", gpuWorkerAsg);
    model.addAttribute("clusterId", clusterId);
    return "dashboard";
  }

  @PostConstruct
  public void init() {
    logger.info("Initializing aws sdk for region {}", region);
    ec2 = AmazonEC2ClientBuilder.standard().withRegion(region).build();

    Instance serverInfo = getInstanceByTag("buildfarm.instance_type", serverTagValue);
    if (serverInfo != null) {
      this.serverAsg = getTagValue("aws:autoscaling:groupName", serverInfo.getTags());
    }
    Instance workerInfo = getInstanceByTag("buildfarm.worker_type", cpuWorkerTagValue);
    if (workerInfo != null) {
      this.workerAsg = getTagValue("aws:autoscaling:groupName", workerInfo.getTags());
    }
    Instance gpuWorkerInfo = getInstanceByTag("buildfarm.worker_type", gpuWorkerTagValue);
    if (gpuWorkerInfo != null) {
      this.gpuWorkerAsg = getTagValue("aws:autoscaling:groupName", gpuWorkerInfo.getTags());
    }
    logger.info("Found Buildfarm deployment in AWS account: " +
                    "clusterId: {}, worker asg: {}, gpu worker asg: {}, server asg: {}, grpc endpoint: {}:{}",
            clusterId, workerAsg, gpuWorkerAsg, serverAsg, deploymentDomain, deploymentPort);
  }

  public String getBaseClusterId() {
    return clusterId.replace("buildfarm-", "");
  }

  private Instance getInstanceByTag(String tagName, String tagValue) {
    DescribeInstancesResult instancesResult = ec2.describeInstances(
            new DescribeInstancesRequest().withFilters(
                    new Filter().withName("instance-state-name").withValues("running"),
                    new Filter().withName("tag:" + clusterTagName).withValues(clusterId),
                    new Filter().withName("tag:" + tagName).withValues(tagValue)));
    for (Reservation r : instancesResult.getReservations()) {
      for (Instance e : r.getInstances()) {
        if (e != null) {
          return e;
        }
      }
    }
    return null;
  }

  private String getTagValue(String tagName, List<Tag> tags) {
    for (Tag tag : tags) {
      if (tagName.equalsIgnoreCase(tag.getKey())) {
        return tag.getValue();
      }
    }
    return "";
  }
}
