package tech.aurora.bfadmin.service.impl;

import build.buildfarm.v1test.AdminGrpc;
import build.buildfarm.v1test.GetClientStartTimeRequest;
import build.buildfarm.v1test.GetClientStartTimeResult;
import build.buildfarm.v1test.GetClientStartTime;
import build.buildfarm.v1test.StopContainerRequest;
import build.buildfarm.v1test.TerminateHostRequest;
import build.buildfarm.v1test.ReindexAllCasRequest;
import build.buildfarm.v1test.ReindexCasRequestResults;
import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClientBuilder;
import com.amazonaws.services.autoscaling.model.AutoScalingGroup;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsResult;
import com.amazonaws.services.autoscaling.model.TagDescription;
import com.amazonaws.services.autoscaling.model.UpdateAutoScalingGroupRequest;
import com.amazonaws.services.autoscaling.model.UpdateAutoScalingGroupResult;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.Tag;
import com.google.rpc.Status;
import tech.aurora.bfadmin.model.Asg;
import tech.aurora.bfadmin.model.ClusterDetails;
import tech.aurora.bfadmin.model.ClusterInfo;
import tech.aurora.bfadmin.model.Instance;
import tech.aurora.bfadmin.service.AdminService;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Service
public class AdminServiceImpl implements AdminService {
  private static final Logger logger = LoggerFactory.getLogger(AdminServiceImpl.class);

  @Value("${buildfarm.cluster.name}")
  private String clusterId;

  @Value("${buildfarm.worker.port}")
  private int workerPort;

  @Value("${aws.region}")
  private String region;

  @Value("${deployment.domain}")
  private String deploymentDomain;

  @Value("${buildfarm.public.port}")
  private int deploymentPort;

  private AmazonEC2 ec2;
  private AmazonAutoScaling autoScale;

  @PostConstruct
  public void init() {
    logger.info("Using AWS region: {}", region);
    ec2 = AmazonEC2ClientBuilder.standard().withRegion(region).build();
    autoScale = AmazonAutoScalingClientBuilder.standard().withRegion(region).build();
  }

  @Override
  public List<String> getAllClusters() {
    List<String> clusters = new ArrayList<>();
    for (AutoScalingGroup asg : autoScale.describeAutoScalingGroups(new DescribeAutoScalingGroupsRequest().withMaxRecords(100)).getAutoScalingGroups()) {
      String clusterId = getAsgTagValue("buildfarm.cluster_id", asg.getTags());
      if (!clusterId.isEmpty() && !clusters.contains(clusterId)) {
        clusters.add(clusterId);
      }
    }
    return clusters;
  }

  @Override
  public List<ClusterInfo> getAllClustersWithDetails() {
    List<ClusterInfo> clusters = new ArrayList<>();
    for (String clusterId : getAllClusters()) {
      clusters.add(getClusterInfo(clusterId));
    }
    return clusters;
  }

  @Override
  public ClusterInfo getClusterInfo() {
    return getClusterInfo(clusterId);
  }

  @Override
  public ClusterInfo getClusterInfo(String clusterId) {
    ClusterInfo clusterInfo = new ClusterInfo();
    clusterInfo.setClusterId(clusterId);
    Asg serverAsg = new Asg();
    serverAsg.setGroupType("server");
    serverAsg.setAsg(getAutoScalingGroup(getAsgNamesFromHosts(clusterId, "server").get(0)));
    clusterInfo.setServers(serverAsg);
    List<Asg> workerAsgs = new ArrayList<>();
    for (String asgName : getAsgNamesFromHosts(clusterId, "worker")) {
      Asg workerAsg = new Asg();
      workerAsg.setGroupType("worker");
      workerAsg.setAsg(getAutoScalingGroup(asgName));
      workerAsg.setWorkerType(getAsgTagValue("buildfarm.worker_type", workerAsg.getAsg().getTags()));
      workerAsgs.add(workerAsg);
    }
    clusterInfo.setWorkers(workerAsgs);
    return clusterInfo;
  }

  @Override
  public ClusterDetails getClusterDetails() {
    ClusterDetails clusterDetails = new ClusterDetails();
    clusterDetails.setClusterId(clusterId);
    clusterDetails.setServers(getInstances(clusterId,"server"));
    clusterDetails.setWorkers(getInstances(clusterId,"worker"));
    return clusterDetails;
  }

  @Override
  public int stopDockerContainer(String instanceId, String containerStr, String grpcEndpoint, int grpcPort) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress(grpcEndpoint, grpcPort).usePlaintext().build();
    AdminGrpc.AdminBlockingStub stub = AdminGrpc.newBlockingStub(channel);
    StopContainerRequest request = StopContainerRequest.newBuilder().setHostId(instanceId).setContainerName(containerStr).build();
    Status status = stub.stopContainer(request);
    logger.info("Rebooted container {} using {}:{} with status {}", instanceId, grpcEndpoint, grpcPort, status);
    channel.shutdown();
    return status.getCode();
  }

  @Override
  public int terminateInstance(String instanceId, String grpcEndpoint, int grpcPort) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress(grpcEndpoint, grpcPort).usePlaintext().build();
    AdminGrpc.AdminBlockingStub stub = AdminGrpc.newBlockingStub(channel);
    TerminateHostRequest request = TerminateHostRequest.newBuilder().setHostId(instanceId).build();
    Status status = stub.terminateHost(request);
    logger.info("Terminated instance {} using {}:{} with status {}", instanceId, grpcEndpoint, grpcPort, status);
    channel.shutdown();
    return status.getCode();
  }

  @Override
  public String getInstanceIdByPrivateDnsName(String dnsName) {
    Filter filter = new Filter().withName("private-dns-name").withValues(dnsName);
    DescribeInstancesRequest describeInstancesRequest =
      new DescribeInstancesRequest().withFilters(filter);
    DescribeInstancesResult instancesResult = ec2.describeInstances(describeInstancesRequest);
    for (Reservation r : instancesResult.getReservations()) {
      for (com.amazonaws.services.ec2.model.Instance e : r.getInstances()) {
        if (e.getPrivateDnsName() != null && e.getPrivateDnsName().equals(dnsName)) {
          return e.getInstanceId();
        }
      }
    }
    return null;
  }

  @Override
  public String scaleGroup(String asgName, Integer desiredInstances) {
    logger.info("Scaling group {} to {} instances", asgName, desiredInstances);
    UpdateAutoScalingGroupRequest request = new UpdateAutoScalingGroupRequest()
      .withAutoScalingGroupName(asgName).withDesiredCapacity(desiredInstances);
    UpdateAutoScalingGroupResult response = autoScale.updateAutoScalingGroup(request);
    return response.toString();
  }

  private List<Instance> getInstances(String clusterId, String type) {
    List<Instance> instances = new ArrayList<>();
    for (com.amazonaws.services.ec2.model.Instance e : getEc2Instances(clusterId, type)) {
      Instance instance = new Instance();
      instance.setEc2Instance(e);
      instance.setClusterId(clusterId);
      if ("worker".equals(type)) {
        instance.setWorkerType(getTagValue("buildfarm.worker_type", e.getTags()));
      }
      String clientKey= "startTime/" + e.getPrivateIpAddress() + ("worker".equals(type) ? ":8981" : "");
      instance.setGroupType(type);
      instances.add(instance);
    }
    return updateContainersUptimes(instances, type);
  }

  private List<Instance> updateContainersUptimes(List<Instance> instances, String type) {
    List<String> hostNames = new ArrayList<>();
    for (Instance instance : instances) {
      hostNames.add("startTime/" + instance.getEc2Instance().getPrivateIpAddress() + ("worker".equals(type) ? ":8981" : ""));
    }
    Map<String, Long> allContainersUptime = getAllContainersUptime(hostNames);
    for (Instance instance : instances) {
      String clientKey = "startTime/" + instance.getEc2Instance().getPrivateIpAddress() + ("worker".equals(type) ? ":8981" : "");
      instance.setContainerStartTime(allContainersUptime.get(clientKey) != null ? allContainersUptime.get(clientKey) : 0L );
    }
    return instances;
  }

  private Map<String, Long>  getAllContainersUptime(List<String> hostNames) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress(deploymentDomain, deploymentPort).usePlaintext().build();
    AdminGrpc.AdminBlockingStub stub = AdminGrpc.newBlockingStub(channel);
    GetClientStartTimeRequest request = GetClientStartTimeRequest.newBuilder()
            .setInstanceName("shard")
            .addAllHostName(hostNames)
            .build();
    GetClientStartTimeResult result = stub.getClientStartTime(request);
    Map<String, Long> allContainersUptime = new HashMap<String, Long>();
    for (GetClientStartTime GetClientStartTime : result.getClientStartTimeList()){
      allContainersUptime.put(GetClientStartTime.getInstanceName(),GetClientStartTime.getClientStartTime().getSeconds());
    }
    if (channel != null) {
      channel.shutdown();
    }
    return allContainersUptime;
  }

  private AutoScalingGroup getAutoScalingGroup(String asgName) {
    DescribeAutoScalingGroupsRequest request = new DescribeAutoScalingGroupsRequest()
      .withAutoScalingGroupNames(Arrays.asList(asgName));
    DescribeAutoScalingGroupsResult response = autoScale.describeAutoScalingGroups(request);
    return response.getAutoScalingGroups().get(0);
  }

  private List<com.amazonaws.services.ec2.model.Instance> getEc2Instances(String clusterId, String type) {
    List<com.amazonaws.services.ec2.model.Instance> instances = new ArrayList<>();
    DescribeInstancesResult instancesResult = ec2.describeInstances(
            new DescribeInstancesRequest().withFilters(
                    new Filter().withName("instance-state-name").withValues("running"),
                    new Filter().withName("tag:buildfarm.cluster_id").withValues(clusterId),
                    new Filter().withName("tag:buildfarm.instance_type").withValues(type)));
    for (Reservation r : instancesResult.getReservations()) {
      for (com.amazonaws.services.ec2.model.Instance e : r.getInstances()) {
        if (e != null) {
          instances.add(e);
        }
      }
    }
    return instances;
  }

  private List<String> getAsgNamesFromHosts(String clusterId, String type) {
    List<String> asgNames = new ArrayList<>();
    DescribeInstancesResult instancesResult = ec2.describeInstances(
            new DescribeInstancesRequest().withFilters(
                    new Filter().withName("instance-state-name").withValues("running"),
                    new Filter().withName("tag:buildfarm.cluster_id").withValues(clusterId),
                    new Filter().withName("tag:buildfarm.instance_type").withValues(type)));
    for (com.amazonaws.services.ec2.model.Instance e : getEc2Instances(clusterId, type)) {
      for (Tag tag : e.getTags()) {
        if ("aws:autoscaling:groupName".equalsIgnoreCase(tag.getKey()) && !asgNames.contains(tag.getValue())) {
          asgNames.add(tag.getValue());
        }
      }
    }
    return asgNames;
  }

  @Override
  public void reindexAllCas(){
    ManagedChannel channel = ManagedChannelBuilder.forAddress(deploymentDomain, deploymentPort).usePlaintext().build();
    AdminGrpc.AdminFutureStub stub = AdminGrpc.newFutureStub(channel);
    ReindexAllCasRequest request = ReindexAllCasRequest.newBuilder().setInstanceName("shard").build();
    stub.reindexAllCas(request);
  }

  private String getTagValue(String tagName, List<Tag> tags) {
    for (Tag tag : tags) {
      if (tagName.equalsIgnoreCase(tag.getKey())) {
        return tag.getValue();
      }
    }
    return "";
  }

  private String getAsgTagValue(String tagName, List<TagDescription> tags) {
    for (TagDescription tag : tags) {
      if (tagName.equalsIgnoreCase(tag.getKey())) {
        return tag.getValue();
      }
    }
    return "";
  }
}
