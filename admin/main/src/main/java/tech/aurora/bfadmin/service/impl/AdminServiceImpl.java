package tech.aurora.bfadmin.service.impl;

import build.buildfarm.v1test.AdminGrpc;
import build.buildfarm.v1test.GetClientStartTimeRequest;
import build.buildfarm.v1test.GetClientStartTimeResult;
import build.buildfarm.v1test.GetHostsRequest;
import build.buildfarm.v1test.GetHostsResult;
import build.buildfarm.v1test.Host;
import build.buildfarm.v1test.ShutDownWorkerGracefullyRequest;
import build.buildfarm.v1test.StopContainerRequest;
import build.buildfarm.v1test.TerminateHostRequest;
import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClientBuilder;
import com.amazonaws.services.autoscaling.model.AutoScalingGroup;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsResult;
import com.amazonaws.services.autoscaling.model.InstancesDistribution;
import com.amazonaws.services.autoscaling.model.MixedInstancesPolicy;
import com.amazonaws.services.autoscaling.model.UpdateAutoScalingGroupRequest;
import com.amazonaws.services.autoscaling.model.UpdateAutoScalingGroupResult;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.Status;
import tech.aurora.bfadmin.model.Ec2Instance;
import tech.aurora.bfadmin.service.AdminService;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Service
public class AdminServiceImpl implements AdminService {
  private static final Logger logger = LoggerFactory.getLogger(AdminServiceImpl.class);

  @Value("${buildfarm.server.port}")
  private int serverPort;

  @Value("${buildfarm.worker.port}")
  private int workerPort;

  @Value("aws.region")
  private String region;

  private AmazonEC2 ec2;
  private AmazonAutoScaling autoScale;

  @PostConstruct
  public void init() {
    ec2 = AmazonEC2ClientBuilder.standard().withRegion(region).build();
    autoScale = AmazonAutoScalingClientBuilder.standard().withRegion(region).build();
  }

  @Override
  public List<Ec2Instance> getInstances(String asgName, int ageInMinutes, String grpcEndpoint, int grpcPort) {
    List<Ec2Instance> instances = new ArrayList<>();
    ManagedChannel channel = ManagedChannelBuilder.forAddress(grpcEndpoint, grpcPort).usePlaintext().build();
    try {
      AdminGrpc.AdminBlockingStub stub = AdminGrpc.newBlockingStub(channel);
      GetHostsRequest request = GetHostsRequest.newBuilder().setFilter(asgName).setAgeInMinutes(ageInMinutes).setStatus("running").build();
      GetHostsResult result = stub.getHosts(request);
      for (Host host : result.getHostsList()) {
        instances.add(new Ec2Instance(
          host.getHostNum(),
          host.getHostId(),
          host.getIpAddress(),
          new Date(Timestamps.toMillis(host.getLaunchTime())),
          host.getState(),
          host.getDnsName(),
          host.getLaunchTime().getSeconds() * 1000,
          getContainerUptime(host.getIpAddress(), asgName, stub), 
          host.getNumCores(),
          host.getType(),
          host.getLifecycle()));
      }
    } catch (Exception e) {
      logger.error("Could not load instances. Is {} cluster up and running?", grpcEndpoint, e);
    } finally {
      channel.shutdown();
    }
    return instances;
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
  public void gracefullyShutDownWorker(String workerName, String grpcEndpoint, int grpcPort) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress(grpcEndpoint, grpcPort).usePlaintext().build();
    AdminGrpc.AdminBlockingStub stub = AdminGrpc.newBlockingStub(channel);
    ShutDownWorkerGracefullyRequest request = ShutDownWorkerGracefullyRequest.newBuilder().setWorkerName(workerName).build();
    stub.shutDownWorkerGracefully(request);
    channel.shutdown();
    logger.info("Initiated graceful worker shutdown for worker {} using {}:{}", grpcEndpoint, grpcPort);
  }

  @Override
  public String getInstanceIdByPrivateDnsName(String dnsName) {
    Filter filter = new Filter().withName("private-dns-name").withValues(dnsName);
    DescribeInstancesRequest describeInstancesRequest =
      new DescribeInstancesRequest().withFilters(filter);
    DescribeInstancesResult instancesResult = ec2.describeInstances(describeInstancesRequest);
    for (Reservation r : instancesResult.getReservations()) {
      for (Instance e : r.getInstances()) {
        if (e.getPrivateDnsName() != null && e.getPrivateDnsName().equals(dnsName)) {
          return e.getInstanceId();
        }
      }
    }
    return null;
  }

  @Override
  public String scaleGroup(String autoScaleGroup, Integer desiredInstances) {
    logger.info("Scaling group {} to {} instances", autoScaleGroup, desiredInstances);
    UpdateAutoScalingGroupRequest request = new UpdateAutoScalingGroupRequest()
      .withAutoScalingGroupName(autoScaleGroup).withDesiredCapacity(desiredInstances);
    UpdateAutoScalingGroupResult response = autoScale.updateAutoScalingGroup(request);
    return response.toString();
  }

  @Override
  public String resizeGroup(String autoScaleGroup, Integer minInstances, Integer maxInstances) {
    logger.info("Resizing group {} to min: {}, max: {} instances", autoScaleGroup, minInstances,
      maxInstances);
    UpdateAutoScalingGroupRequest request =
      new UpdateAutoScalingGroupRequest().withAutoScalingGroupName(autoScaleGroup);
    if (minInstances != null && minInstances > 0) {
      request.setMinSize(minInstances);
    }
    if (maxInstances != null && maxInstances > 0) {
      request.setMaxSize(maxInstances);
    }
    UpdateAutoScalingGroupResult response = autoScale.updateAutoScalingGroup(request);
    return response.toString();
  }

  @Override
  public String scaleOnDemandTargetPercentage(String autoScaleGroup, Integer onDemandTargetPercentage, Integer minPercentage) {
    AutoScalingGroup asg = describeAutoScalingGroup(autoScaleGroup);
    MixedInstancesPolicy mixedInstancesPolicy = asg.getMixedInstancesPolicy();
    InstancesDistribution instancesDistribution = mixedInstancesPolicy.getInstancesDistribution();
    return setOnDemandTargetPercentage(autoScaleGroup, instancesDistribution.getOnDemandPercentageAboveBaseCapacity() + onDemandTargetPercentage, minPercentage);
  }

  @Override
  public String setOnDemandTargetPercentage(String autoScaleGroup, Integer onDemandTargetPercentage, Integer minPercentage) {
    if (onDemandTargetPercentage < minPercentage) {
      onDemandTargetPercentage = minPercentage;
    } else if (onDemandTargetPercentage > 100) {
      onDemandTargetPercentage = 100;
    }
    logger.info("Updating group {} on demand target to: {}%", autoScaleGroup, onDemandTargetPercentage);
    UpdateAutoScalingGroupRequest request =
      new UpdateAutoScalingGroupRequest()
        .withAutoScalingGroupName(autoScaleGroup)
        .withMixedInstancesPolicy(new MixedInstancesPolicy().withInstancesDistribution(new InstancesDistribution().withOnDemandPercentageAboveBaseCapacity(onDemandTargetPercentage)));
    UpdateAutoScalingGroupResult response = autoScale.updateAutoScalingGroup(request);
    return response.toString();
  }

  @Override
  public AutoScalingGroup describeAutoScalingGroup(String autoScaleGroup) {
    DescribeAutoScalingGroupsRequest request = new DescribeAutoScalingGroupsRequest()
      .withAutoScalingGroupNames(Arrays.asList(autoScaleGroup));
    DescribeAutoScalingGroupsResult response = autoScale.describeAutoScalingGroups(request);
    return response.getAutoScalingGroups().get(0);
  }

  private Long getContainerUptime(String hostname, String asgName, AdminGrpc.AdminBlockingStub stub) {
    GetClientStartTimeRequest request = GetClientStartTimeRequest.newBuilder().setClientKey("startTime/" + hostname + (asgName.contains("server") ? "" : (":" + workerPort))).setInstanceName("shard").build();
    GetClientStartTimeResult result = stub.getClientStartTime(request);
    if (result.hasClientStartTime()) {
      return result.getClientStartTime().getSeconds();
    } else {
      return 0L;
    }
  }
}
