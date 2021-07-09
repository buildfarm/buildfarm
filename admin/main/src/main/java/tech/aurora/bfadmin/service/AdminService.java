package tech.aurora.bfadmin.service;

import com.amazonaws.services.autoscaling.model.AutoScalingGroup;
import tech.aurora.bfadmin.model.Ec2Instance;
import java.util.List;

public interface AdminService {

  List<Ec2Instance> getInstances(String asgName, int ageInMinutes, String grpcEndpoint, int grpcPort);

  String scaleGroup(String autoScaleGroup, Integer desiredInstances);

  int terminateInstance(String instanceId, String grpcEndpoint, int grpcPort);

  void gracefullyShutDownWorker(String workerName, String grpcEndpoint, int grpcPort);

  String getInstanceIdByPrivateDnsName(String dnsName);

  String resizeGroup(String autoScaleGroup, Integer minInstances, Integer maxInstances);

  AutoScalingGroup describeAutoScalingGroup(String autoScaleGroup);

  int stopDockerContainer(String instanceId, String containerType, String grpcEndpoint, int grpcPort);

  String scaleOnDemandTargetPercentage(String autoScaleGroup, Integer onDemandTargetPercentage, Integer minPercentage);

  String setOnDemandTargetPercentage(String autoScaleGroup, Integer onDemandTargetPercentage, Integer minPercentage);
}
