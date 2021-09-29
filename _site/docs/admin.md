---
layout: default
title: Admin
nav_order: 6
---

### How to make your AWS cluster visible to Buildfarm Admin

Buildfarm Admin currently only works with Buildfarm clusters deployed in AWS. All hosts must be properly tagged with values specified in application.properties.

* Tag schedulers (servers) with buildfarm.instance_type={deployment.tag.instance.type.server}
* Tag CPU workers with buildfarm.worker_type={deployment.tag.instance.type.cpuworker}
* Tag GPU workers with buildfarm.worker_type={deployment.tag.instance.type.gpuworker}
* Tag all hosts with aws:autoscaling:groupName={<ACTUAL AUTOSCALING GROUP NAME>}
* Tag all hosts with buildfarm.cluster_id={cluster.id}

### REST API Endpoints

* /restart/worker/{instanceId}
* /restart/server/{instanceId}
* /terminate/{instanceId}
* /scale/{autoScaleGroup}/{numInstances}

### Build and Run Buildfarm Admin locally

Create application.properties file and override any default settings from admin/main/src/main/resources/application.properties. Pass your config file location to the optional spring.config.location flag.

`
cd admin/main
./mvnw clean package
java -jar target/bfadmin.jar -Dspring.config.location=file:<PATH_TO_PROPERTIES_FILE>
`

### Run Latest Docker Container

Replace $HOME with the location of your application.properties file. Make sure the AWS environment variables are set on the host machine for the account where Buildfarm cluster is deployed.

`
docker run -p 8080:8080 -v $HOME:/var/lib -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN bazelbuild/buildfarm-admin:latest
`