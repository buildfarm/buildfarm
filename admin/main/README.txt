# Buildfarm Admin

## AWS Requirements

All autoscaling groups and EC2 instances must be properly tagged:

buildfarm.cluster_id=buildfarm-dev
buildfarm.instance_type=[server|worker]
buildfarm.worker_type=[cpu|gpu|other] (for workers only)

## Usage

### Build and Run Locally

```
./mvnw clean package && java -jar target/bfadmin.jar -Dspring.config.location=file:./
```

### Build and Run Docker Container

```
./mvnw clean package docker:build -DdockerImageTags=1.0
docker run -p 8080:8080 -v $HOME:/var/lib -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN bazelbuild/buildfarm-admin:latest
```

### Run dependency check using OWASP plugin

```
./mvnw org.owasp:dependency-check-maven:check
```