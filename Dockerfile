# A minimal container for building and running buildfarm services.
# We copy in the current state of the reposiory to test against PR changes.
#FROM ubuntu:18.04@sha256:152dc042452c496007f07ca9127571cb9c29697f42acbfad72324b2bb2e43c98
FROM ubuntu:24.04@sha256:6015f66923d7afbc53558d7ccffd325d43b4e249f41a6e93eef074c9505d2233

# For network stability we allow apt-get to retry.
# The "80" is required for config priority but its not specifically important.
RUN echo 'APT::Acquire::Retries "5";' > /etc/apt/apt.conf.d/80retries

RUN apt update
RUN apt -y install wget git zip python3-pip python3-dateutil gcc default-jdk-headless g++ redis redis-server
COPY . buildfarm
