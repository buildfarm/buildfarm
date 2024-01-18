# A minimal container for building and running buildfarm services.
# We copy in the current state of the reposiory to test against PR changes.
FROM ubuntu:22.04

# For network stability we allow apt-get to retry.
# The "80" is required for config priority but its not specifically important.
RUN echo 'APT::Acquire::Retries "5";' > /etc/apt/apt.conf.d/80retries

RUN DEBIAN_FRONTEND=noninteractive apt-get -y update \
  && DEBIAN_FRONTEND=noninteractive apt-get -y install \
    wget git \
    gcc g++ \
    unzip zip \
    python3 python3-pip python3-dateutil \
    default-jdk \
    redis redis-server
COPY . buildfarm
